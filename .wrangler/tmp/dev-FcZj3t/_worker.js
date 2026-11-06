var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// www/_worker.js
var FightRoom = class {
  static {
    __name(this, "FightRoom");
  }
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.clients = /* @__PURE__ */ new Map();
    this.hostId = null;
    this.roomCode = null;
    this.quick = false;
    this.lastActivity = Date.now();
    this.messageIdCounter = 0;
    this.pendingSignaling = /* @__PURE__ */ new Map();
    this.peerConnections = /* @__PURE__ */ new Map();
    this.dataChannels = /* @__PURE__ */ new Map();
    this.iceCandidates = /* @__PURE__ */ new Map();
    this.relayCount = /* @__PURE__ */ new Map();
    this.relayFallback = false;
    this.hibernationTimer = null;
    this.sessionCounter = 0;
    this.initFromStorage();
  }
  async initFromStorage() {
    const state = await this.ctx.storage.get("roomState");
    if (state) {
      this.roomCode = state.roomCode;
      this.hostId = state.hostId;
      this.quick = state.quick;
      this.lastActivity = state.lastActivity || Date.now();
      console.log("[FightRoom] State restored from storage:", this.roomCode);
    }
  }
  async persistState() {
    const state = {
      roomCode: this.roomCode,
      hostId: this.hostId,
      clients: Array.from(this.clients.keys()),
      quick: this.quick,
      lastActivity: this.lastActivity
    };
    await this.ctx.storage.put("roomState", state);
  }
  // Called when a WebSocket client connects via Worker's doObj.accept()
  // roomCode and isQuick are passed from Worker since ws.request isn't available
  accept(ws, roomCode = null, isQuick = false) {
    console.log("[FightRoom] accept called:", { roomCode, isQuick, clients: this.clients.size });
    const sessionId = `sess-${++this.sessionCounter}-${Date.now().toString(36)}`;
    ws.sessionId = sessionId;
    this.clients.set(sessionId, ws);
    this.lastActivity = Date.now();
    if (this.clients.size === 1) {
      this.hostId = sessionId;
      if (!this.roomCode) {
        this.roomCode = isQuick ? `QK-${this.generateCode()}` : roomCode || this.generateCode();
        this.quick = isQuick;
        this.ctx.waitUntil(this.persistState());
      }
    } else if (!this.roomCode && roomCode) {
      this.roomCode = roomCode.toUpperCase();
      this.quick = isQuick;
      this.ctx.waitUntil(this.persistState());
    }
    console.log("[FightRoom] after accept:", { sessionId, roomCode: this.roomCode, clients: this.clients.size });
    const initMsg = JSON.stringify({
      type: "roomInit",
      sessionId,
      code: this.roomCode,
      quick: this.quick,
      host: sessionId === this.hostId,
      clients: this.clients.size,
      maxClients: 2
    });
    console.log("[FightRoom] Sending roomInit:", initMsg);
    ws.send(initMsg);
    this.broadcastToAll("peerjoin", { peer: sessionId, host: sessionId === this.hostId }, { except: ws });
    ws.onmessage = async (event) => {
      try {
        console.log("[FightRoom] Received message from", sessionId, ":", event.data);
        const data = JSON.parse(event.data);
        await this.handleMessage(ws, data, sessionId);
      } catch (e) {
        console.error("[FightRoom] Error parsing message:", e);
        ws.close(1008, "invalid JSON");
      }
    };
    ws.onclose = () => {
      this.onClientDisconnect(ws, sessionId);
    };
    ws.onerror = (err) => {
      console.error("[FightRoom] WebSocket error:", err);
      this.onClientDisconnect(ws, sessionId);
    };
    console.log(`[FightRoom] Client connected: ${sessionId}, room: ${this.roomCode}, total: ${this.clients.size}/2`);
  }
  async handleMessage(ws, data, sessionId) {
    const { type, payload } = data;
    this.lastActivity = Date.now();
    switch (type) {
      case "join":
        await this.handleJoin(ws, payload.code, payload.quick, sessionId);
        break;
      case "leave":
        this.onClientDisconnect(ws, sessionId);
        break;
      case "sig":
        await this.handleWebRTCSignaling(ws, payload, sessionId);
        break;
      case "inp":
        this.handleInput(ws, payload, sessionId);
        break;
      case "lob":
        this.handleLobbyMessage(ws, payload, sessionId);
        break;
      case "ping":
        this.broadcastToAll("pong", {}, { except: ws });
        break;
      case "stats":
        this.sendStats(ws, sessionId);
        break;
      default:
        console.warn(`[FightRoom] Unknown message type: ${type}`);
    }
  }
  async handleJoin(ws, code, quick, sessionId) {
    if (code && !/^[A-Z0-9]{6}$/.test(code.toUpperCase())) {
      ws.send(JSON.stringify({
        type: "error",
        message: "Invalid room code format. Must be 6 uppercase letters/numbers."
      }));
      return;
    }
    if (this.roomCode && code && code.toUpperCase() !== this.roomCode) {
      ws.send(JSON.stringify({
        type: "error",
        message: "Room code mismatch."
      }));
      return;
    }
    if (!this.roomCode) {
      this.roomCode = (code || this.generateCode()).toUpperCase();
      this.quick = !!quick;
      await this.persistState();
    }
    ws.send(JSON.stringify({
      type: "roomState",
      code: this.roomCode,
      quick: this.quick,
      host: sessionId === this.hostId,
      clients: Array.from(this.clients.keys())
    }));
  }
  onClientDisconnect(ws, sessionId) {
    this.clients.delete(sessionId);
    this.broadcastToAll("peerleft", { peer: sessionId });
    if (this.hostId === sessionId) {
      const newHost = Array.from(this.clients.keys())[0];
      if (newHost) {
        this.hostId = newHost;
        this.broadcastToAll("hostchange", { newHost });
      }
    }
    console.log(`[FightRoom] Client left: ${sessionId}, remaining: ${this.clients.size}`);
    if (this.clients.size === 0) {
      this.scheduleHibernation();
    }
  }
  async handleWebRTCSignaling(ws, payload, sessionId) {
    const { kind, sdp, candidate } = payload;
    console.log(`[FightRoom] Signaling from ${sessionId}: kind=${kind}`);
    const otherSessionId = Array.from(this.clients.keys()).find(
      (id) => id !== sessionId
    );
    if (!otherSessionId) {
      if (!this.pendingSignaling.has(sessionId)) {
        this.pendingSignaling.set(sessionId, []);
      }
      this.pendingSignaling.get(sessionId).push({ ws, payload, sessionId });
      return;
    }
    const otherWs = this.clients.get(otherSessionId);
    if (!otherWs) return;
    if (candidate) {
      this.iceCandidates.set(sessionId, candidate);
      otherWs.send(JSON.stringify({
        type: "ice-candidate",
        target: sessionId,
        candidate
      }));
    } else if (sdp) {
      if (sdp.type === "offer") {
        otherWs.send(JSON.stringify({
          type: "sdp-offer",
          target: sessionId,
          sdp
        }));
      } else if (sdp.type === "answer") {
        otherWs.send(JSON.stringify({
          type: "sdp-answer",
          target: sessionId,
          sdp
        }));
      }
    }
  }
  setupDataChannel(dc, sessionId) {
    this.dataChannels.set(sessionId, dc);
    dc.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        if (data.e === "inp") {
          this.handleInputFromPeer(null, data.d, sessionId);
        } else if (data.e === "lob" && data.d && (data.d.t === "ping" || data.d.t === "pong")) {
          this.broadcastToAll("lob", { peer: sessionId, data: data.d }, { except: this.getWsForSession(sessionId) });
        }
      } catch (e) {
        console.error("[FightRoom] Error parsing DC message:", e);
      }
    };
    dc.onopen = () => {
      console.log(`[FightRoom] DataChannel open: ${sessionId}`);
      this.clearNoPacketTimer(sessionId);
    };
    dc.onclose = () => {
      console.log(`[FightRoom] DataChannel closed: ${sessionId}`);
      this.onDataChannelClosed(sessionId);
    };
    dc.onerror = (err) => {
      console.error(`[FightRoom] DataChannel error: ${sessionId}:`, err);
      this.triggerRelayFallback(sessionId);
    };
  }
  handleInputFromPeer(ws, inputData, fromSessionId) {
    this.lastActivity = Date.now();
    const bucketKey = fromSessionId;
    if (!this.relayCount.has(bucketKey)) {
      this.relayCount.set(bucketKey, { tokens: 100, last: Date.now() });
    }
    const bucket = this.relayCount.get(bucketKey);
    const now = Date.now();
    const elapsed = now - bucket.last;
    bucket.tokens = Math.min(100, bucket.tokens + Math.floor(elapsed * 100 / 200));
    bucket.last = now;
    if (bucket.tokens < 1) {
      return;
    }
    bucket.tokens -= 1;
    const otherSessionId = Array.from(this.clients.keys()).find(
      (id) => id !== fromSessionId
    );
    if (otherSessionId && this.dataChannels.has(otherSessionId)) {
      const dc = this.dataChannels.get(otherSessionId);
      if (dc && dc.readyState === "open") {
        try {
          dc.send(JSON.stringify({ e: "inp", d: inputData }));
          return;
        } catch (e) {
          console.error("[FightRoom] Error sending via DC:", e);
        }
      }
    }
    this.broadcastToAll("inp", { peer: fromSessionId, data: inputData }, { except: ws });
  }
  handleInput(ws, inputData, sessionId) {
    this.lastActivity = Date.now();
    if (!this.relayCount.has(sessionId)) {
      this.relayCount.set(sessionId, { tokens: 100, last: Date.now() });
    }
    const bucket = this.relayCount.get(sessionId);
    const now = Date.now();
    const elapsed = now - bucket.last;
    bucket.tokens = Math.min(100, bucket.tokens + Math.floor(elapsed * 100 / 200));
    bucket.last = now;
    if (bucket.tokens < 1) {
      return;
    }
    bucket.tokens -= 1;
    this.broadcastToAll("inp", { peer: sessionId, data: inputData }, { except: ws });
  }
  handleLobbyMessage(ws, payload, sessionId) {
    const { type, data } = payload;
    switch (type) {
      case "ready":
        this.broadcastToAll("readyStatus", { peer: sessionId, ready: data.ready });
        break;
      case "lobby":
        this.broadcastToAll("lobby", { peer: sessionId, data }, { except: ws });
        break;
      case "pick":
        this.broadcastToAll("pick", { peer: sessionId, pick: data.pick }, { except: ws });
        break;
      case "ping":
      case "pong":
        this.broadcastToAll("lob", { peer: sessionId, data: { t: type, ts: Date.now() } });
        break;
      default:
        console.warn(`[FightRoom] Unknown lobby type: ${type}`);
    }
  }
  broadcastToAll(type, data, opts = {}) {
    const message = JSON.stringify({ type, data });
    for (const [sId, ws] of this.clients) {
      if (opts.except && sId === opts.except.sessionId) continue;
      if (ws.readyState === WebSocket.OPEN) {
        ws.send(message);
      }
    }
  }
  getWsForSession(sessionId) {
    return this.clients.get(sessionId);
  }
  // Rate limit and relay fallback
  checkRelayFallback(sessionId) {
    const now = Date.now();
    if (now - this.lastActivity > 2500 && this.relayFallback === false) {
      this.relayFallback = true;
      this.broadcastToAll("relayMode", { status: "activated" });
      console.log("[FightRoom] Relay fallback activated for", sessionId);
    }
  }
  scheduleHibernation() {
    if (this.hibernationTimer) {
      clearTimeout(this.hibernationTimer);
    }
    this.hibernationTimer = setTimeout(() => {
      console.log("[FightRoom] Scheduling hibernation - room empty:", this.roomCode);
      this.ctx.waitUntil(this.hibernate());
    }, 5 * 60 * 1e3);
  }
  async hibernate() {
    await this.persistState();
    console.log("[FightRoom] State persisted for hibernation:", this.roomCode);
    for (const ws of this.clients.values()) {
      ws.close();
    }
    this.clients.clear();
    this.peerConnections.clear();
    this.dataChannels.clear();
    this.iceCandidates.clear();
    this.relayCount.clear();
    console.log("[FightRoom] Hibernate complete:", this.roomCode);
  }
  async activate() {
    await this.initFromStorage();
    console.log("[FightRoom] Activated:", this.roomCode);
  }
  // Generate a 6-character uppercase room code
  generateCode() {
    const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let code = "";
    for (let i = 0; i < 6; i++) {
      code += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return code;
  }
};
var worker_default = {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;
    if (request.headers.get("upgrade") === "websocket") {
      return this.handleWebSocketUpgrade(request, env, ctx);
    }
    if (path === "/") {
      return new Response("Ashfall Brawl Workers API", { status: 200 });
    }
    if (path === "/health") {
      return new Response(
        JSON.stringify({ ok: true, env: "configured" }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }
    if (path === "/api/create-room") {
      return this.handleCreateRoom(request, env, ctx);
    }
    if (path === "/api/quick-match") {
      return this.handleQuickMatchmaking(request, env, ctx);
    }
    if (path === "/api/join-room") {
      return this.handleJoinRoom(request, env, ctx);
    }
    return new Response("Not found", { status: 404 });
  },
  async handleWebSocketUpgrade(request, env, ctx) {
    console.log("[Worker] WebSocket upgrade request:", request.url);
    const pair = new WebSocketPair();
    const [client, server] = pair;
    const url = new URL(request.url);
    const roomCode = url.pathname.split("/").pop() || url.searchParams.get("code");
    const isQuick = url.searchParams.get("quick") === "true";
    let doId;
    if (roomCode) {
      const encoder = new TextEncoder();
      const data = encoder.encode(roomCode.toUpperCase());
      const hash = await crypto.subtle.digest("SHA-1", data);
      const hashArray = new Uint8Array(hash);
      doId = `fight-${hashArray[0].toString(16)}${hashArray[1].toString(16)}${hashArray[2].toString(16)}${hashArray[3].toString(16)}`;
    } else {
      doId = `fight-temp-${Date.now().toString(36)}`;
    }
    console.log("[Worker] DO ID:", doId, "roomCode:", roomCode, "isQuick:", isQuick);
    const stub = env.FIGHT_ROOM.idFromName(doId);
    const doObj = env.FIGHT_ROOM.get(stub);
    console.log("[Worker] Calling doObj.accept()");
    doObj.accept(server, roomCode, isQuick);
    console.log("[Worker] doObj.accept() returned");
    return new Response(null, {
      status: 101,
      webSocket: client
    });
  },
  async handleCreateRoom(request, env, ctx) {
    const body = await request.json().catch(() => ({}));
    const code = body.code ? body.code.toUpperCase() : this.generateRoomCode().toUpperCase();
    if (!/^[A-Z0-9]{6}$/.test(code)) {
      return new Response(
        JSON.stringify({ error: "Invalid room code format. Must be 6 uppercase letters/numbers." }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }
    return new Response(
      JSON.stringify({
        roomCode: code,
        type: "code",
        wsUrl: `wss://${env.WORKER_URL?.replace(/^https?:\/\//, "") || "ashfallbrawl.workers.dev"}/${code}`
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  },
  async handleQuickMatchmaking(request, env, ctx) {
    const roomCode = `QK-${this.generateRoomCode().toUpperCase()}`;
    return new Response(
      JSON.stringify({
        roomCode,
        quick: true,
        wsUrl: `wss://${env.WORKER_URL?.replace(/^https?:\/\//, "") || "ashfallbrawl.workers.dev"}/${roomCode}`
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  },
  async handleJoinRoom(request, env, ctx) {
    const body = await request.json().catch(() => ({}));
    const roomCode = (body.code || "").toUpperCase();
    if (!roomCode || !/^[A-Z0-9]{6}$/.test(roomCode)) {
      return new Response(
        JSON.stringify({ error: "Invalid room code. Must be 6 uppercase letters/numbers." }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }
    return new Response(
      JSON.stringify({
        roomCode,
        type: "code",
        wsUrl: `wss://${env.WORKER_URL?.replace(/^https?:\/\//, "") || "ashfallbrawl.workers.dev"}/${roomCode}`
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  },
  generateRoomCode() {
    const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let code = "";
    for (let i = 0; i < 6; i++) {
      code += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return code;
  }
};

// ../../AppData/Local/npm-cache/_npx/32026684e21afda6/node_modules/wrangler/templates/middleware/middleware-ensure-req-body-drained.ts
var drainBody = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } finally {
    try {
      if (request.body !== null && !request.bodyUsed) {
        const reader = request.body.getReader();
        while (!(await reader.read()).done) {
        }
      }
    } catch (e) {
      console.error("Failed to drain the unused request body.", e);
    }
  }
}, "drainBody");
var middleware_ensure_req_body_drained_default = drainBody;

// ../../AppData/Local/npm-cache/_npx/32026684e21afda6/node_modules/wrangler/templates/middleware/middleware-miniflare3-json-error.ts
function reduceError(e) {
  return {
    name: e?.name,
    message: e?.message ?? String(e),
    stack: e?.stack,
    cause: e?.cause === void 0 ? void 0 : reduceError(e.cause)
  };
}
__name(reduceError, "reduceError");
var jsonError = /* @__PURE__ */ __name(async (request, env, _ctx, middlewareCtx) => {
  try {
    return await middlewareCtx.next(request, env);
  } catch (e) {
    const error = reduceError(e);
    const body = JSON.stringify(error);
    const headers = {
      "Content-Type": "application/json",
      "MF-Experimental-Error-Stack": "true"
    };
    const encoded = encodeURIComponent(body);
    if (encoded.length <= 8192) {
      headers["MF-Experimental-Error-Stack-Payload"] = encoded;
    }
    return new Response(body, { status: 500, headers });
  }
}, "jsonError");
var middleware_miniflare3_json_error_default = jsonError;

// .wrangler/tmp/bundle-SO6rG2/middleware-insertion-facade.js
var __INTERNAL_WRANGLER_MIDDLEWARE__ = [
  middleware_ensure_req_body_drained_default,
  middleware_miniflare3_json_error_default
];
var middleware_insertion_facade_default = worker_default;

// ../../AppData/Local/npm-cache/_npx/32026684e21afda6/node_modules/wrangler/templates/middleware/common.ts
var __facade_middleware__ = [];
function __facade_register__(...args) {
  __facade_middleware__.push(...args.flat());
}
__name(__facade_register__, "__facade_register__");
function __facade_invokeChain__(request, env, ctx, dispatch, middlewareChain) {
  const [head, ...tail] = middlewareChain;
  const middlewareCtx = {
    dispatch,
    next(newRequest, newEnv) {
      return __facade_invokeChain__(newRequest, newEnv, ctx, dispatch, tail);
    }
  };
  return head(request, env, ctx, middlewareCtx);
}
__name(__facade_invokeChain__, "__facade_invokeChain__");
function __facade_invoke__(request, env, ctx, dispatch, finalMiddleware) {
  return __facade_invokeChain__(request, env, ctx, dispatch, [
    ...__facade_middleware__,
    finalMiddleware
  ]);
}
__name(__facade_invoke__, "__facade_invoke__");

// .wrangler/tmp/bundle-SO6rG2/middleware-loader.entry.ts
var __Facade_ScheduledController__ = class ___Facade_ScheduledController__ {
  constructor(scheduledTime, cron, noRetry) {
    this.scheduledTime = scheduledTime;
    this.cron = cron;
    this.#noRetry = noRetry;
  }
  scheduledTime;
  cron;
  static {
    __name(this, "__Facade_ScheduledController__");
  }
  #noRetry;
  noRetry() {
    if (!(this instanceof ___Facade_ScheduledController__)) {
      throw new TypeError("Illegal invocation");
    }
    this.#noRetry();
  }
};
function wrapExportedHandler(worker) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return worker;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  const fetchDispatcher = /* @__PURE__ */ __name(function(request, env, ctx) {
    if (worker.fetch === void 0) {
      throw new Error("Handler does not export a fetch() function.");
    }
    return worker.fetch(request, env, ctx);
  }, "fetchDispatcher");
  return {
    ...worker,
    fetch(request, env, ctx) {
      const dispatcher = /* @__PURE__ */ __name(function(type, init) {
        if (type === "scheduled" && worker.scheduled !== void 0) {
          const controller = new __Facade_ScheduledController__(
            Date.now(),
            init.cron ?? "",
            () => {
            }
          );
          return worker.scheduled(controller, env, ctx);
        }
      }, "dispatcher");
      return __facade_invoke__(request, env, ctx, dispatcher, fetchDispatcher);
    }
  };
}
__name(wrapExportedHandler, "wrapExportedHandler");
function wrapWorkerEntrypoint(klass) {
  if (__INTERNAL_WRANGLER_MIDDLEWARE__ === void 0 || __INTERNAL_WRANGLER_MIDDLEWARE__.length === 0) {
    return klass;
  }
  for (const middleware of __INTERNAL_WRANGLER_MIDDLEWARE__) {
    __facade_register__(middleware);
  }
  return class extends klass {
    #fetchDispatcher = /* @__PURE__ */ __name((request, env, ctx) => {
      this.env = env;
      this.ctx = ctx;
      if (super.fetch === void 0) {
        throw new Error("Entrypoint class does not define a fetch() function.");
      }
      return super.fetch(request);
    }, "#fetchDispatcher");
    #dispatcher = /* @__PURE__ */ __name((type, init) => {
      if (type === "scheduled" && super.scheduled !== void 0) {
        const controller = new __Facade_ScheduledController__(
          Date.now(),
          init.cron ?? "",
          () => {
          }
        );
        return super.scheduled(controller);
      }
    }, "#dispatcher");
    fetch(request) {
      return __facade_invoke__(
        request,
        this.env,
        this.ctx,
        this.#dispatcher,
        this.#fetchDispatcher
      );
    }
  };
}
__name(wrapWorkerEntrypoint, "wrapWorkerEntrypoint");
var WRAPPED_ENTRY;
if (typeof middleware_insertion_facade_default === "object") {
  WRAPPED_ENTRY = wrapExportedHandler(middleware_insertion_facade_default);
} else if (typeof middleware_insertion_facade_default === "function") {
  WRAPPED_ENTRY = wrapWorkerEntrypoint(middleware_insertion_facade_default);
}
var middleware_loader_entry_default = WRAPPED_ENTRY;
export {
  FightRoom,
  __INTERNAL_WRANGLER_MIDDLEWARE__,
  middleware_loader_entry_default as default
};
//# sourceMappingURL=_worker.js.map
