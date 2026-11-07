var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

// durableObjects/FightRoom.js
var FightRoom = class {
  static {
    __name(this, "FightRoom");
  }
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.sessions = /* @__PURE__ */ new Map();
    this.roomCode = null;
    this.quick = false;
    this.lastInputAt = 0;
    this.relayMode = false;
    this.pendingSignals = /* @__PURE__ */ new Map();
    this.rateLimitBuckets = /* @__PURE__ */ new Map();
    const websockets = ctx.getWebSockets();
    console.log(`[FightRoom] Constructor: restoring ${websockets.length} hibernated sockets`);
    for (const ws of websockets) {
      const attachment = ws.deserializeAttachment();
      console.log(`[FightRoom] Hibernated socket attachment:`, attachment);
      if (attachment && attachment.sessionId) {
        this.sessions.set(attachment.sessionId, {
          ws,
          isHost: attachment.isHost,
          token: attachment.token,
          joinedAt: attachment.joinedAt
        });
        ws.onmessage = (event) => this.onWebSocketMessage(ws, event.data);
        ws.onclose = () => this.onWebSocketClose(ws);
        ws.onerror = (err) => this.onWebSocketError(ws, err);
        console.log(`[FightRoom] Restored session: ${attachment.sessionId}`);
      }
    }
  }
  // Standard DO fetch handler for WebSocket upgrades
  async fetch(request) {
    const upgradeHeader = request.headers.get("Upgrade");
    if (upgradeHeader !== "websocket") {
      return new Response("Expected WebSocket upgrade", { status: 400 });
    }
    const url = new URL(request.url);
    const roomCode = url.searchParams.get("code") || url.pathname.split("/").pop();
    const isQuick = url.searchParams.get("quick") === "true";
    const pair = new WebSocketPair();
    const [client, server] = pair;
    const sessionId = this.generateSessionId();
    const isHost = this.sessions.size === 0;
    if (!this.roomCode) {
      this.roomCode = isQuick ? `QK-${this.generateCode()}` : roomCode || this.generateCode();
      this.quick = isQuick;
      await this.persistState();
    } else if (roomCode && roomCode.toUpperCase() !== this.roomCode) {
      server.close(1008, "Room code mismatch");
      return new Response(null, { status: 101, webSocket: client });
    }
    if (this.sessions.size >= 2) {
      server.close(1003, "Room full (max 2 players)");
      return new Response(null, { status: 101, webSocket: client });
    }
    const token = crypto.randomUUID();
    await this.ctx.storage.put(`session:${token}`, {
      sessionId,
      isHost,
      roomCode: this.roomCode
    }, { expirationTtl: 86400 });
    server.serializeAttachment({ sessionId, isHost, token, joinedAt: Date.now() });
    this.ctx.acceptWebSocket(server);
    server.onmessage = (event) => this.onWebSocketMessage(server, event.data);
    server.onclose = () => this.onWebSocketClose(server);
    server.onerror = (err) => this.onWebSocketError(server, err);
    this.sessions.set(sessionId, { ws: server, isHost, token, joinedAt: Date.now() });
    server.send(JSON.stringify({
      type: "roomInit",
      sessionId,
      code: this.roomCode,
      quick: this.quick,
      isHost,
      maxClients: 2,
      sessionToken: token
    }));
    this.broadcast({ type: "peerjoin", sessionId, isHost }, { except: sessionId });
    this.drainPendingSignals(sessionId);
    return new Response(null, { status: 101, webSocket: client });
  }
  onWebSocketMessage(ws, message) {
    try {
      const data = JSON.parse(message);
      const sessionId = this.getSessionId(ws);
      if (!sessionId) return;
      this.handleMessage(ws, data, sessionId);
    } catch (e) {
      console.error("[FightRoom] Error parsing message:", e);
      ws.close(1003, "invalid JSON");
    }
  }
  onWebSocketClose(ws) {
    const sessionId = this.getSessionId(ws);
    if (!sessionId) return;
    this.onDisconnect(sessionId);
  }
  onWebSocketError(ws, error) {
    console.error("[FightRoom] WebSocket error:", error);
    const sessionId = this.getSessionId(ws);
    if (sessionId) this.onDisconnect(sessionId);
  }
  getSessionId(ws) {
    for (const [sessionId, session] of this.sessions) {
      if (session.ws === ws) return sessionId;
    }
    return null;
  }
  async handleMessage(ws, data, sessionId) {
    const { type, payload } = data;
    this.lastInputAt = Date.now();
    switch (type) {
      case "join":
        await this.handleJoin(ws, payload, sessionId);
        break;
      case "leave":
        this.onDisconnect(sessionId);
        break;
      case "sig":
        await this.handleSignaling(payload, sessionId);
        break;
      case "inp":
        this.handleInput(payload, sessionId);
        break;
      case "lob":
        this.handleLobbyMessage(payload, sessionId);
        break;
      case "relayRequest":
        this.handleRelayRequest(sessionId);
        break;
      case "ping":
        ws.send(JSON.stringify({ type: "pong" }));
        break;
      default:
        console.warn(`[FightRoom] Unknown message type: ${type}`);
    }
  }
  async handleJoin(ws, payload, sessionId) {
    const { code, sessionToken } = payload || {};
    if (sessionToken) {
      const stored = await this.ctx.storage.get(`session:${sessionToken}`);
      if (stored && stored.sessionId === sessionId && stored.roomCode === this.roomCode) {
        const session2 = this.sessions.get(sessionId);
        if (session2) {
          session2.token = sessionToken;
          session2.ws = ws;
          ws.serializeAttachment({ sessionId, isHost: session2.isHost, token: sessionToken, joinedAt: session2.joinedAt });
          ws.send(JSON.stringify({ type: "reconnected", sessionId, isHost: session2.isHost }));
          this.drainPendingSignals(sessionId);
          return;
        }
      }
    }
    if (code && !this.isValidRoomCode(code)) {
      ws.send(JSON.stringify({ type: "error", code: "INVALID_CODE" }));
      return;
    }
    if (this.roomCode && code && code.toUpperCase() !== this.roomCode) {
      ws.send(JSON.stringify({ type: "error", code: "CODE_MISMATCH" }));
      return;
    }
    const session = this.sessions.get(sessionId);
    ws.send(JSON.stringify({
      type: "roomState",
      code: this.roomCode,
      quick: this.quick,
      isHost: session?.isHost,
      clients: Array.from(this.sessions.keys())
    }));
  }
  isValidRoomCode(code) {
    const normalized = code.toUpperCase();
    return /^[A-Z0-9]{6}$/.test(normalized) || /^QK-[A-Z0-9]{6}$/.test(normalized);
  }
  async handleSignaling(payload, fromSessionId) {
    const { kind, sdp, candidate } = payload;
    console.log(`[FightRoom] Signaling from ${fromSessionId}: kind=${kind}`);
    const toSessionId = Array.from(this.sessions.keys()).find((id) => id !== fromSessionId);
    if (!toSessionId) {
      if (!this.pendingSignals.has(fromSessionId)) {
        this.pendingSignals.set(fromSessionId, []);
      }
      this.pendingSignals.get(fromSessionId).push({ kind, sdp, candidate });
      return;
    }
    const toSession = this.sessions.get(toSessionId);
    if (!toSession || toSession.ws.readyState !== WebSocket.OPEN) return;
    toSession.ws.send(JSON.stringify({
      type: "sig",
      from: fromSessionId,
      payload: { kind, sdp, candidate }
    }));
  }
  drainPendingSignals(sessionId) {
    const queued = this.pendingSignals.get(sessionId);
    if (!queued || queued.length === 0) return;
    const peerId = Array.from(this.sessions.keys()).find((id) => id !== sessionId);
    if (!peerId) return;
    const peerSession = this.sessions.get(peerId);
    if (!peerSession || peerSession.ws.readyState !== WebSocket.OPEN) return;
    for (const signal of queued) {
      peerSession.ws.send(JSON.stringify({
        type: "sig",
        from: sessionId,
        payload: signal
      }));
    }
    this.pendingSignals.delete(sessionId);
  }
  handleInput(payload, fromSessionId) {
    const bucketKey = fromSessionId;
    if (!this.rateLimitBuckets.has(bucketKey)) {
      this.rateLimitBuckets.set(bucketKey, { tokens: 100, last: Date.now() });
    }
    const bucket = this.rateLimitBuckets.get(bucketKey);
    const now = Date.now();
    const elapsed = now - bucket.last;
    bucket.tokens = Math.min(100, bucket.tokens + Math.floor(elapsed * 100 / 200));
    bucket.last = now;
    if (bucket.tokens < 1) return;
    bucket.tokens -= 1;
    if (this.relayMode) {
      this.broadcast({ type: "inp", from: fromSessionId, data: payload }, { except: fromSessionId });
    }
  }
  handleRelayRequest(fromSessionId) {
    if (!this.relayMode) {
      this.relayMode = true;
      this.broadcast({ type: "relayMode", status: "activated" });
      console.log("[FightRoom] Relay fallback activated by", fromSessionId);
    }
  }
  handleLobbyMessage(payload, fromSessionId) {
    const { type, data } = payload;
    switch (type) {
      case "ready":
        this.broadcast({ type: "readyStatus", peer: fromSessionId, ready: data?.ready }, { except: fromSessionId });
        break;
      case "lobby":
        this.broadcast({ type: "lobby", peer: fromSessionId, data }, { except: fromSessionId });
        break;
      case "pick":
        this.broadcast({ type: "pick", peer: fromSessionId, pick: data?.pick }, { except: fromSessionId });
        break;
      case "ping":
      case "pong":
        this.broadcast({ type: "lob", peer: fromSessionId, data: { t: type, ts: Date.now() } });
        break;
      default:
        console.warn(`[FightRoom] Unknown lobby type: ${type}`);
    }
  }
  onDisconnect(sessionId) {
    const session = this.sessions.get(sessionId);
    if (!session) return;
    this.sessions.delete(sessionId);
    this.broadcast({ type: "peerleft", peer: sessionId });
    if (session.isHost) {
      const newHostId = Array.from(this.sessions.keys())[0];
      if (newHostId) {
        const newHost = this.sessions.get(newHostId);
        newHost.isHost = true;
        this.broadcast({ type: "hostchange", newHost: newHostId });
      }
    }
    console.log(`[FightRoom] Client left: ${sessionId}, remaining: ${this.sessions.size}`);
  }
  broadcast(message, opts = {}) {
    const msg = JSON.stringify(message);
    for (const [sId, session] of this.sessions) {
      if (opts.except && sId === opts.except) continue;
      if (session.ws.readyState === WebSocket.OPEN) {
        session.ws.send(msg);
      }
    }
  }
  async persistState() {
    await this.ctx.storage.put("roomState", {
      roomCode: this.roomCode,
      quick: this.quick,
      lastInputAt: this.lastInputAt,
      hostSessionId: this.getHostSessionId()
    });
  }
  getHostSessionId() {
    for (const [sessionId, session] of this.sessions) {
      if (session.isHost) return sessionId;
    }
    return null;
  }
  generateSessionId() {
    return `sess-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 8)}`;
  }
  generateCode() {
    const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    let code = "";
    for (let i = 0; i < 6; i++) {
      code += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return code;
  }
};

// www/_worker.js
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
    return doObj.fetch(request);
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

// .wrangler/tmp/bundle-V7jf3B/middleware-insertion-facade.js
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

// .wrangler/tmp/bundle-V7jf3B/middleware-loader.entry.ts
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
