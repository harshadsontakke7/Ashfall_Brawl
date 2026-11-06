// Durable Object class for FightRoom
export class FightRoom {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.clients = new Map(); // sessionId -> ws
    this.hostId = null;
    this.roomCode = null;
    this.quick = false;
    this.lastActivity = Date.now();
    this.messageIdCounter = 0;
    this.pendingSignaling = new Map();
    this.peerConnections = new Map();
    this.dataChannels = new Map();
    this.iceCandidates = new Map();
    this.relayCount = new Map();
    this.relayFallback = false;
    this.hibernationTimer = null;
    this.sessionCounter = 0;

    // Initialize from storage
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
      lastActivity: this.lastActivity,
    };
    await this.ctx.storage.put("roomState", state);
  }

  // Called when a WebSocket client connects via Worker's doObj.accept()
  // roomCode and isQuick are passed from Worker since ws.request isn't available
  accept(ws, roomCode = null, isQuick = false) {
    console.log("[FightRoom] accept called:", { roomCode, isQuick, clients: this.clients.size });
    // Generate unique session ID
    const sessionId = `sess-${++this.sessionCounter}-${Date.now().toString(36)}`;
    ws.sessionId = sessionId;

    this.clients.set(sessionId, ws);
    this.lastActivity = Date.now();

    // If this is the first client, they become host
    if (this.clients.size === 1) {
      this.hostId = sessionId;
      if (!this.roomCode) {
        this.roomCode = isQuick ? `QK-${this.generateCode()}` : (roomCode || this.generateCode());
        this.quick = isQuick;
        this.ctx.waitUntil(this.persistState());
      }
    } else if (!this.roomCode && roomCode) {
      // Second client joining with a room code
      this.roomCode = roomCode.toUpperCase();
      this.quick = isQuick;
      this.ctx.waitUntil(this.persistState());
    }

    console.log("[FightRoom] after accept:", { sessionId, roomCode: this.roomCode, clients: this.clients.size });

    // Send initial room state to client
    const initMsg = JSON.stringify({
      type: "roomInit",
      sessionId,
      code: this.roomCode,
      quick: this.quick,
      host: sessionId === this.hostId,
      clients: this.clients.size,
      maxClients: 2,
    });
    console.log("[FightRoom] Sending roomInit:", initMsg);
    ws.send(initMsg);

    // Notify others of new connection
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
        // Re-join with code - validate and confirm
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
    // Validate room code format
    if (code && !/^[A-Z0-9]{6}$/.test(code.toUpperCase())) {
      ws.send(JSON.stringify({
        type: "error",
        message: "Invalid room code format. Must be 6 uppercase letters/numbers."
      }));
      return;
    }

    // If room already has a code, validate it matches
    if (this.roomCode && code && code.toUpperCase() !== this.roomCode) {
      ws.send(JSON.stringify({
        type: "error",
        message: "Room code mismatch."
      }));
      return;
    }

    // If this is first client and no code yet, set it
    if (!this.roomCode) {
      this.roomCode = (code || this.generateCode()).toUpperCase();
      this.quick = !!quick;
      await this.persistState();
    }

    // Send room state
    ws.send(JSON.stringify({
      type: "roomState",
      code: this.roomCode,
      quick: this.quick,
      host: sessionId === this.hostId,
      clients: Array.from(this.clients.keys()),
    }));
  }

  onClientDisconnect(ws, sessionId) {
    this.clients.delete(sessionId);

    // Notify other client
    this.broadcastToAll("peerleft", { peer: sessionId });

    // If host left, appoint new host
    if (this.hostId === sessionId) {
      const newHost = Array.from(this.clients.keys())[0];
      if (newHost) {
        this.hostId = newHost;
        this.broadcastToAll("hostchange", { newHost });
      }
    }

    console.log(`[FightRoom] Client left: ${sessionId}, remaining: ${this.clients.size}`);

    // If room is empty, schedule hibernation
    if (this.clients.size === 0) {
      this.scheduleHibernation();
    }
  }

  async handleWebRTCSignaling(ws, payload, sessionId) {
    const { kind, sdp, candidate } = payload;
    console.log(`[FightRoom] Signaling from ${sessionId}: kind=${kind}`);

    // Find the other client
    const otherSessionId = Array.from(this.clients.keys()).find(
      id => id !== sessionId
    );

    if (!otherSessionId) {
      // Queue signaling until other client joins
      if (!this.pendingSignaling.has(sessionId)) {
        this.pendingSignaling.set(sessionId, []);
      }
      this.pendingSignaling.get(sessionId).push({ ws, payload, sessionId });
      return;
    }

    const otherWs = this.clients.get(otherSessionId);
    if (!otherWs) return;

    if (candidate) {
      // ICE candidate - forward to peer
      this.iceCandidates.set(sessionId, candidate);
      otherWs.send(JSON.stringify({
        type: "ice-candidate",
        target: sessionId,
        candidate: candidate,
      }));
    } else if (sdp) {
      // SDP offer/answer - forward to peer
      if (sdp.type === "offer") {
        otherWs.send(JSON.stringify({
          type: "sdp-offer",
          target: sessionId,
          sdp: sdp,
        }));
      } else if (sdp.type === "answer") {
        otherWs.send(JSON.stringify({
          type: "sdp-answer",
          target: sessionId,
          sdp: sdp,
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

    // Rate limiting: 100 tokens per 200ms ≈ 500 inputs/sec max burst
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
      return; // Rate limited
    }
    bucket.tokens -= 1;

    // Relay to other client via DataChannel if P2P active
    const otherSessionId = Array.from(this.clients.keys()).find(
      id => id !== fromSessionId
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

    // Fallback: broadcast via Durable Object WS
    this.broadcastToAll("inp", { peer: fromSessionId, data: inputData }, { except: ws });
  }

  handleInput(ws, inputData, sessionId) {
    this.lastActivity = Date.now();

    // Rate limiting for relay inputs
    if (!this.relayCount.has(sessionId)) {
      this.relayCount.set(sessionId, { tokens: 100, last: Date.now() });
    }
    const bucket = this.relayCount.get(sessionId);
    const now = Date.now();
    const elapsed = now - bucket.last;
    bucket.tokens = Math.min(100, bucket.tokens + Math.floor(elapsed * 100 / 200));
    bucket.last = now;

    if (bucket.tokens < 1) {
      return; // Rate limited
    }
    bucket.tokens -= 1;

    // Broadcast via relay
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
    }, 5 * 60 * 1000); // 5 minutes
  }

  async hibernate() {
    // Persist state before hibernation
    await this.persistState();
    console.log("[FightRoom] State persisted for hibernation:", this.roomCode);

    // Close all connections
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
}

// Worker entrypoint
export default {
  async fetch(request, env, ctx) {
    const url = new URL(request.url);
    const path = url.pathname;

    // Handle WebSocket upgrade requests
    if (request.headers.get("upgrade") === "websocket") {
      return this.handleWebSocketUpgrade(request, env, ctx);
    }

    // HTTP routes
    if (path === "/") {
      return new Response("Ashfall Brawl Workers API", { status: 200 });
    }

    if (path === "/health") {
      return new Response(
        JSON.stringify({ ok: true, env: "configured" }),
        { status: 200, headers: { "Content-Type": "application/json" } }
      );
    }

    // Room creation endpoint
    if (path === "/api/create-room") {
      return this.handleCreateRoom(request, env, ctx);
    }

    // Quick matchmaking endpoint
    if (path === "/api/quick-match") {
      return this.handleQuickMatchmaking(request, env, ctx);
    }

    // Join room endpoint
    if (path === "/api/join-room") {
      return this.handleJoinRoom(request, env, ctx);
    }

    return new Response("Not found", { status: 404 });
  },

  async handleWebSocketUpgrade(request, env, ctx) {
    console.log("[Worker] WebSocket upgrade request:", request.url);
    // Create WebSocket pair - DON'T call client.accept() manually
    const pair = new WebSocketPair();
    const [client, server] = pair;

    // Get room code from URL
    const url = new URL(request.url);
    const roomCode = url.pathname.split("/").pop() || url.searchParams.get("code");
    const isQuick = url.searchParams.get("quick") === "true";

    // Bind to specific Durable Object based on room code
    let doId;
    if (roomCode) {
      // Deterministic DO ID from room code
      const encoder = new TextEncoder();
      const data = encoder.encode(roomCode.toUpperCase());
      const hash = await crypto.subtle.digest("SHA-1", data);
      const hashArray = new Uint8Array(hash);
      doId = `fight-${hashArray[0].toString(16)}${hashArray[1].toString(16)}${hashArray[2].toString(16)}${hashArray[3].toString(16)}`;
    } else {
      // No code - create temp room
      doId = `fight-temp-${Date.now().toString(36)}`;
    }

    console.log("[Worker] DO ID:", doId, "roomCode:", roomCode, "isQuick:", isQuick);

    // Get the Durable Object stub and forward the WebSocket
    const stub = env.FIGHT_ROOM.idFromName(doId);
    const doObj = env.FIGHT_ROOM.get(stub);

    // Pass the server WebSocket to the DO with roomCode and isQuick
    console.log("[Worker] Calling doObj.accept()");
    doObj.accept(server, roomCode, isQuick);
    console.log("[Worker] doObj.accept() returned");

    // Return the client WebSocket - cloudflare will accept it automatically
    return new Response(null, {
      status: 101,
      webSocket: client,
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
        wsUrl: `wss://${env.WORKER_URL?.replace(/^https?:\/\//, '') || "ashfallbrawl.workers.dev"}/${code}`,
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  },

  async handleQuickMatchmaking(request, env, ctx) {
    // Quick matchmaking: create a "quick" room
    const roomCode = `QK-${this.generateRoomCode().toUpperCase()}`;

    return new Response(
      JSON.stringify({
        roomCode,
        quick: true,
        wsUrl: `wss://${env.WORKER_URL?.replace(/^https?:\/\//, '') || "ashfallbrawl.workers.dev"}/${roomCode}`,
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
        wsUrl: `wss://${env.WORKER_URL?.replace(/^https?:\/\//, '') || "ashfallbrawl.workers.dev"}/${roomCode}`,
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
  },
};