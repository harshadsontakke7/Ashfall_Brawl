export class FightRoom {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
    this.sessions = new Map(); // sessionId -> { ws, isHost, token, joinedAt }
    this.roomCode = null;
    this.quick = false;
    this.lastInputAt = 0;
    this.relayMode = false;
    this.pendingSignals = new Map(); // sessionId -> array of { kind, sdp, candidate }
    this.rateLimitBuckets = new Map();

    // Restore hibernated WebSockets
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

    // Create WebSocket pair
    const pair = new WebSocketPair();
    const [client, server] = pair;

    // Generate session ID early
    const sessionId = this.generateSessionId();

    // Determine if host
    const isHost = this.sessions.size === 0;

    // If no room code yet, generate one
    if (!this.roomCode) {
      this.roomCode = isQuick ? `QK-${this.generateCode()}` : (roomCode || this.generateCode());
      this.quick = isQuick;
      await this.persistState();
    } else if (roomCode && roomCode.toUpperCase() !== this.roomCode) {
      // Room exists but code mismatch
      server.close(1008, "Room code mismatch");
      return new Response(null, { status: 101, webSocket: client });
    }

    // Check capacity (max 2 players)
    if (this.sessions.size >= 2) {
      server.close(1003, "Room full (max 2 players)");
      return new Response(null, { status: 101, webSocket: client });
    }

    // Generate session token for reconnection
    const token = crypto.randomUUID();
    await this.ctx.storage.put(`session:${token}`, {
      sessionId,
      isHost,
      roomCode: this.roomCode
    }, { expirationTtl: 86400 });

    // Attach metadata to WebSocket for hibernation
    server.serializeAttachment({ sessionId, isHost, token, joinedAt: Date.now() });

    // Accept server WS for hibernation (must be done before setting handlers)
    this.ctx.acceptWebSocket(server);

    // Set up message handlers IMMEDIATELY after acceptWebSocket
    server.onmessage = (event) => this.onWebSocketMessage(server, event.data);
    server.onclose = () => this.onWebSocketClose(server);
    server.onerror = (err) => this.onWebSocketError(server, err);

    // Store session
    this.sessions.set(sessionId, { ws: server, isHost, token, joinedAt: Date.now() });

    // Send roomInit to new client
    server.send(JSON.stringify({
      type: "roomInit",
      sessionId,
      code: this.roomCode,
      quick: this.quick,
      isHost,
      maxClients: 2,
      sessionToken: token,
    }));

    // Notify peer of join
    this.broadcast({ type: "peerjoin", sessionId, isHost }, { except: sessionId });

    // Drain pending signals for this session
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

    // Reconnection with session token
    if (sessionToken) {
      const stored = await this.ctx.storage.get(`session:${sessionToken}`);
      if (stored && stored.sessionId === sessionId && stored.roomCode === this.roomCode) {
        // Valid reconnection - update session
        const session = this.sessions.get(sessionId);
        if (session) {
          session.token = sessionToken;
          session.ws = ws;
          ws.serializeAttachment({ sessionId, isHost: session.isHost, token: sessionToken, joinedAt: session.joinedAt });
          ws.send(JSON.stringify({ type: "reconnected", sessionId, isHost: session.isHost }));
          this.drainPendingSignals(sessionId);
          return;
        }
      }
    }

    // Validate room code format
    if (code && !this.isValidRoomCode(code)) {
      ws.send(JSON.stringify({ type: "error", code: "INVALID_CODE" }));
      return;
    }

    // If room has code, validate match
    if (this.roomCode && code && code.toUpperCase() !== this.roomCode) {
      ws.send(JSON.stringify({ type: "error", code: "CODE_MISMATCH" }));
      return;
    }

    // Send room state
    const session = this.sessions.get(sessionId);
    ws.send(JSON.stringify({
      type: "roomState",
      code: this.roomCode,
      quick: this.quick,
      isHost: session?.isHost,
      clients: Array.from(this.sessions.keys()),
    }));
  }

  isValidRoomCode(code) {
    const normalized = code.toUpperCase();
    // Normal: 6 chars A-Z0-9; Quick: QK- + 6 chars
    return /^[A-Z0-9]{6}$/.test(normalized) || /^QK-[A-Z0-9]{6}$/.test(normalized);
  }

  async handleSignaling(payload, fromSessionId) {
    const { kind, sdp, candidate } = payload;
    console.log(`[FightRoom] Signaling from ${fromSessionId}: kind=${kind}`);

    // Find the other session
    const toSessionId = Array.from(this.sessions.keys()).find(id => id !== fromSessionId);

    if (!toSessionId) {
      // Queue until peer joins
      if (!this.pendingSignals.has(fromSessionId)) {
        this.pendingSignals.set(fromSessionId, []);
      }
      this.pendingSignals.get(fromSessionId).push({ kind, sdp, candidate });
      return;
    }

    const toSession = this.sessions.get(toSessionId);
    if (!toSession || toSession.ws.readyState !== WebSocket.OPEN) return;

    // Forward signaling message
    toSession.ws.send(JSON.stringify({
      type: "sig",
      from: fromSessionId,
      payload: { kind, sdp, candidate },
    }));
  }

  drainPendingSignals(sessionId) {
    const queued = this.pendingSignals.get(sessionId);
    if (!queued || queued.length === 0) return;

    const peerId = Array.from(this.sessions.keys()).find(id => id !== sessionId);
    if (!peerId) return;

    const peerSession = this.sessions.get(peerId);
    if (!peerSession || peerSession.ws.readyState !== WebSocket.OPEN) return;

    for (const signal of queued) {
      peerSession.ws.send(JSON.stringify({
        type: "sig",
        from: sessionId,
        payload: signal,
      }));
    }
    this.pendingSignals.delete(sessionId);
  }

  handleInput(payload, fromSessionId) {
    // Rate limiting: 100 tokens per 200ms
    const bucketKey = fromSessionId;
    if (!this.rateLimitBuckets.has(bucketKey)) {
      this.rateLimitBuckets.set(bucketKey, { tokens: 100, last: Date.now() });
    }
    const bucket = this.rateLimitBuckets.get(bucketKey);
    const now = Date.now();
    const elapsed = now - bucket.last;
    bucket.tokens = Math.min(100, bucket.tokens + Math.floor(elapsed * 100 / 200));
    bucket.last = now;

    if (bucket.tokens < 1) return; // Rate limited
    bucket.tokens -= 1;

    // In relay mode, broadcast via DO WebSocket
    if (this.relayMode) {
      this.broadcast({ type: "inp", from: fromSessionId, data: payload }, { except: fromSessionId });
    }
    // In P2P mode, DO does NOT relay inp — browsers exchange directly via DataChannel
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

    // Notify other client
    this.broadcast({ type: "peerleft", peer: sessionId });

    // If host left, appoint new host
    if (session.isHost) {
      const newHostId = Array.from(this.sessions.keys())[0];
      if (newHostId) {
        const newHost = this.sessions.get(newHostId);
        newHost.isHost = true;
        this.broadcast({ type: "hostchange", newHost: newHostId });
      }
    }

    console.log(`[FightRoom] Client left: ${sessionId}, remaining: ${this.sessions.size}`);

    // If room empty, DO will hibernate automatically (no connected WebSockets)
    // State is already persisted via storage
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
      hostSessionId: this.getHostSessionId(),
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
}