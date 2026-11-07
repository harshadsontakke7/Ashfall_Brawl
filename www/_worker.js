// Export Durable Object class for Wrangler
export { FightRoom } from "../durableObjects/FightRoom.js";

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

    // Get the Durable Object stub and forward the request
    // The DO's fetch() handler will handle the WebSocket upgrade
    const stub = env.FIGHT_ROOM.idFromName(doId);
    const doObj = env.FIGHT_ROOM.get(stub);

    // Forward the request to DO - DO will handle WebSocket upgrade
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