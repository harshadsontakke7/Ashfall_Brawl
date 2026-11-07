export default {
  async fetch(request, env, ctx): Promise<Response> {
    // Handle WebSocket upgrade for Durable Object connections
    if (request.headers.get("upgrade") === "websocket") {
      const cfId = request.headers.get("sec-websocket-key");
      // Accept the WebSocket upgrade
      const pair = request.accept("websocket", cfId);
      // Bind the Durable Object - the framework will route to FightRoom
      // This is handled by the wrangler.toml durable_objects config
      // We just need to accept and let the binding happen
      return pair[0];
    }

    return new Response("Ashfall Brawl Workers", { status: 200 });
  },
};