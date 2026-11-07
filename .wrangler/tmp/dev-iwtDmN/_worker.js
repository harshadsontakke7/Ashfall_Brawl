var __defProp = Object.defineProperty;
var __name = (target, value) => __defProp(target, "name", { value, configurable: true });

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

// .wrangler/tmp/bundle-Tkat7V/middleware-insertion-facade.js
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

// .wrangler/tmp/bundle-Tkat7V/middleware-loader.entry.ts
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
  __INTERNAL_WRANGLER_MIDDLEWARE__,
  middleware_loader_entry_default as default
};
//# sourceMappingURL=_worker.js.map
