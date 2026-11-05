// Shared message definitions + validation. Every client message passes through here.
const LOBBY_TYPES = new Set(['join','lobby','pick','ready','rematch','start','delay','ping','pong','bye','arena']);
const HOST_ONLY = new Set(['lobby','start','delay','arena']);   // only the room creator may send these
const STR_KEYS = ['pick','hp','gp','arena'];
const INT_KEYS = ['mid','dl'];
const ID_RE = /^[\w-]{0,24}$/;

const isInt = v => Number.isInteger(v) && Math.abs(v) <= 9007199254740991;

function sanitizeLobby(m) {
  if (!m || typeof m !== 'object' || !LOBBY_TYPES.has(m.t)) return null;
  const o = { t: m.t };
  for (const k of STR_KEYS) if (m[k] !== undefined) { if (typeof m[k] !== 'string' || !ID_RE.test(m[k])) return null; o[k] = m[k]; }
  for (const k of INT_KEYS) if (m[k] !== undefined) { if (!isInt(m[k])) return null; o[k] = m[k]; }
  if (m.ts !== undefined) { if (typeof m.ts !== 'number' || !isFinite(m.ts)) return null; o.ts = m.ts; }
  if (m.ready !== undefined) o.ready = !!m.ready;
  return o;
}

function sanitizeInput(m) {
  if (!m || typeof m !== 'object') return null;
  if (!isInt(m.mid) || !isInt(m.a) || m.a < 0 || !isInt(m.k) || !Array.isArray(m.d) || m.d.length > 32) return null;
  if (m.seq !== undefined && !isInt(m.seq)) return null;
  const d = new Array(m.d.length);
  for (let i = 0; i < m.d.length; i++) { const v = m.d[i]; if (!isInt(v) || v < 0 || v > 4095) return null; d[i] = v; }
  const o = { mid: m.mid, seq: m.seq, a: m.a, d, k: m.k };
  if (m.c !== undefined) { if (!Array.isArray(m.c) || m.c.length !== 2 || !isInt(m.c[0]) || !isInt(m.c[1])) return null; o.c = m.c; }
  return o;
}

// WebRTC signalling (offer/answer/ICE) - relayed between the two peers, never interpreted by the server.
const SIG_KINDS = new Set(['hi','offer','answer','ice']);
function sanitizeSig(m) {
  if (!m || typeof m !== 'object' || !SIG_KINDS.has(m.k)) return null;
  const o = { k: m.k };
  if (m.s !== undefined) { if (typeof m.s !== 'string' || m.s.length > 3000) return null; o.s = m.s; }
  return o;
}

// Simple token bucket: rate tokens/sec, up to burst.
class Bucket {
  constructor(rate, burst) { this.rate = rate; this.burst = burst; this.tokens = burst; this.t = Date.now(); }
  take() {
    const now = Date.now();
    this.tokens = Math.min(this.burst, this.tokens + (now - this.t) * this.rate / 1000);
    this.t = now;
    if (this.tokens < 1) return false;
    this.tokens -= 1; return true;
  }
}

module.exports = { HOST_ONLY, sanitizeLobby, sanitizeInput, sanitizeSig, Bucket };
