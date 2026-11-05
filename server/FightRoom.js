const { Room } = require('@colyseus/core');
const { HOST_ONLY, sanitizeLobby, sanitizeInput, Bucket } = require('./protocol');

const CODE_RE = /^[A-Z]{6}$/;
const RECONNECT_SECONDS = 20;
const IDLE_MS = 10 * 60 * 1000;
const MAX_DROPS = 500;

// Relay room for a 2-player deterministic-lockstep game. The server owns room lifecycle,
// seats, host rights, validation and rate limits; clients run the identical simulation.
class FightRoom extends Room {
  maxClients = 2;

  onCreate(options) {
    const code = String((options && options.code) || '').toUpperCase();
    if (!CODE_RE.test(code)) throw new Error('Invalid room code');
    this.quick = !!(options && options.quick);
    this.setMetadata({ quick: this.quick });   // the room code is only known via the filterBy option
    this.hostId = null;
    console.log('[room ' + code + '] created' + (this.quick ? ' (quick)' : ''));
    this.lastActivity = Date.now();
    this.buckets = new Map();
    this.drops = new Map();

    this.onMessage('lob', (c, raw) => this.relay(c, 'lob', sanitizeLobby(raw), 'lob'));
    this.onMessage('inp', (c, raw) => this.relay(c, 'inp', sanitizeInput(raw), 'inp'));
    this.onMessage('*', () => {});

    // reap rooms nobody is using (abandoned lobby or frozen match)
    this.clock.setInterval(() => {
      if (Date.now() - this.lastActivity > IDLE_MS) this.disconnect();
    }, 60000);
  }

  relay(client, type, data, bucketKey) {
    let b = this.buckets.get(client.sessionId + bucketKey);
    if (!b) { b = bucketKey === 'inp' ? new Bucket(100, 200) : new Bucket(30, 60); this.buckets.set(client.sessionId + bucketKey, b); }
    if (!data || !b.take() || (type === 'lob' && HOST_ONLY.has(data.t) && client.sessionId !== this.hostId)) {
      const n = (this.drops.get(client.sessionId) || 0) + 1;
      this.drops.set(client.sessionId, n);
      if (n > MAX_DROPS) client.leave(4000);     // abusive client
      return;
    }
    this.lastActivity = Date.now();
    if (data.t !== 'ping' && data.t !== 'pong' && type === 'lob') console.log('[room ' + this.roomId + '] lob ' + data.t + ' from ' + client.sessionId);
    this.broadcast(type, { peer: client.sessionId, data }, { except: client });
  }

  onJoin(client) {
    if (!this.hostId) this.hostId = client.sessionId;
    console.log('[room ' + this.roomId + '] join ' + client.sessionId + ' (' + (this.clients.length) + '/2)' + (client.sessionId === this.hostId ? ' host' : ''));
    this.lastActivity = Date.now();
    this.broadcast('peerjoin', { peer: client.sessionId }, { except: client });
  }

  async onLeave(client, consented) {
    console.log('[room ' + this.roomId + '] leave ' + client.sessionId + ' consented=' + consented);
    if (!consented) {
      try { await this.allowReconnection(client, RECONNECT_SECONDS); return; } catch (_) { /* timed out */ }
    }
    this.buckets.delete(client.sessionId + 'lob'); this.buckets.delete(client.sessionId + 'inp');
    this.drops.delete(client.sessionId);
    this.broadcast('peerleft', { peer: client.sessionId });
  }
}

module.exports = { FightRoom };
