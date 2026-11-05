// Ashfall Brawl multiplayer server (Express + Colyseus). Deploy on Render as a Web Service.
const http = require('http');
const express = require('express');
const cors = require('cors');
const { Server } = require('colyseus');
const { WebSocketTransport } = require('@colyseus/ws-transport');
const { FightRoom } = require('./FightRoom');

const PORT = Number(process.env.PORT) || 2567;
const ORIGINS = (process.env.ALLOWED_ORIGINS || '').split(',').map(s => s.trim().replace(/\/$/, '')).filter(Boolean);
if (!ORIGINS.length) console.warn('ALLOWED_ORIGINS is not set: all origins are accepted. Set it to your Vercel URL.');
const allowed = o => !ORIGINS.length || !o || ORIGINS.includes(o);

const app = express();
app.disable('x-powered-by');
app.use(cors({ origin: (o, cb) => cb(null, allowed(o)) }));
app.get('/', (_req, res) => res.send('Ashfall Brawl server OK'));
app.get('/health', (_req, res) => res.json({ ok: true, uptime: Math.round(process.uptime()) }));

const httpServer = http.createServer(app);
const gameServer = new Server({
  transport: new WebSocketTransport({
    server: httpServer,
    maxPayload: 4096,
    pingInterval: 5000,
    pingMaxRetries: 3,
    verifyClient: (info, cb) => cb(allowed(info.origin)),
  }),
});
gameServer.define('fight', FightRoom).filterBy(['code']);
gameServer.listen(PORT).then(() => console.log('Listening on :' + PORT));

process.on('uncaughtException', e => console.error('uncaughtException', e));
process.on('unhandledRejection', e => console.error('unhandledRejection', e));
const shutdown = sig => {
  console.log(sig + ': shutting down');
  gameServer.gracefullyShutdown().catch(() => {}).finally(() => process.exit(0));
  setTimeout(() => process.exit(0), 10000).unref();
};
process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
