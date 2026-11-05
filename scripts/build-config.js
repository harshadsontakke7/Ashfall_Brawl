// Vercel build step: writes public/config.js from environment variables.
//   GAME_SERVER_URL   (required on Vercel)  e.g. wss://your-app.onrender.com
//   ICE_SERVERS_JSON  (optional)            e.g. [{"urls":"stun:stun.l.google.com:19302"},{"urls":"turn:HOST:443","username":"U","credential":"P"}]
const fs = require('fs');
const url = (process.env.GAME_SERVER_URL || '').trim();
if (process.env.VERCEL && !/^wss?:\/\//.test(url)) {
  console.error('GAME_SERVER_URL must be set (e.g. wss://your-app.onrender.com) in Vercel > Settings > Environment Variables.');
  process.exit(1);
}
let ice = '';
const rawIce = (process.env.ICE_SERVERS_JSON || '').trim();
if (rawIce) {
  try { ice = JSON.stringify(JSON.parse(rawIce)); }
  catch (e) { console.error('ICE_SERVERS_JSON is not valid JSON.'); process.exit(1); }
}
if (url) fs.writeFileSync('public/config.js',
  'window.GAME_SERVER_URL=' + JSON.stringify(url) + ';\n' + (ice ? 'window.ICE_SERVERS=' + ice + ';\n' : ''));
