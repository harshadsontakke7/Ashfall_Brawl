// Vercel build step: writes public/config.js from the GAME_SERVER_URL environment variable.
const fs = require('fs');
const url = (process.env.GAME_SERVER_URL || '').trim();
if (process.env.VERCEL && !/^wss?:\/\//.test(url)) {
  console.error('GAME_SERVER_URL must be set (e.g. wss://your-app.onrender.com) in Vercel > Settings > Environment Variables.');
  process.exit(1);
}
if (url) fs.writeFileSync('public/config.js', 'window.GAME_SERVER_URL=' + JSON.stringify(url) + ';\n');
