# Ashfall Brawl – deployment

## 1. Backend (Render)
1. Push this folder to GitHub. In Render: New > Blueprint (uses `render.yaml`), or New > Web Service with root dir `server`, build `npm install`, start `npm start`.
2. Set env var `ALLOWED_ORIGINS` to your Vercel URL (comma-separate multiple). Health check: `/health`.
3. Use a paid instance for live play: the free tier sleeps and drops WebSockets.

## 2. Frontend (Vercel)
1. Import the repo in Vercel (framework: Other). `vercel.json` sets the build command and output directory `public`.
2. In Vercel > Settings > Environment Variables set `GAME_SERVER_URL=wss://YOUR-SERVICE.onrender.com` (the build writes it into `public/config.js`).

## Local test (public/config.js already points to ws://localhost:2567)
cd server && npm install && npm start   (ws://localhost:2567), then open index.html via any static server.
