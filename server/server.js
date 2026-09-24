// Signaling server: serves the page and relays messages between the two peers in a room.
// It never touches audio/video. That flows directly browser-to-browser.
const express = require('express');
const http = require('http');
const { WebSocketServer } = require('ws');
require('dotenv').config();
dotenv.config({ path: '.env' });



const app = express();
app.use(express.static('public'));
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

const rooms = new Map(); // roomId -> Set<WebSocket> (max 2)

wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'join') {
      const peers = rooms.get(msg.room) || new Set();
      if (peers.size >= 2) return ws.send(JSON.stringify({ type: 'full' }));
      // Tell the peer already waiting that someone arrived: they will send the offer.
      peers.forEach((p) => p.send(JSON.stringify({ type: 'peer-joined' })));
      peers.add(ws);
      rooms.set(msg.room, peers);
      ws.room = msg.room;
      return;
    }

    // offer / answer / candidate: forward untouched to the other peer
    const peers = rooms.get(ws.room);
    if (!peers) return;
    peers.forEach((p) => { if (p !== ws && p.readyState === 1) p.send(raw.toString()); });
  });

  ws.on('close', () => {
    const peers = rooms.get(ws.room);
    if (!peers) return;
    peers.delete(ws);
    peers.forEach((p) => p.send(JSON.stringify({ type: 'peer-left' })));
    if (peers.size === 0) rooms.delete(ws.room);
  });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`Open http://localhost:${PORT}`));
