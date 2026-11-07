import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";
import { WebSocketServer } from "ws";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC = path.join(__dirname, "public");
const PHOTOS = path.join(__dirname, "photos");
const PORT = Number(process.env.PORT) || 3000;

fs.mkdirSync(PHOTOS, { recursive: true });

const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript",
  ".css": "text/css",
  ".jpg": "image/jpeg",
};

const server = http.createServer((req, res) => {
  const url = new URL(req.url, "http://x");
  let pathname = decodeURIComponent(url.pathname);
  if (pathname === "/") pathname = "/index.html";
  if (pathname === "/camera" || pathname === "/remote") pathname += ".html";

  const base = pathname.startsWith("/photos/") ? __dirname : PUBLIC;
  const file = path.normalize(path.join(base, pathname));
  if (!file.startsWith(base + path.sep)) {
    res.writeHead(403).end();
    return;
  }
  fs.readFile(file, (err, data) => {
    if (err) {
      res.writeHead(404).end("Not found");
      return;
    }
    res.writeHead(200, { "Content-Type": TYPES[path.extname(file)] || "application/octet-stream" });
    res.end(data);
  });
});

// rooms: name -> Set<ws>; each ws has .id, .role ("camera" | "remote"), .room,
// and (remotes only) .jpeg = true when it wants fallback JPEG preview frames
const rooms = new Map();
const wss = new WebSocketServer({ server, maxPayload: 20 * 1024 * 1024 });

function peers(room) {
  return rooms.get(room) || new Set();
}

function send(ws, msg) {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
}

function broadcast(room, role, msg) {
  for (const ws of peers(room)) if (ws.role === role) send(ws, msg);
}

function announceJpegViewers(room) {
  const count = [...peers(room)].filter((w) => w.role === "remote" && w.jpeg).length;
  broadcast(room, "camera", { type: "jpegViewers", count });
}

function announce(room) {
  const list = [...peers(room)];
  const cameras = list.filter((w) => w.role === "camera").length;
  const remotes = list.filter((w) => w.role === "remote").length;
  for (const ws of list) send(ws, { type: "presence", cameras, remotes });
}

wss.on("connection", (ws, req) => {
  const url = new URL(req.url, "http://x");
  ws.id = randomUUID();
  ws.room = url.searchParams.get("room") || "default";
  ws.role = url.searchParams.get("role") === "camera" ? "camera" : "remote";

  if (!rooms.has(ws.room)) rooms.set(ws.room, new Set());
  rooms.get(ws.room).add(ws);
  announce(ws.room);
  if (ws.role === "camera") announceJpegViewers(ws.room);

  ws.on("message", (raw) => {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }

    if (ws.role === "remote" && msg.type === "shoot") {
      const delay = Math.max(0, Math.min(10, Number(msg.delay) || 0));
      broadcast(ws.room, "camera", { type: "shoot", delay });
    } else if (ws.role === "remote" && msg.type === "signal") {
      // WebRTC offer/ICE from a remote -> the room's camera, tagged with who sent it
      const camera = [...peers(ws.room)].find((w) => w.role === "camera");
      if (camera) send(camera, { type: "signal", from: ws.id, data: msg.data });
    } else if (ws.role === "camera" && msg.type === "signal") {
      // WebRTC answer/ICE from the camera -> the specific remote it's for
      const target = [...peers(ws.room)].find((w) => w.id === msg.to);
      if (target) send(target, { type: "signal", data: msg.data });
    } else if (ws.role === "remote" && msg.type === "preview") {
      ws.jpeg = msg.mode === "jpeg";
      announceJpegViewers(ws.room);
    } else if (ws.role === "camera" && msg.type === "frame" && typeof msg.data === "string") {
      for (const w of peers(ws.room)) if (w.role === "remote" && w.jpeg) send(w, { type: "frame", data: msg.data });
    } else if (ws.role === "camera" && msg.type === "countdown") {
      broadcast(ws.room, "remote", { type: "countdown", remaining: msg.remaining });
    } else if (ws.role === "camera" && msg.type === "photo" && typeof msg.data === "string") {
      const b64 = msg.data.replace(/^data:image\/jpeg;base64,/, "");
      const name = `photo-${new Date().toISOString().replace(/[:.]/g, "-")}.jpg`;
      fs.writeFile(path.join(PHOTOS, name), Buffer.from(b64, "base64"), (err) => {
        if (err) console.error("Failed to save photo:", err);
        else console.log(`Saved photos/${name}`);
        broadcast(ws.room, "remote", { type: "photo", url: `/photos/${name}` });
      });
    }
  });

  ws.on("close", () => {
    const set = rooms.get(ws.room);
    set?.delete(ws);
    if (set && set.size === 0) rooms.delete(ws.room);
    else {
      announce(ws.room);
      if (ws.role === "remote" && ws.jpeg) announceJpegViewers(ws.room);
    }
    if (ws.role === "remote") broadcast(ws.room, "camera", { type: "peerLeft", id: ws.id });
  });
});

function lanAddresses() {
  return Object.values(os.networkInterfaces())
    .flat()
    .filter((i) => i && i.family === "IPv4" && !i.internal)
    .map((i) => i.address);
}

server.listen(PORT, () => {
  console.log(`\nCamera (open on your Mac):  http://localhost:${PORT}/camera`);
  for (const ip of lanAddresses()) {
    console.log(`Remote (open on your phone): http://${ip}:${PORT}/remote`);
  }
  console.log(`\nPhotos are saved to ${PHOTOS}\n`);
});
