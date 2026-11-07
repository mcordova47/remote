import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { fileURLToPath } from "node:url";
import { execFile } from "node:child_process";
import { randomUUID, randomBytes, timingSafeEqual } from "node:crypto";
import { WebSocketServer } from "ws";
import qrcode from "qrcode-terminal";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PUBLIC = path.join(__dirname, "public");
const PHOTOS = path.resolve(process.env.PHOTOS_DIR || path.join(__dirname, "photos"));
const PORT = Number(process.env.PORT) || 3000;

fs.mkdirSync(PHOTOS, { recursive: true });

const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript",
  ".css": "text/css",
  ".jpg": "image/jpeg",
};

// --- Saving to Photos ---
// Each photo is imported into a "Remote Shutter" album in the Mac's Photos app (and from
// there syncs to your phone if iCloud Photos is on). The file in photos/ is kept as a
// backup. The first import makes macOS ask whether this terminal may control Photos.
const ALBUM = process.env.PHOTOS_ALBUM || "Remote Shutter";
const SAVE_TO_PHOTOS = process.platform === "darwin" && process.env.SAVE_TO_PHOTOS !== "0";
const IMPORT_SCRIPT = [
  "on run argv",
  "  set f to (POSIX file (item 1 of argv)) as alias",
  '  tell application "Photos"',
  "    if not (exists album (item 2 of argv)) then make new album named (item 2 of argv)",
  "    import {f} into album (item 2 of argv) skip check duplicates true",
  "  end tell",
  "end run",
];

// Imports run one at a time so quick bursts of shots don't race each other.
let importQueue = Promise.resolve();

function importToPhotos(file) {
  const run = () =>
    new Promise((resolve, reject) => {
      const args = [...IMPORT_SCRIPT.flatMap((line) => ["-e", line]), file, ALBUM];
      // Long timeout: the first run waits on the macOS permission prompt.
      execFile("osascript", args, { timeout: 120_000 }, (err, _out, stderr) =>
        err ? reject(new Error(stderr.trim() || err.message)) : resolve()
      );
    });
  const result = importQueue.then(run);
  importQueue = result.catch(() => {});
  return result;
}

// --- Access key ---
// A new random key every run. Phones get it by scanning the QR code printed at startup,
// which sets a cookie; restarting the server invalidates every old cookie. Requests made
// directly on this Mac (localhost) don't need it, so the camera page just works.
const KEY = randomBytes(18).toString("base64url");
const COOKIE = "rs_key";

function isLocal(req) {
  const addr = req.socket.remoteAddress;
  const host = (req.headers.host || "").replace(/:\d+$/, "");
  return (
    ["127.0.0.1", "::1", "::ffff:127.0.0.1"].includes(addr) &&
    ["localhost", "127.0.0.1", "[::1]"].includes(host) &&
    !req.headers["x-forwarded-for"] // a tunnel (e.g. cloudflared) also connects from localhost
  );
}

function keyMatches(value) {
  const a = Buffer.from(value || "");
  const b = Buffer.from(KEY);
  return a.length === b.length && timingSafeEqual(a, b);
}

function cookieKey(req) {
  return (req.headers.cookie || "").match(new RegExp(`(?:^|;\\s*)${COOKIE}=([^;]+)`))?.[1];
}

function authorized(req) {
  return isLocal(req) || keyMatches(cookieKey(req));
}

const LOCKED_PAGE = `<!doctype html><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Remote Shutter</title>
<body style="background:#111;color:#eee;font-family:-apple-system,system-ui,sans-serif;text-align:center;padding:48px 24px">
<h1 style="font-size:22px">Scan to connect</h1>
<p>Scan the QR code shown in the terminal where the server is running.</p>`;

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, "http://x");
  let pathname = decodeURIComponent(url.pathname);

  // Visiting any page with ?key=<KEY> (the QR code link) logs this device in.
  if (url.searchParams.has("key")) {
    url.searchParams.delete("key");
    const location = url.pathname + url.search;
    if (keyMatches(new URL(req.url, "http://x").searchParams.get("key"))) {
      res.writeHead(302, {
        "Set-Cookie": `${COOKIE}=${KEY}; Path=/; HttpOnly; SameSite=Lax`,
        Location: location,
      });
    } else {
      res.writeHead(302, { Location: location });
    }
    res.end();
    return;
  }

  if (!authorized(req)) {
    const wantsPage = (req.headers.accept || "").includes("text/html");
    res.writeHead(401, { "Content-Type": wantsPage ? "text/html; charset=utf-8" : "text/plain" });
    res.end(wantsPage ? LOCKED_PAGE : "Unauthorized");
    return;
  }

  if (pathname === "/") pathname = "/index.html";
  if (["/camera", "/remote"].includes(pathname)) pathname += ".html";

  const isPhoto = pathname.startsWith("/photos/");
  const base = isPhoto ? PHOTOS : PUBLIC;
  const file = path.normalize(path.join(base, isPhoto ? pathname.slice("/photos".length) : pathname));
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
const wss = new WebSocketServer({
  server,
  maxPayload: 20 * 1024 * 1024,
  verifyClient: ({ req }) => authorized(req),
});

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
      const file = path.join(PHOTOS, name);
      const url = `/photos/${name}`;
      fs.writeFile(file, Buffer.from(b64, "base64"), (err) => {
        if (err) {
          console.error("Failed to save photo:", err);
          broadcast(ws.room, "remote", { type: "saved", url, ok: false, error: "Couldn't save the photo" });
          return;
        }
        broadcast(ws.room, "remote", { type: "photo", url, toPhotos: SAVE_TO_PHOTOS });
        if (!SAVE_TO_PHOTOS) return console.log(`Saved photos/${name}`);
        importToPhotos(file).then(
          () => {
            console.log(`Saved photos/${name} and added it to the "${ALBUM}" album in Photos`);
            broadcast(ws.room, "remote", { type: "saved", url, ok: true });
          },
          (e) => {
            console.error(`Saved photos/${name}, but couldn't add it to Photos: ${e.message}`);
            console.error("If macOS blocked it: System Settings › Privacy & Security › Automation › allow this terminal to control Photos.");
            broadcast(ws.room, "remote", { type: "saved", url, ok: false, error: "Couldn't add to Photos (saved on the Mac)" });
          }
        );
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

// The address the phone should use. Skips virtual adapters that VM/container tools
// (bridge*, vmnet*, docker*) and VPNs (utun*) create, which a phone can't reach.
// Set REMOTE_HOST to override.
function lanAddress() {
  if (process.env.REMOTE_HOST) return process.env.REMOTE_HOST;
  const virtual = /^(bridge|utun|vmnet|docker|veth|llw|awdl|tun|tap)/;
  const candidates = Object.entries(os.networkInterfaces())
    .filter(([name]) => !virtual.test(name))
    .flatMap(([, addrs]) => addrs)
    .filter((i) => i && i.family === "IPv4" && !i.internal);
  return candidates[0]?.address || "localhost";
}

server.listen(PORT, () => {
  const remoteUrl = `http://${lanAddress()}:${PORT}/remote?key=${KEY}`;
  console.log(`\nCamera (open on this Mac): http://localhost:${PORT}/camera`);
  console.log(`\nRemote: scan with your phone (new code every run)\n`);
  qrcode.generate(remoteUrl, { small: true }, (qr) => console.log(qr));
  console.log(`${remoteUrl}\n`);
  if (SAVE_TO_PHOTOS) console.log(`Photos go to the "${ALBUM}" album in Photos (copies kept in ${PHOTOS})\n`);
  else console.log(`Photos are saved to ${PHOTOS}\n`);
});
