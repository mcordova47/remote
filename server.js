import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";
import { fileURLToPath } from "node:url";
import zlib from "node:zlib";
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

const PHOTO_NAME = /^photo-[\w-]+\.jpg$/;

async function listPhotos() {
  const names = (await fs.promises.readdir(PHOTOS)).filter((n) => PHOTO_NAME.test(n));
  const photos = await Promise.all(
    names.map(async (name) => {
      const { mtimeMs, size } = await fs.promises.stat(path.join(PHOTOS, name));
      return { name, url: `/photos/${name}`, time: mtimeMs, size };
    })
  );
  return photos.sort((a, b) => b.time - a.time);
}

// Streams an uncompressed ("stored") zip of every photo. JPEGs don't compress further,
// so storing them is just as small and avoids a dependency.
async function sendZip(res) {
  const photos = await listPhotos();
  res.writeHead(200, {
    "Content-Type": "application/zip",
    "Content-Disposition": `attachment; filename="photos-${new Date().toISOString().slice(0, 10)}.zip"`,
  });

  const central = [];
  let offset = 0;
  for (const { name, time } of photos) {
    const data = await fs.promises.readFile(path.join(PHOTOS, name));
    const nameBuf = Buffer.from(name);
    const crc = zlib.crc32(data);
    const d = new Date(time);
    const dosTime = (d.getHours() << 11) | (d.getMinutes() << 5) | (d.getSeconds() >> 1);
    const dosDate = ((d.getFullYear() - 1980) << 9) | ((d.getMonth() + 1) << 5) | d.getDate();

    const local = Buffer.alloc(30);
    local.writeUInt32LE(0x04034b50, 0); // local file header signature
    local.writeUInt16LE(20, 4); // version needed
    local.writeUInt16LE(dosTime, 10);
    local.writeUInt16LE(dosDate, 12);
    local.writeUInt32LE(crc, 14);
    local.writeUInt32LE(data.length, 18); // compressed size
    local.writeUInt32LE(data.length, 22); // uncompressed size
    local.writeUInt16LE(nameBuf.length, 26);
    res.write(local);
    res.write(nameBuf);
    res.write(data);

    const entry = Buffer.alloc(46);
    entry.writeUInt32LE(0x02014b50, 0); // central directory header signature
    entry.writeUInt16LE(20, 4); // version made by
    entry.writeUInt16LE(20, 6); // version needed
    entry.writeUInt16LE(dosTime, 12);
    entry.writeUInt16LE(dosDate, 14);
    entry.writeUInt32LE(crc, 16);
    entry.writeUInt32LE(data.length, 20);
    entry.writeUInt32LE(data.length, 24);
    entry.writeUInt16LE(nameBuf.length, 28);
    entry.writeUInt32LE(offset, 42); // where this file's local header starts
    central.push(entry, nameBuf);
    offset += local.length + nameBuf.length + data.length;
  }

  const centralBuf = Buffer.concat(central);
  const end = Buffer.alloc(22);
  end.writeUInt32LE(0x06054b50, 0); // end of central directory signature
  end.writeUInt16LE(photos.length, 8);
  end.writeUInt16LE(photos.length, 10);
  end.writeUInt32LE(centralBuf.length, 12);
  end.writeUInt32LE(offset, 16);
  res.end(Buffer.concat([centralBuf, end]));
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

  try {
    if (req.method === "DELETE" && pathname.startsWith("/api/photos")) {
      // DELETE /api/photos deletes every photo; DELETE /api/photos/<name> deletes one
      const name = pathname.slice("/api/photos/".length);
      if (name && !PHOTO_NAME.test(name)) {
        res.writeHead(400).end();
        return;
      }
      const names = name ? [name] : (await listPhotos()).map((p) => p.name);
      await Promise.all(names.map((n) => fs.promises.rm(path.join(PHOTOS, n), { force: true })));
      console.log(`Deleted ${names.length} photo${names.length === 1 ? "" : "s"}`);
      res.writeHead(204).end();
      return;
    }
    if (pathname === "/api/photos") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(await listPhotos()));
      return;
    }
    if (pathname === "/photos.zip") {
      await sendZip(res);
      return;
    }
  } catch (err) {
    console.error(err);
    if (!res.headersSent) res.writeHead(500);
    res.end();
    return;
  }

  if (pathname === "/") pathname = "/index.html";
  if (["/camera", "/remote", "/gallery"].includes(pathname)) pathname += ".html";

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
    const headers = { "Content-Type": TYPES[path.extname(file)] || "application/octet-stream" };
    if (url.searchParams.has("download")) {
      headers["Content-Disposition"] = `attachment; filename="${path.basename(file)}"`;
    }
    res.writeHead(200, headers);
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
  console.log(`Photos are saved to ${PHOTOS}\n`);
});
