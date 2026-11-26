// Tactical Strike relay + web host. No dependencies: needs only Node 18+.
//
//  - Serves the Godot web export from ./public  (http://host:PORT/)
//  - WebSocket relay on /ws with 4-letter rooms (one host + one guest each).
//    {"t":"create"}            -> {"t":"created","code":"ABCD"}
//    {"t":"join","code":"ABCD"} -> guest gets {"t":"joined"}, host gets {"t":"peer_joined"}
//    any other message is forwarded unchanged to the other player in the room.
//    When someone leaves, the other gets {"t":"peer_left"}.

const http = require("http");
const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

const PORT = process.env.PORT || 8080;
const PUBLIC = path.join(__dirname, "public");
const MIME = {
  ".html": "text/html; charset=utf-8", ".js": "application/javascript", ".wasm": "application/wasm",
  ".pck": "application/octet-stream", ".png": "image/png", ".ico": "image/x-icon",
  ".svg": "image/svg+xml", ".json": "application/json", ".css": "text/css",
};

// ------------------------------------------------------------------ static files
const server = http.createServer((req, res) => {
  let url = "/";
  try { url = decodeURIComponent(req.url.split("?")[0]); } catch (e) { /* keep "/" */ }
  if (url === "/") url = "/index.html";
  const file = path.normalize(path.join(PUBLIC, url));
  if (!file.startsWith(PUBLIC)) { res.writeHead(403); res.end(); return; }
  fs.stat(file, (err, st) => {
    if (err || !st.isFile()) {
      res.writeHead(404, { "Content-Type": "text/plain" });
      res.end(fs.existsSync(path.join(PUBLIC, "index.html"))
        ? "Not found"
        : "Tactical Strike server is running. Export the Godot web build into server/public/ to play in the browser.");
      return;
    }
    res.writeHead(200, {
      "Content-Type": MIME[path.extname(file).toLowerCase()] || "application/octet-stream",
      // needed if the web export is built with thread support; harmless otherwise
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp",
      "Cache-Control": "no-cache",
    });
    fs.createReadStream(file).pipe(res);
  });
});

// ------------------------------------------------------------------ minimal WebSocket
class Client {
  constructor(socket) {
    this.socket = socket;
    this.buf = Buffer.alloc(0);
    this.frag = [];
    this.room = null;
    this.alive = true;
    this.closed = false;
    socket.on("data", (d) => this.onData(d));
    socket.on("close", () => this.onClose());
    socket.on("error", () => this.onClose());
  }

  onData(chunk) {
    this.buf = Buffer.concat([this.buf, chunk]);
    for (;;) {
      if (this.buf.length < 2) return;
      const b0 = this.buf[0], b1 = this.buf[1];
      const fin = (b0 & 0x80) !== 0, op = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.buf.length < 10) return; len = Number(this.buf.readBigUInt64BE(2)); off = 10; }
      if (len > 1 << 20) { this.close(); return; }
      let mask = null;
      if (masked) { if (this.buf.length < off + 4) return; mask = this.buf.subarray(off, off + 4); off += 4; }
      if (this.buf.length < off + len) return;
      const payload = Buffer.from(this.buf.subarray(off, off + len));
      if (mask) for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
      this.buf = this.buf.subarray(off + len);

      if (op === 0x8) { this.close(); return; }                 // close
      if (op === 0x9) { this.sendFrame(0xA, payload); continue; } // ping -> pong
      if (op === 0xA) { this.alive = true; continue; }           // pong
      if (op === 0x1 || op === 0x2 || op === 0x0) {
        this.frag.push(payload);
        if (fin) { const text = Buffer.concat(this.frag).toString("utf8"); this.frag = []; onMessage(this, text); }
      }
    }
  }

  sendFrame(op, payload) {
    if (this.closed) return;
    const n = payload.length;
    let head;
    if (n < 126) head = Buffer.from([0x80 | op, n]);
    else if (n < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(n, 2); }
    else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(n), 2); }
    this.socket.write(Buffer.concat([head, payload]));
  }

  send(text) { this.sendFrame(0x1, Buffer.from(text, "utf8")); }
  sendJson(obj) { this.send(JSON.stringify(obj)); }

  close() {
    if (this.closed) return;
    try { this.sendFrame(0x8, Buffer.alloc(0)); } catch (e) { /* ignore */ }
    this.socket.end();
    this.onClose();
  }

  onClose() {
    if (this.closed) return;
    this.closed = true;
    clients.delete(this);
    leaveRoom(this);
  }
}

// ------------------------------------------------------------------ rooms
const rooms = new Map();   // code -> {host, guest}
const clients = new Set();
const LETTERS = "ABCDEFGHJKLMNPQRSTUVWXYZ"; // no I / O

function newCode() {
  for (let tries = 0; tries < 1000; tries++) {
    let c = "";
    for (let i = 0; i < 4; i++) c += LETTERS[crypto.randomInt(LETTERS.length)];
    if (!rooms.has(c)) return c;
  }
  return null;
}

function other(client) {
  const r = client.room && rooms.get(client.room);
  if (!r) return null;
  return r.host === client ? r.guest : r.host;
}

function leaveRoom(client) {
  const code = client.room;
  if (!code) return;
  client.room = null;
  const r = rooms.get(code);
  if (!r) return;
  const peer = r.host === client ? r.guest : r.host;
  if (r.host === client) {
    rooms.delete(code);            // host left: room is gone
    if (peer) { peer.room = null; peer.sendJson({ t: "peer_left" }); }
  } else {
    r.guest = null;                // guest left: host can wait for someone else
    if (peer) peer.sendJson({ t: "peer_left" });
  }
}

function onMessage(client, text) {
  let msg;
  try { msg = JSON.parse(text); } catch (e) { return; }
  if (!msg || typeof msg !== "object") return;

  if (msg.t === "create") {
    leaveRoom(client);
    const code = newCode();
    if (!code) { client.sendJson({ t: "error", msg: "Server is full, try again later." }); return; }
    rooms.set(code, { host: client, guest: null });
    client.room = code;
    client.sendJson({ t: "created", code });
    return;
  }
  if (msg.t === "join") {
    const code = String(msg.code || "").toUpperCase();
    const r = rooms.get(code);
    if (!r) { client.sendJson({ t: "error", msg: `No room called ${code}. Check the code.` }); return; }
    if (r.guest) { client.sendJson({ t: "error", msg: `Room ${code} is already full.` }); return; }
    leaveRoom(client);
    r.guest = client;
    client.room = code;
    client.sendJson({ t: "joined", code });
    r.host.sendJson({ t: "peer_joined" });
    return;
  }
  const peer = other(client);
  if (peer) peer.send(text);       // relay everything else untouched
}

server.on("upgrade", (req, socket) => {
  if (req.url.split("?")[0] !== "/ws" || (req.headers.upgrade || "").toLowerCase() !== "websocket") {
    socket.destroy();
    return;
  }
  const key = req.headers["sec-websocket-key"];
  if (!key) { socket.destroy(); return; }
  const accept = crypto.createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
    `Sec-WebSocket-Accept: ${accept}\r\n\r\n`);
  socket.setNoDelay(true);
  clients.add(new Client(socket));
});

// keep connections alive through proxies, and drop dead ones
setInterval(() => {
  for (const c of clients) {
    if (!c.alive) { c.close(); continue; }
    c.alive = false;
    c.sendFrame(0x9, Buffer.alloc(0));
  }
}, 25000);

server.listen(PORT, () => {
  console.log(`Tactical Strike server on http://localhost:${PORT}  (relay at ws://localhost:${PORT}/ws)`);
});
