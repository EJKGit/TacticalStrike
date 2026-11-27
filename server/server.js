// Tactical Strike relay + web host. No dependencies: needs only Node 18+.
//
//  - Serves the Godot web export from ./public  (http://host:PORT/)
//  - WebSocket relay on /ws with 4-letter rooms of up to 4 players.
//    {"t":"create"}            -> {"t":"created","code":"ABCD"}
//    {"t":"join","code":"ABCD"} -> guest gets {"t":"joined"}, host gets {"t":"peer_joined"}
//    any other message is forwarded to the others in the room (or just to "to": <player id>),
//    tagged with "from": <sender's player id>.
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

// ------------------------------------------------------------------ unpack .zip files
// GitHub's website only accepts uploads up to 25 MB, and the game's index.wasm is bigger.
// Compress it in Finder (right-click > Compress) and upload index.wasm.zip instead:
// at startup every .zip in public/ is unpacked next to itself (no dependencies needed).
const zlib = require("zlib");

function unzipFile(zipPath) {
  const buf = fs.readFileSync(zipPath);
  // find the End Of Central Directory record
  let eocd = -1;
  for (let i = buf.length - 22; i >= Math.max(0, buf.length - 65557); i--) {
    if (buf.readUInt32LE(i) === 0x06054b50) { eocd = i; break; }
  }
  if (eocd < 0) throw new Error("not a zip file");
  const count = buf.readUInt16LE(eocd + 10);
  let p = buf.readUInt32LE(eocd + 16);
  const outDir = path.dirname(zipPath);
  const written = [];
  for (let n = 0; n < count; n++) {
    if (buf.readUInt32LE(p) !== 0x02014b50) throw new Error("bad central directory");
    const method = buf.readUInt16LE(p + 10);
    const compSize = buf.readUInt32LE(p + 20);
    const nameLen = buf.readUInt16LE(p + 28);
    const extraLen = buf.readUInt16LE(p + 30);
    const commentLen = buf.readUInt16LE(p + 32);
    const localOff = buf.readUInt32LE(p + 42);
    const name = buf.toString("utf8", p + 46, p + 46 + nameLen);
    p += 46 + nameLen + extraLen + commentLen;
    // skip folders and macOS metadata (__MACOSX/, ._files, .DS_Store)
    const base = path.basename(name);
    if (name.endsWith("/") || name.startsWith("__MACOSX") || base.startsWith("._") || base === ".DS_Store") continue;
    const lNameLen = buf.readUInt16LE(localOff + 26);
    const lExtraLen = buf.readUInt16LE(localOff + 28);
    const start = localOff + 30 + lNameLen + lExtraLen;
    const data = buf.subarray(start, start + compSize);
    let out;
    if (method === 0) out = data;
    else if (method === 8) out = zlib.inflateRawSync(data);
    else throw new Error(`unsupported compression in ${name}`);
    const dest = path.normalize(path.join(outDir, base));
    if (!dest.startsWith(PUBLIC)) continue;
    fs.writeFileSync(dest, out);
    written.push(`${base} (${(out.length / 1048576).toFixed(1)} MB)`);
  }
  return written;
}

function unpackZips(dir) {
  if (!fs.existsSync(dir)) return;
  for (const f of fs.readdirSync(dir)) {
    const full = path.join(dir, f);
    if (fs.statSync(full).isDirectory()) { unpackZips(full); continue; }
    if (!f.toLowerCase().endsWith(".zip")) continue;
    try { console.log(`Unpacked ${f}: ${unzipFile(full).join(", ")}`); }
    catch (e) { console.log(`Could not unpack ${f}: ${e.message}`); }
  }
}
unpackZips(PUBLIC);

// ------------------------------------------------------------------ static files
const server = http.createServer((req, res) => {
  let url = "/";
  try { url = decodeURIComponent(req.url.split("?")[0]); } catch (e) { /* keep "/" */ }
  if (url === "/") url = "/index.html";
  const file = path.normalize(path.join(PUBLIC, url));
  if (!file.startsWith(PUBLIC)) { res.writeHead(403); res.end(); return; }
  // A gzipped copy (e.g. index.wasm.gz) is served in place of a missing file. This keeps
  // the big .wasm under GitHub's 25 MB browser-upload limit.
  let gzipped = false;
  if (!fs.existsSync(file) && fs.existsSync(file + ".gz")) gzipped = true;
  const real = gzipped ? file + ".gz" : file;
  fs.stat(real, (err, st) => {
    if (err || !st.isFile()) {
      res.writeHead(404, { "Content-Type": "text/plain" });
      res.end(fs.existsSync(path.join(PUBLIC, "index.html"))
        ? "Not found"
        : "Tactical Strike server is running. Export the Godot web build into server/public/ to play in the browser.");
      return;
    }
    const headers = {
      "Content-Type": MIME[path.extname(file).toLowerCase()] || "application/octet-stream",
      // needed if the web export is built with thread support; harmless otherwise
      "Cross-Origin-Opener-Policy": "same-origin",
      "Cross-Origin-Embedder-Policy": "require-corp",
      "Cache-Control": "no-cache",
    };
    if (gzipped) headers["Content-Encoding"] = "gzip";
    res.writeHead(200, headers);
    fs.createReadStream(real).pipe(res);
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
// A room holds up to MAX_PLAYERS. The creator is player 1 (the host); joiners get
// 2, 3, 4. Every relayed message gets "from": <player id>. A message with "to": <id>
// goes only to that player, anything else goes to everyone else in the room.
// The room lives until the last player leaves.
const MAX_PLAYERS = 4;
const rooms = new Map();   // code -> {members: Map(pid -> client), next: int}
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

function leaveRoom(client) {
  const code = client.room;
  if (!code) return;
  client.room = null;
  const r = rooms.get(code);
  if (!r) return;
  r.members.delete(client.pid);
  if (r.members.size === 0) { rooms.delete(code); return; }
  for (const c of r.members.values()) c.sendJson({ t: "peer_left", pid: client.pid, from: client.pid });
}

function onMessage(client, text) {
  let msg;
  try { msg = JSON.parse(text); } catch (e) { return; }
  if (!msg || typeof msg !== "object") return;

  if (msg.t === "create") {
    leaveRoom(client);
    const code = newCode();
    if (!code) { client.sendJson({ t: "error", msg: "Server is full, try again later." }); return; }
    client.pid = 1;
    rooms.set(code, { members: new Map([[1, client]]), next: 2 });
    client.room = code;
    client.sendJson({ t: "created", code, pid: 1, peers: [1] });
    return;
  }
  if (msg.t === "join") {
    const code = String(msg.code || "").toUpperCase();
    const r = rooms.get(code);
    if (!r) { client.sendJson({ t: "error", msg: `No room called ${code}. Check the code.` }); return; }
    if (r.members.size >= MAX_PLAYERS) { client.sendJson({ t: "error", msg: `Room ${code} is full (${MAX_PLAYERS} players).` }); return; }
    leaveRoom(client);
    client.pid = r.next++;
    for (const c of r.members.values()) c.sendJson({ t: "peer_joined", pid: client.pid, from: client.pid });
    r.members.set(client.pid, client);
    client.room = code;
    client.sendJson({ t: "joined", code, pid: client.pid, peers: [...r.members.keys()] });
    return;
  }
  const r = client.room && rooms.get(client.room);
  if (!r) return;
  msg.from = client.pid;
  const out = JSON.stringify(msg);
  if (msg.to !== undefined) {
    const target = r.members.get(Number(msg.to));
    if (target && target !== client) target.send(out);
    return;
  }
  for (const c of r.members.values()) if (c !== client) c.send(out);
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
