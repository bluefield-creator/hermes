import { CHUNK, chunkCount, sealChunk, openChunk } from "./crypto.js";

const ICE_SERVERS = [
  { urls: ["stun:stun.l.google.com:19302", "stun:stun1.l.google.com:19302"] },
  { urls: "stun:stun.cloudflare.com:3478" },
];
const P2P_TIMEOUT_MS = 12_000;
const HIGH_WATER = 4 * 1024 * 1024;
const LOW_WATER = 1 * 1024 * 1024;
const WINDOW = 256;
const ACK_EVERY = 16;
const SLAB = 16;

export function wsUrl(path) {
  return (location.protocol === "https:" ? "wss://" : "ws://") + location.host + path;
}

export function forceRelay() {
  return new URLSearchParams(location.search).has("relay");
}

export class Signal {
  constructor(path) {
    this.handlers = [];
    this.closeHandlers = [];
    this.ws = new WebSocket(wsUrl(path));
    this.ws.onmessage = (e) => {
      let m;
      try { m = JSON.parse(e.data); } catch { return; }
      for (const h of this.handlers) h(m);
    };
    this.ws.onclose = () => { for (const h of this.closeHandlers) h(); };
  }
  on(h) { this.handlers.push(h); }
  onClose(h) { this.closeHandlers.push(h); }
  send(obj) { if (this.ws.readyState === 1) this.ws.send(JSON.stringify(obj)); }
  close() { this.ws.close(); }
}

function dcChannel(dc, pc) {
  dc.binaryType = "arraybuffer";
  dc.bufferedAmountLowThreshold = LOW_WATER;
  return wrap({
    kind: "p2p",
    raw: dc,
    send: (d) => dc.send(d),
    drain: () => dc.bufferedAmount <= HIGH_WATER ? null : new Promise((r) => {
      dc.addEventListener("bufferedamountlow", r, { once: true });
      dc.addEventListener("close", r, { once: true });
    }),
    close: () => { dc.close(); pc.close(); },
    buffered: () => dc.bufferedAmount,
  });
}

function wsChannel(ws) {
  ws.binaryType = "arraybuffer";
  return wrap({
    kind: "relay",
    raw: ws,
    send: (d) => ws.send(d),
    drain: () => ws.bufferedAmount <= HIGH_WATER ? null : (async () => {
      while (ws.bufferedAmount > LOW_WATER && ws.readyState === 1) await sleep(5);
    })(),
    close: () => ws.close(),
    buffered: () => ws.bufferedAmount,
  });
}

function wrap(ch) {
  const msg = [], cls = [];
  let closed = false;
  ch.raw.addEventListener("message", (e) => { for (const h of msg) h(e.data); });
  ch.raw.addEventListener("close", () => { if (!closed) { closed = true; for (const h of cls) h(); } });
  ch.onMessage = (h) => msg.push(h);
  ch.onClose = (h) => cls.push(h);
  ch.isOpen = () => ch.raw.readyState === 1 || ch.raw.readyState === "open";
  ch.sendJSON = (o) => { if (ch.isOpen()) ch.send(JSON.stringify(o)); };
  return ch;
}

function openRelay(room, peer, role) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(wsUrl(`/relay/${room}/${peer}/${role}`));
    ws.onopen = () => resolve(wsChannel(ws));
    ws.onerror = () => reject(new Error("Relay connection failed"));
  });
}

export class Link {
  constructor({ role, signal, room, peer }) {
    Object.assign(this, { role, signal, room, peer });
    this.pc = null;
    this.relaying = false;
    this.done = false;
    this.pendingIce = [];
    this.channel = new Promise((res, rej) => { this.resolve = res; this.reject = rej; });
    this.channel.then(() => { this.done = true; clearTimeout(this.timer); }, () => {});
  }

  out(m) {
    this.signal.send(this.role === "s" ? { to: this.peer, ...m } : m);
  }

  start() {
    if (forceRelay() || typeof RTCPeerConnection === "undefined") return this.goRelay();
    this.timer = setTimeout(() => this.goRelay(), P2P_TIMEOUT_MS + (this.role === "r" ? 4000 : 0));
    if (this.role === "s") this.offer().catch(() => this.goRelay());
  }

  makePc() {
    const pc = (this.pc = new RTCPeerConnection({ iceServers: ICE_SERVERS }));
    pc.onicecandidate = (e) => { if (e.candidate) this.out({ type: "ice", c: e.candidate.toJSON() }); };
    pc.onconnectionstatechange = () => {
      if (pc.connectionState === "failed" && !this.done) this.goRelay();
    };
    return pc;
  }

  async offer() {
    const pc = this.makePc();
    const dc = pc.createDataChannel("hermes", { ordered: true });
    dc.onopen = () => { if (!this.relaying) this.resolve(dcChannel(dc, pc)); };
    await pc.setLocalDescription(await pc.createOffer());
    this.out({ type: "offer", sdp: pc.localDescription.sdp });
  }

  async handle(m) {
    if (this.relaying) return;
    try {
      if (m.type === "offer" && this.role === "r") {
        const pc = this.makePc();
        pc.ondatachannel = (e) => {
          const dc = e.channel;
          const ready = () => { if (!this.relaying) this.resolve(dcChannel(dc, pc)); };
          dc.readyState === "open" ? ready() : (dc.onopen = ready);
        };
        await pc.setRemoteDescription({ type: "offer", sdp: m.sdp });
        await this.flushIce();
        await pc.setLocalDescription(await pc.createAnswer());
        this.out({ type: "answer", sdp: pc.localDescription.sdp });
      } else if (m.type === "answer" && this.role === "s" && this.pc) {
        await this.pc.setRemoteDescription({ type: "answer", sdp: m.sdp });
        await this.flushIce();
      } else if (m.type === "ice") {
        if (this.pc && this.pc.remoteDescription) await this.pc.addIceCandidate(m.c).catch(() => {});
        else this.pendingIce.push(m.c);
      } else if (m.type === "relay") {
        this.goRelay(true);
      }
    } catch {
      this.goRelay();
    }
  }

  async flushIce() {
    const list = this.pendingIce.splice(0);
    for (const c of list) await this.pc.addIceCandidate(c).catch(() => {});
  }

  async goRelay(remote = false) {
    if (this.relaying || this.done) return;
    this.relaying = true;
    clearTimeout(this.timer);
    if (!remote) this.out({ type: "relay" });
    try { this.pc?.close(); } catch {}
    try {
      this.resolve(await openRelay(this.room, this.peer, this.role));
    } catch (e) {
      this.reject(e);
    }
  }
}

export async function sendFile(ch, file, key, onAck) {
  const count = chunkCount(file.size);
  let acked = 0;
  let wake = null;
  let failure = null;
  let finished;
  const done = new Promise((r) => (finished = r));

  ch.onMessage((d) => {
    if (typeof d !== "string") return;
    let m; try { m = JSON.parse(d); } catch { return; }
    if (m.t === "ack") {
      acked = Math.max(acked, m.n);
      onAck(Math.min(file.size, acked * CHUNK));
    } else if (m.t === "done") {
      onAck(file.size);
      finished();
    } else if (m.t === "error") {
      failure = new Error(m.msg || "Receiver error");
    }
    if (wake) { wake(); wake = null; }
  });
  ch.onClose(() => {
    failure ??= new Error("Connection lost");
    if (wake) { wake(); wake = null; }
    finished();
  });

  const slabBytes = CHUNK * SLAB;
  const readSlab = (s) => file.slice(s * slabBytes, (s + 1) * slabBytes).arrayBuffer();
  let slabIdx = 0, slab = await readSlab(0), nextSlab = count > SLAB ? readSlab(1) : null;

  for (let i = 0; i < count; i++) {
    if (failure) throw failure;
    const s = Math.floor(i / SLAB);
    if (s !== slabIdx) {
      slab = await nextSlab;
      slabIdx = s;
      nextSlab = (s + 1) * SLAB < count ? readSlab(s + 1) : null;
    }
    const off = (i % SLAB) * CHUNK;
    const ct = await sealChunk(key, i, new Uint8Array(slab, off, Math.min(CHUNK, slab.byteLength - off)));
    while (i - acked >= WINDOW && !failure) await new Promise((r) => (wake = r));
    if (failure) throw failure;
    const wait = ch.drain();
    if (wait) await wait;
    if (!ch.isOpen()) throw failure ?? new Error("Connection lost");
    ch.send(ct);
  }
  await done;
  if (failure) throw failure;
}

export function receiveFile(ch, meta, key, sink, onProgress) {
  const count = chunkCount(meta.size);
  return new Promise((resolve, reject) => {
    let index = 0;
    let written = 0;
    let queue = Promise.resolve();
    let failed = false;
    const fail = (e) => {
      if (failed) return;
      failed = true;
      ch.sendJSON({ t: "error", msg: String(e.message || e) });
      sink.abort();
      reject(e);
    };
    ch.onMessage((d) => {
      if (typeof d === "string") {
        try {
          const m = JSON.parse(d);
          if (m.t === "error") fail(new Error(m.msg || "Sender error"));
        } catch {}
        return;
      }
      const i = index++;
      if (i >= count) return fail(new Error("Received more data than expected"));
      queue = queue.then(async () => {
        if (failed) return;
        let pt;
        try {
          pt = await openChunk(key, i, d);
        } catch {
          throw new Error("Decryption failed: data was corrupted or tampered with");
        }
        written += pt.byteLength;
        await sink.write(pt);
        onProgress(written);
        if ((i + 1) % ACK_EVERY === 0 || i === count - 1) ch.sendJSON({ t: "ack", n: i + 1 });
        if (i === count - 1) {
          if (written !== meta.size) throw new Error("Size mismatch");
          await sink.close();
          ch.sendJSON({ t: "done" });
          resolve();
        }
      }).catch(fail);
    });
    ch.onClose(() => { if (index < count || written < meta.size) fail(new Error("Connection lost")); });
    ch.sendJSON({ t: "start" });
  });
}

export function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
