import {
  b64u, unb64u, randomBytes, importMaster, sessionKeys, sealMeta, openMeta,
} from "./crypto.js";
import { Signal, Link, sendFile, receiveFile } from "./transfer.js";
import { openSink, prepareServiceWorker } from "./sink.js";

const $ = (id) => document.getElementById(id);

function show(id) {
  for (const s of document.querySelectorAll("main > section")) s.hidden = s.id !== id;
}

function oops(msg) {
  $("oops-msg").textContent = msg;
  show("oops");
}

function fmtBytes(n) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let i = 0;
  while (n >= 1000 && i < units.length - 1) { n /= 1000; i++; }
  return (i === 0 ? n : n.toFixed(n < 10 ? 2 : n < 100 ? 1 : 0)) + " " + units[i];
}

function meter() {
  const samples = [];
  return (bytes) => {
    const now = performance.now();
    samples.push([now, bytes]);
    while (samples.length > 2 && now - samples[0][0] > 3000) samples.shift();
    const [t0, b0] = samples[0];
    return now - t0 > 200 ? ((bytes - b0) / (now - t0)) * 1000 : 0;
  };
}

function progress(root, total) {
  root.hidden = false;
  root.classList.remove("done", "failed");
  const fill = root.querySelector(".fill");
  const pct = root.querySelector(".pct");
  const bytes = root.querySelector(".bytes");
  const note = root.querySelector(".note");
  const rate = meter();
  let last = 0;
  const api = {
    set(n, force) {
      const now = performance.now();
      const speed = rate(n);
      if (!force && now - last < 100) return;
      last = now;
      const p = total ? (n / total) * 100 : 100;
      fill.style.clipPath = `inset(0 ${100 - p}% 0 0 round 999px)`;
      pct.textContent = Math.floor(p) + "%";
      bytes.textContent = `${fmtBytes(n)} / ${fmtBytes(total)}` + (speed ? ` (${fmtBytes(speed)}/s)` : "");
    },
    note(text, cls = "") { note.textContent = text; note.className = "note " + cls; },
    done(text) { api.set(total, true); root.classList.add("done"); bytes.textContent = fmtBytes(total); api.note(text, "ok"); },
    fail(text) { root.classList.add("failed"); api.note(text, "bad"); },
  };
  api.set(0, true);
  return api;
}

function progressTemplate() {
  const el = document.createElement("div");
  el.className = "progress";
  el.innerHTML = '<div class="bar"><div class="fill"></div></div>' +
    '<div class="stats"><span class="pct">0%</span><span class="bytes"></span></div><p class="note"></p>';
  return el;
}

function viaText(ch) {
  return ch.kind === "p2p"
    ? "direct p2p · end-to-end encrypted"
    : "relayed live through the server · still end-to-end encrypted";
}

let wakeLock = null;
async function keepAwake(on) {
  try {
    if (on && !wakeLock && navigator.wakeLock) wakeLock = await navigator.wakeLock.request("screen");
    if (!on && wakeLock) { await wakeLock.release(); wakeLock = null; }
  } catch {}
}

function senderPick() {
  show("pick");
  const drop = $("drop");
  const input = $("file");
  input.addEventListener("change", () => input.files[0] && startSending(input.files[0]));
  for (const ev of ["dragenter", "dragover"]) {
    window.addEventListener(ev, (e) => { e.preventDefault(); drop.classList.add("over"); });
  }
  window.addEventListener("dragleave", (e) => { if (!e.relatedTarget) drop.classList.remove("over"); });
  window.addEventListener("drop", (e) => {
    e.preventDefault();
    drop.classList.remove("over");
    const f = e.dataTransfer?.files?.[0];
    if (f && $("pick").hidden === false) startSending(f);
  });
}

async function startSending(file) {
  const rawKey = randomBytes(32);
  const master = await importMaster(rawKey);
  const metaPlain = { name: file.name, size: file.size, type: file.type };

  show("send");
  $("s-name").textContent = file.name;
  $("s-size").textContent = fmtBytes(file.size);
  $("s-link").value = "connecting…";

  const peers = new Map();
  let active = 0;
  let room = null;
  window.addEventListener("beforeunload", (e) => { e.preventDefault(); e.returnValue = ""; });

  const signal = new Signal("/ws/send");
  signal.onClose(() => {
    $("s-waiting").hidden = false;
    $("s-waiting").textContent = "lost connection to the server. reload the page to share again.";
    $("s-link").value = "";
  });
  signal.on(async (m) => {
    if (m.type === "created") {
      room = m.room;
      const link = `${location.origin}/${room}#${b64u(rawKey)}`;
      $("s-link").value = link;
      return;
    }
    if (m.type === "error") return oops("the server is too busy right now. try again in a bit.");
    if (m.type === "peer-joined") {
      const peer = m.peer;
      const ui = document.createElement("div");
      ui.className = "peer";
      ui.innerHTML = '<div class="who"></div>';
      const bar = progressTemplate();
      ui.appendChild(bar);
      $("peers").appendChild(ui);
      $("s-waiting").hidden = true;
      const label = `receiver ${peers.size + 1}`;
      ui.querySelector(".who").textContent = label;
      const p = progress(bar, file.size);
      p.note("connecting…");

      const link = new Link({ role: "s", signal, room, peer });
      peers.set(peer, { link, ui, p, state: "connecting" });

      const salt = randomBytes(16);
      const keys = await sessionKeys(master, salt);
      const meta = await sealMeta(keys.meta, metaPlain);
      signal.send({ to: peer, type: "meta", salt: b64u(salt), meta: b64u(meta) });
      link.start();

      let ch;
      try {
        ch = await link.channel;
      } catch (e) {
        p.fail("couldn't connect: " + e.message);
        return;
      }
      p.note(`connected (${ch.kind === "p2p" ? "p2p" : "relay"}). waiting for them to hit download…`);
      ch.onMessage(async function onStart(d) {
        if (typeof d !== "string" || peers.get(peer)?.state !== "connecting") return;
        let msg; try { msg = JSON.parse(d); } catch { return; }
        if (msg.t !== "start") return;
        peers.get(peer).state = "sending";
        p.note(viaText(ch));
        active++;
        keepAwake(true);
        try {
          await sendFile(ch, file, keys.data, (n) => p.set(n));
          p.done("delivered ✓");
        } catch (e) {
          p.fail("stopped: " + e.message);
        } finally {
          peers.get(peer) && (peers.get(peer).state = "finished");
          if (--active === 0) keepAwake(false);
          setTimeout(() => ch.close(), 1000);
        }
      });
      ch.onClose(() => {
        if (peers.get(peer)?.state === "connecting") p.fail("they left");
      });
      return;
    }
    if (m.type === "peer-left") {
      const s = peers.get(m.peer);
      if (s && s.state !== "finished") {
        if (s.state === "connecting") s.ui.remove();
        peers.delete(m.peer);
        if (!$("peers").children.length) $("s-waiting").hidden = false;
      }
      return;
    }
    if (m.from) peers.get(m.from)?.link.handle(m);
  });

  $("s-copy").onclick = async () => {
    const v = $("s-link").value;
    try { await navigator.clipboard.writeText(v); } catch { $("s-link").select(); document.execCommand("copy"); }
    $("s-copy").textContent = "copied!";
    setTimeout(() => ($("s-copy").textContent = "copy"), 1500);
  };
  $("s-link").addEventListener("focus", (e) => e.target.select());
}

async function receiver(room, keyB64) {
  show("recv");
  prepareServiceWorker();
  let master;
  try {
    const raw = unb64u(keyB64);
    if (raw.length !== 32) throw 0;
    master = await importMaster(raw);
  } catch {
    return oops("this link is incomplete. make sure you copied all of it, including the part after #.");
  }
  const go = $("r-go");
  const bar = $("r-progress");
  let meta = null, keys = null, link = null, channelUp = false, started = false, finished = false;

  const signal = new Signal(`/ws/recv/${room}`);
  signal.onClose(() => {
    if (!started && !finished) oops("couldn't reach the sender. the link may have expired: it only works while the sender's tab is open.");
  });
  signal.on(async (m) => {
    if (m.type === "error") {
      return oops(m.error === "not_found"
        ? "this link has expired. links only work while the sender's tab is open."
        : "too many people are downloading this right now. try again in a minute.");
    }
    if (m.type === "joined") {
      link = new Link({ role: "r", signal, room, peer: m.peer });
      link.start();
      link.channel.then(
        () => { channelUp = true; if (meta && !started) ready(); },
        (e) => failNow("couldn't connect to the sender: " + e.message),
      );
      return;
    }
    if (m.type === "meta") {
      try {
        keys = await sessionKeys(master, unb64u(m.salt));
        meta = await openMeta(keys.meta, unb64u(m.meta));
      } catch {
        return oops("this link's key doesn't match. make sure you copied the whole link.");
      }
      document.title = `${meta.name} · hermes`;
      $("r-name").textContent = meta.name;
      $("r-size").textContent = fmtBytes(meta.size);
      if (channelUp) ready(); else go.textContent = "connecting…";
      return;
    }
    if (m.type === "sender-left") {
      if (!finished) {
        if (started) failNow("the sender closed their tab.");
        else oops("the sender closed their tab, so this link doesn't work anymore.");
      }
      return;
    }
    link?.handle(m);
  });

  let p = null;
  const failNow = (msg) => { p ? p.fail(msg) : oops(msg); go.hidden = true; };

  function ready() {
    go.disabled = false;
    go.textContent = "download";
  }

  go.onclick = async () => {
    if (started || !meta) return;
    let sink;
    try {
      sink = await openSink(meta);
    } catch (e) {
      if (e.name === "AbortError") return;
      return failNow("couldn't start the download: " + e.message);
    }
    started = true;
    go.hidden = true;
    $("r-title").textContent = "downloading ur file";
    if (sink.kind === "memory") {
      $("r-hint").textContent = "this browser can't stream to disk, so the file is kept in memory until it's done. very large files may not fit.";
    }
    const ch = await link.channel;
    p = progress(bar, meta.size);
    p.note(viaText(ch));
    keepAwake(true);
    try {
      await receiveFile(ch, meta, keys.data, sink, (n) => p.set(n));
      finished = true;
      p.done(sink.kind === "fs" ? "saved ✓" : "done ✓ check your downloads");
      $("r-title").textContent = "got it!";
    } catch (e) {
      p.fail("stopped: " + e.message);
    } finally {
      keepAwake(false);
    }
  };
}

const path = location.pathname;
const roomMatch = path.match(/^\/([A-Za-z0-9]{10})$/);
if (path === "/about" || path === "/donate") {
  show(path.slice(1));
} else if (roomMatch) {
  const key = location.hash.slice(1);
  if (!key) oops("this link is missing its key (the part after #). ask the sender to copy the whole thing.");
  else receiver(roomMatch[1], key);
} else {
  senderPick();
}
