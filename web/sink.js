const isIOS = /iP(hone|ad|od)/.test(navigator.userAgent) ||
  (navigator.platform === "MacIntel" && navigator.maxTouchPoints > 1);

let swReady = null;

export function prepareServiceWorker() {
  if (!("serviceWorker" in navigator) || isIOS) return null;
  swReady ??= navigator.serviceWorker
    .register("/sw.js", { scope: "/" })
    .then(() => navigator.serviceWorker.ready)
    .catch(() => null);
  return swReady;
}

export function forcedSink() {
  return new URLSearchParams(location.search).get("sink");
}

export async function openSink(meta) {
  const forced = forcedSink();
  if ((!forced || forced === "fs") && "showSaveFilePicker" in window) {
    try {
      const handle = await window.showSaveFilePicker({ suggestedName: meta.name });
      return await fsSink(handle);
    } catch (e) {
      if (e.name === "AbortError") throw e;
    }
  }
  if (!forced || forced === "sw") {
    try {
      const s = await swSink(meta);
      if (s) return s;
    } catch {}
  }
  return blobSink(meta);
}

async function fsSink(handle) {
  const w = await handle.createWritable();
  return {
    kind: "fs",
    write: (buf) => w.write(new Uint8Array(buf)),
    close: () => w.close(),
    abort: () => w.abort().catch(() => {}),
  };
}

async function swSink(meta) {
  const reg = await prepareServiceWorker();
  if (!reg || !reg.active) return null;
  const id = Array.from(crypto.getRandomValues(new Uint8Array(12)), (b) => b.toString(16).padStart(2, "0")).join("");
  const { port1, port2 } = new MessageChannel();

  let credits = 0;
  let wake = null;
  let cancelled = false;
  let firstPull;
  const gotPull = new Promise((r) => (firstPull = r));
  port1.onmessage = (m) => {
    const t = m.data.type;
    if (t === "pull") {
      credits++;
      firstPull();
      if (wake) { wake(); wake = null; }
    } else if (t === "cancel") {
      cancelled = true;
      if (wake) { wake(); wake = null; }
    }
  };
  const registered = new Promise((r) => {
    const prev = port1.onmessage;
    port1.onmessage = (m) => { if (m.data.type === "registered") { port1.onmessage = prev; r(); } else prev(m); };
  });
  reg.active.postMessage({ type: "register", id, name: meta.name, size: meta.size }, [port2]);
  await withTimeout(registered, 3000);

  const iframe = document.createElement("iframe");
  iframe.hidden = true;
  iframe.src = "/_dl/" + id;
  document.body.appendChild(iframe);
  try {
    await withTimeout(gotPull, 8000);
  } catch (e) {
    iframe.remove();
    throw e;
  }

  const keepAlive = setInterval(() => reg.active?.postMessage({ type: "ping" }), 10_000);
  const done = () => { clearInterval(keepAlive); setTimeout(() => iframe.remove(), 60_000); };

  return {
    kind: "sw",
    async write(buf) {
      while (credits === 0 && !cancelled) await new Promise((r) => (wake = r));
      if (cancelled) throw new Error("Download was cancelled in the browser");
      credits--;
      port1.postMessage({ type: "chunk", chunk: buf }, [buf]);
    },
    async close() { port1.postMessage({ type: "end" }); done(); },
    async abort() { port1.postMessage({ type: "abort" }); done(); },
  };
}

function blobSink(meta) {
  const parts = [];
  return {
    kind: "memory",
    async write(buf) { parts.push(buf); },
    async close() {
      const url = URL.createObjectURL(new Blob(parts, { type: meta.type || "application/octet-stream" }));
      parts.length = 0;
      const a = document.createElement("a");
      a.href = url;
      a.download = meta.name;
      document.body.appendChild(a);
      a.click();
      a.remove();
      setTimeout(() => URL.revokeObjectURL(url), 120_000);
    },
    async abort() { parts.length = 0; },
  };
}

function withTimeout(p, ms) {
  return Promise.race([p, new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), ms))]);
}
