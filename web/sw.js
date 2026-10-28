self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (e) => e.waitUntil(self.clients.claim()));

const pending = new Map();

self.addEventListener("message", (e) => {
  const d = e.data || {};
  if (d.type === "register" && e.ports[0]) {
    pending.set(d.id, { ...d, port: e.ports[0] });
    e.ports[0].postMessage({ type: "registered" });
  }
});

self.addEventListener("fetch", (e) => {
  const url = new URL(e.request.url);
  if (!url.pathname.startsWith("/_dl/")) return;
  const id = url.pathname.slice(5);
  const p = pending.get(id);
  if (!p) return;
  pending.delete(id);

  const port = p.port;
  let waiting = null;
  let ctrl;
  port.onmessage = (m) => {
    const d = m.data;
    if (d.type === "chunk") {
      ctrl.enqueue(new Uint8Array(d.chunk));
      if (waiting) { waiting(); waiting = null; }
    } else if (d.type === "end") {
      ctrl.close();
      if (waiting) { waiting(); waiting = null; }
    } else if (d.type === "abort") {
      ctrl.error(new Error("aborted"));
    }
  };
  const stream = new ReadableStream(
    {
      start(c) { ctrl = c; },
      pull() {
        return new Promise((resolve) => {
          waiting = resolve;
          port.postMessage({ type: "pull" });
        });
      },
      cancel() { port.postMessage({ type: "cancel" }); },
    },
    new CountQueuingStrategy({ highWaterMark: 16 }),
  );

  const name = encodeURIComponent(p.name || "download").replace(/['()*]/g, (c) => "%" + c.charCodeAt(0).toString(16));
  const headers = {
    "Content-Type": "application/octet-stream",
    "Content-Disposition": `attachment; filename*=UTF-8''${name}`,
    "X-Content-Type-Options": "nosniff",
  };
  if (Number.isFinite(p.size)) headers["Content-Length"] = String(p.size);
  e.respondWith(new Response(stream, { headers }));
});
