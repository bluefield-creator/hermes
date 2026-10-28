const te = new TextEncoder();
const td = new TextDecoder();

export const CHUNK = 65536 - 16;

export function b64u(bytes) {
  let s = "";
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

export function unb64u(str) {
  const s = atob(str.replace(/-/g, "+").replace(/_/g, "/"));
  const out = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i);
  return out;
}

export function randomBytes(n) {
  return crypto.getRandomValues(new Uint8Array(n));
}

export function importMaster(raw) {
  return crypto.subtle.importKey("raw", raw, "HKDF", false, ["deriveKey"]);
}

function derive(master, salt, info) {
  return crypto.subtle.deriveKey(
    { name: "HKDF", hash: "SHA-256", salt, info: te.encode(info) },
    master,
    { name: "AES-GCM", length: 256 },
    false,
    ["encrypt", "decrypt"],
  );
}

function iv(index) {
  const v = new Uint8Array(12);
  const d = new DataView(v.buffer);
  d.setUint32(4, Math.floor(index / 2 ** 32));
  d.setUint32(8, index >>> 0);
  return v;
}

export async function sessionKeys(master, salt) {
  const [meta, data] = await Promise.all([
    derive(master, salt, "hermes/v1/meta"),
    derive(master, salt, "hermes/v1/data"),
  ]);
  return { meta, data };
}

export async function sealMeta(key, obj) {
  const ct = await crypto.subtle.encrypt({ name: "AES-GCM", iv: iv(0) }, key, te.encode(JSON.stringify(obj)));
  return new Uint8Array(ct);
}

export async function openMeta(key, ct) {
  const pt = await crypto.subtle.decrypt({ name: "AES-GCM", iv: iv(0) }, key, ct);
  return JSON.parse(td.decode(pt));
}

export function sealChunk(key, index, plain) {
  return crypto.subtle.encrypt({ name: "AES-GCM", iv: iv(index) }, key, plain);
}

export function openChunk(key, index, ct) {
  return crypto.subtle.decrypt({ name: "AES-GCM", iv: iv(index) }, key, ct);
}

export function chunkCount(size) {
  return Math.max(1, Math.ceil(size / CHUNK));
}
