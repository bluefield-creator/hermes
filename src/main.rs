use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use axum::{
    body::Bytes,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

const ROOM_ID_LEN: usize = 10;
const PEER_ID_LEN: usize = 16;
const MAX_ROOMS: usize = 10_000;
const MAX_PEERS_PER_ROOM: usize = 16;
const MAX_RELAYS: usize = 2_000;
const SIGNAL_MAX: usize = 64 * 1024;
const SIGNAL_QUEUE: usize = 256;
const RELAY_MAX: usize = 256 * 1024;
const RELAY_PAIR_TIMEOUT: Duration = Duration::from_secs(30);
const PING_EVERY: Duration = Duration::from_secs(20);

type Tx = mpsc::Sender<Message>;

struct Room {
    sender: Tx,
    peers: HashMap<String, Tx>,
}

struct PendingRelay {
    role: Role,
    nonce: u64,
    tx: oneshot::Sender<WebSocket>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Role {
    Sender,
    Receiver,
}

#[derive(Default)]
struct Hub {
    rooms: Mutex<HashMap<String, Room>>,
    relays: Mutex<HashMap<String, PendingRelay>>,
    active_relays: AtomicUsize,
}

type AppState = Arc<Hub>;

#[tokio::main]
async fn main() {
    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8080);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));

    let app = Router::new()
        .route("/", get(page))
        .route("/about", get(page))
        .route("/donate", get(page))
        .route("/healthz", get(|| async { "ok" }))
        .route("/sw.js", get(|| async { asset("sw.js") }))
        .route("/s/{file}", get(|Path(f): Path<String>| async move { asset(&f) }))
        .route("/ws/send", get(ws_send))
        .route("/ws/recv/{room}", get(ws_recv))
        .route("/relay/{room}/{peer}/{role}", get(ws_relay))
        .route("/{room}", get(room_page))
        .fallback(|| async { not_found() })
        .with_state(Arc::new(Hub::default()));

    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind");
    eprintln!("hermes listening on http://{addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .expect("server");
}

macro_rules! web {
    ($f:literal) => {
        include_bytes!(concat!("../web/", $f)).as_slice()
    };
}

fn asset(name: &str) -> Response {
    let (body, ctype): (&'static [u8], &str) = match name {
        "app.js" => (web!("app.js"), "text/javascript; charset=utf-8"),
        "crypto.js" => (web!("crypto.js"), "text/javascript; charset=utf-8"),
        "sink.js" => (web!("sink.js"), "text/javascript; charset=utf-8"),
        "transfer.js" => (web!("transfer.js"), "text/javascript; charset=utf-8"),
        "style.css" => (web!("style.css"), "text/css; charset=utf-8"),
        "logo.svg" => (web!("logo.svg"), "image/svg+xml"),
        "sw.js" => (web!("sw.js"), "text/javascript; charset=utf-8"),
        _ => return not_found(),
    };
    let mut res = (StatusCode::OK, body).into_response();
    let h = res.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_str(ctype).unwrap());
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    h.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    res
}

async fn page(headers: HeaderMap) -> Response {
    let mut res = (StatusCode::OK, web!("index.html")).into_response();
    let h = res.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));
    h.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    h.insert(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    h.insert(header::REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .filter(|h| h.bytes().all(|b| b.is_ascii_alphanumeric() || b"-.:[]".contains(&b)))
        .unwrap_or("");
    let connect = if host.is_empty() {
        String::new()
    } else {
        format!(" wss://{host} ws://{host}")
    };
    let csp = format!(
        "default-src 'self'; script-src 'self'; style-src 'self'; img-src 'self' data:; \
         connect-src 'self'{connect}; frame-src 'self'; worker-src 'self'; \
         frame-ancestors 'none'; base-uri 'none'; form-action 'none'"
    );
    if let Ok(v) = HeaderValue::from_str(&csp) {
        h.insert(header::CONTENT_SECURITY_POLICY, v);
    }
    res
}

async fn room_page(Path(room): Path<String>, headers: HeaderMap) -> Response {
    if valid_id(&room, ROOM_ID_LEN) {
        page(headers).await
    } else {
        not_found()
    }
}

fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "not found").into_response()
}

const ALPHABET: &[u8] = b"abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789";

fn rand_id(len: usize) -> String {
    let mut buf = vec![0u8; len];
    let mut out = String::with_capacity(len);
    let mut i = 0;
    while out.len() < len {
        if i == 0 {
            getrandom::fill(&mut buf).expect("getrandom");
        }
        let b = buf[i] as usize;
        i = (i + 1) % len;
        if b < 256 - 256 % ALPHABET.len() {
            out.push(ALPHABET[b % ALPHABET.len()] as char);
        }
    }
    out
}

fn valid_id(s: &str, len: usize) -> bool {
    s.len() == len && s.bytes().all(|b| ALPHABET.contains(&b))
}

fn text(v: Value) -> Message {
    Message::Text(v.to_string().into())
}

async fn ws_send(ws: WebSocketUpgrade, State(hub): State<AppState>) -> Response {
    ws.max_message_size(SIGNAL_MAX)
        .max_frame_size(SIGNAL_MAX)
        .on_upgrade(move |s| sender_session(hub, s))
}

async fn ws_recv(
    ws: WebSocketUpgrade,
    Path(room): Path<String>,
    State(hub): State<AppState>,
) -> Response {
    ws.max_message_size(SIGNAL_MAX)
        .max_frame_size(SIGNAL_MAX)
        .on_upgrade(move |s| receiver_session(hub, room, s))
}

async fn write_loop(mut sink: SplitSink<WebSocket, Message>, mut rx: mpsc::Receiver<Message>) {
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.tick().await;
    loop {
        tokio::select! {
            m = rx.recv() => match m {
                Some(m) => if sink.send(m).await.is_err() { return },
                None => { let _ = sink.send(Message::Close(None)).await; return }
            },
            _ = ping.tick() => if sink.send(Message::Ping(Bytes::new())).await.is_err() { return },
        }
    }
}

async fn sender_session(hub: AppState, socket: WebSocket) {
    let (sink, mut stream) = socket.split();
    let (tx, rx) = mpsc::channel(SIGNAL_QUEUE);
    let mut writer = tokio::spawn(write_loop(sink, rx));

    let room = {
        let mut rooms = hub.rooms.lock().unwrap();
        if rooms.len() >= MAX_ROOMS {
            None
        } else {
            let id = loop {
                let id = rand_id(ROOM_ID_LEN);
                if !rooms.contains_key(&id) {
                    break id;
                }
            };
            rooms.insert(id.clone(), Room { sender: tx.clone(), peers: HashMap::new() });
            Some(id)
        }
    };
    let Some(room) = room else {
        let _ = tx.try_send(text(json!({"type": "error", "error": "busy"})));
        drop(tx);
        let _ = writer.await;
        return;
    };
    let _ = tx.try_send(text(json!({"type": "created", "room": room})));

    let reader = async {
        while let Some(Ok(msg)) = stream.next().await {
            let Message::Text(t) = msg else {
                if matches!(msg, Message::Close(_)) {
                    break;
                }
                continue;
            };
            let Ok(Value::Object(mut obj)) = serde_json::from_str::<Value>(&t) else { continue };
            let Some(Value::String(to)) = obj.remove("to") else { continue };
            let target = hub.rooms.lock().unwrap().get(&room).and_then(|r| r.peers.get(&to).cloned());
            if let Some(target) = target {
                let _ = target.try_send(text(Value::Object(obj)));
            }
        }
    };
    let writer_done = tokio::select! { _ = reader => false, _ = &mut writer => true };

    let removed = hub.rooms.lock().unwrap().remove(&room);
    if let Some(r) = removed {
        for peer in r.peers.values() {
            let _ = peer.try_send(text(json!({"type": "sender-left"})));
        }
    }
    drop(tx);
    if !writer_done {
        let _ = tokio::time::timeout(Duration::from_secs(5), writer).await;
    }
}

async fn receiver_session(hub: AppState, room: String, socket: WebSocket) {
    let (sink, mut stream) = socket.split();
    let (tx, rx) = mpsc::channel(SIGNAL_QUEUE);
    let mut writer = tokio::spawn(write_loop(sink, rx));

    let joined = {
        let mut rooms = hub.rooms.lock().unwrap();
        match rooms.get_mut(&room) {
            None => Err("not_found"),
            Some(r) if r.peers.len() >= MAX_PEERS_PER_ROOM => Err("full"),
            Some(r) => {
                let peer = rand_id(PEER_ID_LEN);
                r.peers.insert(peer.clone(), tx.clone());
                Ok((peer, r.sender.clone()))
            }
        }
    };
    let (peer, sender) = match joined {
        Ok(j) => j,
        Err(e) => {
            let _ = tx.try_send(text(json!({"type": "error", "error": e})));
            drop(tx);
            let _ = tokio::time::timeout(Duration::from_secs(5), writer).await;
            return;
        }
    };
    let _ = tx.try_send(text(json!({"type": "joined", "peer": peer})));
    let _ = sender.try_send(text(json!({"type": "peer-joined", "peer": peer})));
    drop(tx);

    let reader = async {
        while let Some(Ok(msg)) = stream.next().await {
            let Message::Text(t) = msg else {
                if matches!(msg, Message::Close(_)) {
                    break;
                }
                continue;
            };
            let Ok(Value::Object(mut obj)) = serde_json::from_str::<Value>(&t) else { continue };
            obj.insert("from".into(), Value::String(peer.clone()));
            let _ = sender.try_send(text(Value::Object(obj)));
        }
    };
    let writer_done = tokio::select! { _ = reader => false, _ = &mut writer => true };

    let removed = hub
        .rooms
        .lock()
        .unwrap()
        .get_mut(&room)
        .and_then(|r| r.peers.remove(&peer));
    if removed.is_some() {
        let _ = sender.try_send(text(json!({"type": "peer-left", "peer": peer})));
    }
    drop(removed);
    if !writer_done {
        let _ = tokio::time::timeout(Duration::from_secs(5), writer).await;
    }
}

async fn ws_relay(
    ws: WebSocketUpgrade,
    Path((room, peer, role)): Path<(String, String, String)>,
    State(hub): State<AppState>,
) -> Response {
    let role = match role.as_str() {
        "s" => Role::Sender,
        "r" => Role::Receiver,
        _ => return not_found(),
    };
    let known = hub
        .rooms
        .lock()
        .unwrap()
        .get(&room)
        .is_some_and(|r| r.peers.contains_key(&peer));
    if !known {
        return (StatusCode::FORBIDDEN, "unknown peer").into_response();
    }
    if hub.active_relays.load(Ordering::Relaxed) >= MAX_RELAYS {
        return (StatusCode::SERVICE_UNAVAILABLE, "relay busy").into_response();
    }
    ws.max_message_size(RELAY_MAX)
        .max_frame_size(RELAY_MAX)
        .write_buffer_size(0)
        .max_write_buffer_size(2 * RELAY_MAX)
        .on_upgrade(move |s| relay_session(hub, format!("{room}/{peer}"), role, s))
}

async fn relay_session(hub: AppState, key: String, role: Role, socket: WebSocket) {
    let nonce = u64::from_ne_bytes(rand_id(8).as_bytes().try_into().unwrap());
    let rx = {
        let mut relays = hub.relays.lock().unwrap();
        match relays.remove(&key) {
            Some(other) if other.role != role => {
                let _ = other.tx.send(socket);
                return;
            }
            Some(other) => {
                relays.insert(key, other);
                return;
            }
            None => {
                let (tx, rx) = oneshot::channel();
                relays.insert(key.clone(), PendingRelay { role, nonce, tx });
                rx
            }
        }
    };
    let other = match tokio::time::timeout(RELAY_PAIR_TIMEOUT, rx).await {
        Ok(Ok(ws)) => ws,
        _ => {
            let mut relays = hub.relays.lock().unwrap();
            if relays.get(&key).is_some_and(|p| p.nonce == nonce) {
                relays.remove(&key);
            }
            return;
        }
    };
    hub.active_relays.fetch_add(1, Ordering::Relaxed);
    pipe(socket, other).await;
    hub.active_relays.fetch_sub(1, Ordering::Relaxed);
}

async fn pipe(a: WebSocket, b: WebSocket) {
    let (mut a_tx, mut a_rx) = a.split();
    let (mut b_tx, mut b_rx) = b.split();
    tokio::select! {
        _ = forward(&mut a_rx, &mut b_tx) => {},
        _ = forward(&mut b_rx, &mut a_tx) => {},
    }
    let close = async {
        let _ = a_tx.send(Message::Close(None)).await;
        let _ = b_tx.send(Message::Close(None)).await;
    };
    let _ = tokio::time::timeout(Duration::from_secs(2), close).await;
}

async fn forward(rx: &mut SplitStream<WebSocket>, tx: &mut SplitSink<WebSocket, Message>) {
    let mut ping = tokio::time::interval(PING_EVERY);
    ping.tick().await;
    loop {
        tokio::select! {
            m = rx.next() => match m {
                Some(Ok(m @ (Message::Binary(_) | Message::Text(_)))) => {
                    if tx.send(m).await.is_err() { return }
                    ping.reset();
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => return,
                Some(Ok(_)) => {}
            },
            _ = ping.tick() => if tx.send(Message::Ping(Bytes::new())).await.is_err() { return },
        }
    }
}
