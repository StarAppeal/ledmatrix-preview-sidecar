use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        ConnectInfo, State,
    },
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::{
    net::{IpAddr, SocketAddr},
    sync::Arc,
    time::Duration,
};
use tokio::sync::broadcast::{self, error::RecvError};

use crate::{
    auth::{
        is_object_id, owns_virtual_device, start_device_session, verify_token, AuthError, AuthMsg,
    },
    net::frame_payload,
    state::{is_device_room, AppState, FrameFormat, ViewerRx, DEVICE_ROOM_PREFIX},
    util::{client_ip::client_ip, ip_limits::ConnGuard, rate_limit::ViewerLimiter},
};

// Legit binaries are 64x64 frames; this caps what reaches the preview decoder.
const MAX_MESSAGE_SIZE: usize = 1024 * 1024;
const AUTH_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
) -> Response {
    let ip = client_ip(&headers, peer);
    let Some(guard) = state.ip_limits.try_acquire(ip) else {
        tracing::warn!("Too many connections from {}, refusing", ip);
        return StatusCode::TOO_MANY_REQUESTS.into_response();
    };
    ws.max_message_size(MAX_MESSAGE_SIZE)
        .max_frame_size(MAX_MESSAGE_SIZE)
        .on_upgrade(move |socket| handle_socket(socket, state, ip, guard))
}

struct Viewer {
    // user id for the preview, dev:<device id> for a virtual device
    room_id: String,
    format: FrameFormat,
    jwt: String,
}

fn text(value: serde_json::Value) -> Message {
    Message::Text(value.to_string().into())
}

fn frame_message(format: FrameFormat, frame: axum::body::Bytes) -> Message {
    match format {
        FrameFormat::Raw => Message::Binary(frame),
        FrameFormat::Png => Message::Text(frame_payload(&frame).into()),
    }
}

async fn authenticate(
    socket: &mut WebSocket,
    state: &AppState,
    ip: IpAddr,
) -> Result<Viewer, AuthError> {
    let Ok(Some(Ok(Message::Text(msg)))) = tokio::time::timeout(AUTH_TIMEOUT, socket.next()).await
    else {
        return Err(AuthError::Rejected);
    };
    let auth = serde_json::from_str::<AuthMsg>(&msg)
        .ok()
        .filter(|auth| auth.msg_type == "AUTH")
        .ok_or(AuthError::Rejected)?;

    if !state.ip_limits.allow_auth(ip) {
        tracing::warn!("Too many AUTH attempts from {}", ip);
        return Err(AuthError::RateLimited);
    }

    let user_id = verify_token(&state.http, &auth.token, &state.backend_url)
        .await
        .inspect_err(|err| tracing::warn!("WebSocket auth failed: {:?}", err))?;

    let format = match auth.format.as_deref() {
        Some("raw") => FrameFormat::Raw,
        _ => FrameFormat::Png,
    };

    let room_id = match auth.device {
        None => user_id,
        Some(device_id) => {
            let owned = is_object_id(&device_id)
                && owns_virtual_device(&state.http, &auth.token, &state.backend_url, &device_id)
                    .await?;
            if !owned {
                tracing::warn!("User {} may not watch device {}", user_id, device_id);
                return Err(AuthError::Rejected);
            }
            format!("{}{}", DEVICE_ROOM_PREFIX, device_id)
        }
    };

    Ok(Viewer {
        room_id,
        format,
        jwt: auth.token,
    })
}

// The guard holds this connection's slot in the per-IP limit until the socket closes
async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>, ip: IpAddr, _guard: ConnGuard) {
    let viewer = match authenticate(&mut socket, &state, ip).await {
        Ok(viewer) => viewer,
        Err(AuthError::Rejected) => {
            let _ = socket.send(text(json!({"type": "ERROR"}))).await;
            return;
        }
        // The client treats ERROR as final; START_FAILED makes it reconnect with backoff
        Err(AuthError::Unavailable) => {
            let _ = socket.send(text(json!({"type": "START_FAILED"}))).await;
            return;
        }
        // Unknown to older clients; they reconnect with backoff once the socket closes
        Err(AuthError::RateLimited) => {
            let _ = socket.send(text(json!({"type": "RATE_LIMITED"}))).await;
            return;
        }
    };
    let room_id = viewer.room_id.clone();
    let format = viewer.format;

    let mut resync_rx = state.resync_tx.subscribe();
    let joined = match state.join_room(&room_id, format) {
        Ok(joined) => joined,
        Err(_) => {
            tracing::warn!("Too many device sessions, refusing {}", room_id);
            let _ = socket.send(text(json!({"type": "BUSY"}))).await;
            return;
        }
    };

    if let Some(ticket) = joined.start_ticket {
        let device_id = &room_id[DEVICE_ROOM_PREFIX.len()..];
        let token =
            start_device_session(&state.http, &viewer.jwt, &state.backend_url, device_id).await;
        let started = state.finish_device_start(&room_id, ticket, token.as_deref());
        if token.is_none() {
            tracing::warn!("Could not start device session {}", room_id);
            state.leave_room(&room_id, format);
            let _ = socket.send(text(json!({"type": "START_FAILED"}))).await;
            return;
        }
        if started {
            tracing::info!("Started device session {}", room_id);
        }
    }

    tracing::info!("WebSocket client registered for {}", room_id);
    let _ = socket.send(text(json!({"type": "AUTH_SUCCESS"}))).await;
    if let Some(frame) = joined.cached_frame {
        let _ = socket.send(frame_message(format, frame)).await;
    }

    let (mut sender, mut receiver) = socket.split();

    let mut start_failed_rx = joined.start_failed_rx;
    let mut send_task = tokio::spawn(async move {
        let frames = async {
            match joined.rx {
                ViewerRx::Png(rx) => {
                    forward(rx, &mut sender, |msg| Message::Text(msg.into())).await
                }
                ViewerRx::Raw(rx) => forward(rx, &mut sender, Message::Binary).await,
            }
        };
        tokio::select! {
            _ = frames => {}
            _ = start_failed_rx.recv() => {
                let _ = sender.send(text(json!({"type": "START_FAILED"}))).await;
            }
        }
    });

    let cmd_tx_clone = state.cmd_tx.clone();
    let uid_clone = room_id.clone();
    // Virtual devices only follow the backend; their viewers are read just to notice the close.
    let forward_input = !is_device_room(&room_id);
    let mut recv_task = tokio::spawn(async move {
        let mut limiter = ViewerLimiter::new();
        while let Some(msg_result) = receiver.next().await {
            if !forward_input {
                continue;
            }
            // A flooding viewer must not fill the shared command channel, which resyncs everyone
            let size = match &msg_result {
                Ok(Message::Text(text)) => text.len(),
                Ok(Message::Binary(bin)) => bin.len(),
                _ => 0,
            };
            if size > 0 && !limiter.allow(size) {
                tracing::debug!("Dropping message of {} over the rate limit", uid_clone);
                continue;
            }
            match msg_result {
                Ok(Message::Text(text)) => {
                    if let Ok(json_data) = serde_json::from_str::<serde_json::Value>(&text) {
                        let cmd =
                            json!({"event": "message", "user_id": uid_clone, "data": json_data});
                        let _ = cmd_tx_clone.send(cmd.to_string() + "\n");
                    }
                }
                Ok(Message::Binary(bin)) => {
                    use base64::{engine::general_purpose::STANDARD, Engine as _};
                    let b64 = STANDARD.encode(&bin);
                    let cmd = json!({"event": "binary", "user_id": uid_clone, "data": b64});
                    let _ = cmd_tx_clone.send(cmd.to_string() + "\n");
                }
                _ => {}
            }
        }
    });

    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
        _ = resync_rx.changed() => {
            tracing::info!("Preview service reconnected, closing websocket for {}", room_id);
            send_task.abort();
            recv_task.abort();
        }
    }

    if state.leave_room(&room_id, format) {
        tracing::info!("All websockets closed for {}", room_id);
    }
}

async fn forward<T: Clone, S>(
    mut rx: broadcast::Receiver<T>,
    sender: &mut S,
    to_message: impl Fn(T) -> Message,
) where
    S: SinkExt<Message> + Unpin,
{
    loop {
        let msg = match rx.recv().await {
            Ok(msg) => msg,
            // Only the newest frame matters, so a slow client just skips ahead.
            Err(RecvError::Lagged(_)) => continue,
            Err(RecvError::Closed) => break,
        };
        if sender.send(to_message(msg)).await.is_err() {
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{state::test_state_with, util::ip_limits::IpLimits};
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{self, client::IntoClientRequest, handshake::client::Request},
    };

    async fn serve(limits: IpLimits) -> (Arc<AppState>, String) {
        let (state, _) = test_state_with(limits);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/", listener.local_addr().unwrap());
        let app = axum::Router::new()
            .route("/", axum::routing::get(ws_handler))
            .with_state(state.clone());
        tokio::spawn(async move {
            axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
        });
        (state, url)
    }

    fn request(url: &str, cf_ip: Option<&str>) -> Request {
        let mut req = url.into_client_request().unwrap();
        if let Some(ip) = cf_ip {
            req.headers_mut()
                .insert("cf-connecting-ip", ip.parse().unwrap());
        }
        req
    }

    async fn is_refused(req: Request) -> bool {
        match connect_async(req).await {
            Err(tungstenite::Error::Http(res)) => res.status() == StatusCode::TOO_MANY_REQUESTS,
            _ => false,
        }
    }

    #[tokio::test]
    async fn refuses_connections_over_the_per_ip_cap() {
        let (_, url) = serve(IpLimits::new(1, 30)).await;
        let _first = connect_async(request(&url, Some("203.0.113.7")))
            .await
            .unwrap();

        assert!(is_refused(request(&url, Some("203.0.113.7"))).await);
        assert!(connect_async(request(&url, Some("203.0.113.8")))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn frees_the_slot_when_a_socket_closes_before_auth() {
        let (_, url) = serve(IpLimits::new(1, 30)).await;
        let (mut ws, _) = connect_async(request(&url, None)).await.unwrap();
        assert!(is_refused(request(&url, None)).await);

        ws.close(None).await.unwrap();
        let reconnected = tokio::time::timeout(Duration::from_secs(2), async {
            while connect_async(request(&url, None)).await.is_err() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(reconnected.is_ok(), "slot was not released");
    }

    #[tokio::test]
    async fn answers_rate_limited_before_asking_the_backend() {
        let (state, url) = serve(IpLimits::new(10, 1)).await;
        assert!(state.ip_limits.allow_auth("127.0.0.1".parse().unwrap()));

        let (mut ws, _) = connect_async(request(&url, None)).await.unwrap();
        let auth = json!({"type": "AUTH", "token": "token"}).to_string();
        ws.send(tungstenite::Message::text(auth)).await.unwrap();

        let reply = ws.next().await.unwrap().unwrap();
        let reply: serde_json::Value = serde_json::from_str(reply.to_text().unwrap()).unwrap();
        assert_eq!(reply, json!({"type": "RATE_LIMITED"}));

        let closed = tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(Ok(msg)) = ws.next().await {
                if msg.is_close() {
                    break;
                }
            }
        })
        .await;
        assert!(closed.is_ok(), "socket stayed open");
    }
}
