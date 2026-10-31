use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use tokio::sync::broadcast::{self, error::RecvError};

use crate::{
    auth::{is_object_id, owns_virtual_device, start_device_session, verify_token, AuthMsg},
    net::frame_payload,
    state::{is_device_room, AppState, FrameFormat, ViewerRx, DEVICE_ROOM_PREFIX},
};

// Legit binaries are 64x64 frames; this caps what reaches the preview decoder.
const MAX_MESSAGE_SIZE: usize = 1024 * 1024;
const AUTH_TIMEOUT: Duration = Duration::from_secs(10);

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.max_message_size(MAX_MESSAGE_SIZE)
        .max_frame_size(MAX_MESSAGE_SIZE)
        .on_upgrade(|socket| handle_socket(socket, state))
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

async fn authenticate(socket: &mut WebSocket, state: &AppState) -> Option<Viewer> {
    let Ok(Some(Ok(Message::Text(msg)))) = tokio::time::timeout(AUTH_TIMEOUT, socket.next()).await
    else {
        return None;
    };
    let auth = serde_json::from_str::<AuthMsg>(&msg)
        .ok()
        .filter(|auth| auth.msg_type == "AUTH")?;

    let user_id = verify_token(&state.http, &auth.token, &state.backend_url).await;
    if user_id.is_empty() {
        tracing::warn!("WebSocket auth failed: empty user_id");
        return None;
    }

    let format = match auth.format.as_deref() {
        Some("raw") => FrameFormat::Raw,
        _ => FrameFormat::Png,
    };

    let room_id = match auth.device {
        None => user_id,
        Some(device_id) => {
            if !is_object_id(&device_id)
                || !owns_virtual_device(&state.http, &auth.token, &state.backend_url, &device_id)
                    .await
            {
                tracing::warn!("User {} may not watch device {}", user_id, device_id);
                return None;
            }
            format!("{}{}", DEVICE_ROOM_PREFIX, device_id)
        }
    };

    Some(Viewer {
        room_id,
        format,
        jwt: auth.token,
    })
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let Some(viewer) = authenticate(&mut socket, &state).await else {
        let _ = socket.send(text(json!({"type": "ERROR"}))).await;
        return;
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
        while let Some(msg_result) = receiver.next().await {
            if !forward_input {
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
