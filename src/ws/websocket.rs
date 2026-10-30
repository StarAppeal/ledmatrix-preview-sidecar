use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;

use crate::{auth::{AuthMsg, verify_token}, state::AppState};

// Legit binaries are 64x64 frames; this caps what reaches the preview decoder.
const MAX_MESSAGE_SIZE: usize = 1024 * 1024;

pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.max_message_size(MAX_MESSAGE_SIZE)
        .max_frame_size(MAX_MESSAGE_SIZE)
        .on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let mut user_id = String::new();

    if let Some(Ok(Message::Text(text))) = socket.next().await {
        if let Ok(auth_data) = serde_json::from_str::<AuthMsg>(&text) {
            if auth_data.msg_type == "AUTH" {
                user_id = verify_token(&auth_data.token, &state.backend_url).await;
            }
        }
    }

    if user_id.is_empty() {
        tracing::warn!("WebSocket auth failed: empty user_id");
        let _ = socket.send(Message::Text(json!({"type": "ERROR"}).to_string().into())).await;
        return;
    }

    tracing::info!("WebSocket authenticated user_id {}", user_id);
    let _ = socket.send(Message::Text(json!({"type": "AUTH_SUCCESS"}).to_string().into())).await;

    let mut resync_rx = state.resync_tx.subscribe();
    let (mut rx, cached_frame) = state.join_room(&user_id);

    if let Some(frame) = cached_frame {
        let _ = socket.send(Message::Text(frame.into())).await;
    }

    tracing::info!("WebSocket client registered user_id {}", user_id);
    let (mut sender, mut receiver) = socket.split();

    let mut send_task = tokio::spawn(async move {
        loop {
            let msg = match rx.recv().await {
                Ok(msg) => msg,
                // Only the newest frame matters, so a slow client just skips ahead.
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => break,
            };
            if sender.send(Message::Text(msg.into())).await.is_err() {
                break;
            }
        }
    });

    let cmd_tx_clone = state.cmd_tx.clone();
    let uid_clone = user_id.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(msg_result) = receiver.next().await {
            match msg_result {
                Ok(Message::Text(text)) => {
                    if let Ok(json_data) = serde_json::from_str::<serde_json::Value>(&text) {
                        let cmd = json!({"event": "message", "user_id": uid_clone, "data": json_data});
                        let _ = cmd_tx_clone.send(cmd.to_string() + "\n");
                    }
                }
                Ok(Message::Binary(bin)) => {
                    use base64::{Engine as _, engine::general_purpose::STANDARD};
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
            tracing::info!("Preview service reconnected, closing websocket for user_id {}", user_id);
            send_task.abort();
            recv_task.abort();
        }
    }

    if state.leave_room(&user_id) {
        tracing::info!("All websockets closed for user_id {}", user_id);
    }
}
