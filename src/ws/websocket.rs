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
use tokio::sync::mpsc;

use crate::{auth::{AuthMsg, verify_token}, state::AppState};
use crate::state::app_state::UserRoom;

// WebSocket entrypoint with auth flow.
pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let mut user_id = String::new();

    if let Some(msg) = socket.next().await {
        if let Ok(Message::Text(text)) = msg {
            if let Ok(auth_data) = serde_json::from_str::<AuthMsg>(&text) {
                if auth_data.msg_type == "AUTH" {
                    user_id = verify_token(&auth_data.token, &state.backend_url).await;
                }
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

    let (mut rx, is_first) = {
        let mut room = state.ws_clients.entry(user_id.clone()).or_insert_with(|| {
            let (tx, _) = tokio::sync::broadcast::channel(16);
            UserRoom { tx, connection_count: 0 }
        });

        room.connection_count += 1;
        (room.tx.subscribe(), room.connection_count == 1)
    };

    if is_first {
        let _ = state.cmd_tx.send(json!({"event": "connect", "user_id": user_id}).to_string() + "\n");
    }

    tracing::info!("WebSocket client registered user_id {}", user_id);
    let (mut sender, mut receiver) = socket.split();

    let mut send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if sender.send(Message::Text(msg.into())).await.is_err() {
                break;
            }
        }
    });

    let cmd_tx_clone = state.cmd_tx.clone();
    let uid_clone = user_id.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(Message::Text(text))) = receiver.next().await {
            if let Ok(json_data) = serde_json::from_str::<serde_json::Value>(&text) {
                let cmd = json!({"event": "message", "user_id": uid_clone, "data": json_data});
                let _ = cmd_tx_clone.send(cmd.to_string() + "\n");
            }
        }
    });

    tokio::select! {
        _ = (&mut send_task) => recv_task.abort(),
        _ = (&mut recv_task) => send_task.abort(),
    }

    let is_last = {
        if let Some(mut room) = state.ws_clients.get_mut(&user_id) {
            room.connection_count -= 1;
            room.connection_count == 0
        } else {
            false
        }
    };

    if is_last {
        state.ws_clients.remove(&user_id);
        tracing::info!("All websockets closed for user_id {}", user_id);
        let _ = state.cmd_tx.send(json!({"event": "disconnect", "user_id": user_id}).to_string() + "\n");
    }
}
