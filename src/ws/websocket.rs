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

// WebSocket entrypoint with auth flow.
pub async fn ws_handler(ws: WebSocketUpgrade, State(state): State<Arc<AppState>>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_socket(socket, state))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<AppState>) {
    let mut user_id = String::new();

    // 1. Wait for AUTH message
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
        let _ = socket
            .send(Message::Text(json!({"type": "ERROR"}).to_string().into()))
            .await;
        return;
    }

    let _ = socket
        .send(Message::Text(
            json!({"type": "AUTH_SUCCESS"}).to_string().into(),
        ))
        .await;

    // Send connect event to Python
    let _ = state.cmd_tx.send(json!({"event": "connect", "user_id": user_id}).to_string() + "\n");

    let (mut sender, mut receiver) = socket.split();
    let (tx, mut rx) = mpsc::channel::<Message>(32);
    state.ws_clients.insert(user_id.clone(), tx);

    // Task: send to WebSocket
    let mut send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sender.send(msg).await.is_err() {
                break;
            }
        }
    });

    // Task: receive from WebSocket and forward to Python
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
    };

    // Cleanup after disconnect
    state.ws_clients.remove(&user_id);
    let _ = state.cmd_tx.send(json!({"event": "disconnect", "user_id": user_id}).to_string() + "\n");
}
