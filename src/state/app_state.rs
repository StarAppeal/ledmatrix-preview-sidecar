use axum::extract::ws::Message;
use dashmap::DashMap;
use tokio::sync::{broadcast, mpsc};

// Shared state for WebSocket clients and command broadcasting.
pub struct AppState {
    // user_id -> WebSocket sender
    pub ws_clients: DashMap<String, mpsc::Sender<Message>>,
    // Broadcast channel for commands (connect, clicks) to Python
    pub cmd_tx: broadcast::Sender<String>,
    pub backend_url: String,
}


