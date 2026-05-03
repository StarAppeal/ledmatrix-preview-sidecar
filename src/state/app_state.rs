use axum::extract::ws::Message;
use dashmap::DashMap;
use tokio::sync::{broadcast, mpsc};

pub struct UserRoom {
    pub tx: broadcast::Sender<String>,
    pub connection_count: usize,
}

// Shared state for WebSocket clients and command broadcasting.
pub struct AppState {
    // user_id -> UserRoom
    pub ws_clients: DashMap<String, UserRoom>,
    // Broadcast channel for commands (connect, clicks) to Python
    pub cmd_tx: broadcast::Sender<String>,
    pub backend_url: String,
}


