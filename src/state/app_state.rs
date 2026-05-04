use dashmap::DashMap;
use tokio::sync::{broadcast};

pub struct UserRoom {
    pub tx: broadcast::Sender<String>,
    pub connection_count: usize,
}

// Shared state for WebSocket clients and command broadcasting.
pub struct AppState {
    // user_id -> UserRoom
    pub ws_clients: DashMap<String, UserRoom>,
    pub cmd_tx: broadcast::Sender<String>,
    pub backend_url: String,
}


