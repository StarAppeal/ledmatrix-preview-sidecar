use dashmap::DashMap;
use tokio::sync::{broadcast, watch};

pub struct UserRoom {
    pub tx: broadcast::Sender<String>,
    pub connection_count: usize,
    pub last_frame: Option<String>,
}

// Shared state for WebSocket clients and command broadcasting.
pub struct AppState {
    // user_id -> UserRoom
    pub ws_clients: DashMap<String, UserRoom>,
    pub cmd_tx: broadcast::Sender<String>,
    pub backend_url: String,
    pub tcp_secret: Option<String>,
    // Bumped whenever a command client connects; browser sockets close on change
    // and reconnect, which re-creates their session in the new preview instance.
    pub resync_tx: watch::Sender<u64>,
}


