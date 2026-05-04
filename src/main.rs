use axum::{
    routing::get,
    Router,
};
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;

mod auth;
mod net;
mod state;
mod util;
mod ws;

use net::{start_command_listener, start_frame_listener};
use state::AppState;
use ws::ws_handler;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let (cmd_tx, _) = tokio::sync::broadcast::channel(100);
    let state = Arc::new(AppState {
        ws_clients: dashmap::DashMap::new(),
        cmd_tx,
        backend_url: std::env::var("BACKEND_INTERNAL_URL")
            .unwrap_or_else(|_| "http://ledmatrix-backend:3000".to_string()),
    });

    // 1. Start the TCP server for frames
    let state_clone = state.clone();
    tokio::spawn(async move { start_frame_listener(state_clone).await });

    let state_clone2 = state.clone();
    tokio::spawn(async move { start_command_listener(state_clone2).await });

    // 3. Start the public WebSocket server for users
    let app = Router::new()
        .route("/", get(ws_handler))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8765));
    tracing::info!("Rust WebSocket server listening on {}", addr);
    let listener = TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
