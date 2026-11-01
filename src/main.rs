use axum::{routing::get, Router};
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
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt::init();

    let tcp_secret = std::env::var("PREVIEW_TCP_SECRET")
        .ok()
        .filter(|s| !s.is_empty());
    if tcp_secret.is_none() {
        if std::env::var("PREVIEW_ALLOW_INSECURE_TCP").as_deref() != Ok("1") {
            return Err(std::io::Error::other(
                "PREVIEW_TCP_SECRET is not set; set it, or PREVIEW_ALLOW_INSECURE_TCP=1 for local development",
            ));
        }
        tracing::warn!("PREVIEW_TCP_SECRET is not set, TCP ports 5001/5002 accept any client");
    }

    let max_device_rooms = match std::env::var("MAX_DEVICE_SESSIONS") {
        Err(_) => 50,
        Ok(value) => match value.trim().parse::<usize>() {
            Ok(n) if n > 0 => n,
            _ => {
                return Err(std::io::Error::other(format!(
                    "MAX_DEVICE_SESSIONS must be a positive integer, got {:?}",
                    value
                )))
            }
        },
    };

    let (cmd_tx, _) = tokio::sync::broadcast::channel(1024);
    let state = Arc::new(AppState {
        ws_clients: dashmap::DashMap::new(),
        cmd_tx,
        backend_url: std::env::var("BACKEND_INTERNAL_URL")
            .unwrap_or_else(|_| "http://ledmatrix-backend:3000".to_string()),
        http: auth::http_client(),
        tcp_secret,
        resync_tx: tokio::sync::watch::channel(0).0,
        max_device_rooms,
        device_grace: std::time::Duration::from_secs(60),
        device_join_lock: std::sync::Mutex::new(()),
    });

    // Bind everything up front so a taken port fails startup instead of
    // silently killing a spawned task.
    let frame_listener = TcpListener::bind("0.0.0.0:5001").await?;
    let command_listener = TcpListener::bind("0.0.0.0:5002").await?;
    let addr = SocketAddr::from(([0, 0, 0, 0], 8765));
    let ws_listener = TcpListener::bind(&addr).await?;

    let mut frame_task = tokio::spawn(start_frame_listener(frame_listener, state.clone()));
    let mut command_task = tokio::spawn(start_command_listener(command_listener, state.clone()));

    let app = Router::new().route("/", get(ws_handler)).with_state(state);

    tracing::info!("Rust WebSocket server listening on {}", addr);
    let mut ws_task = tokio::spawn(async move { axum::serve(ws_listener, app).await });

    tokio::select! {
        res = &mut frame_task => tracing::error!("TCP frame listener stopped: {:?}", res),
        res = &mut command_task => tracing::error!("TCP command listener stopped: {:?}", res),
        res = &mut ws_task => tracing::error!("WebSocket server stopped: {:?}", res),
    }

    Err(std::io::Error::other(
        "a listener task terminated unexpectedly",
    ))
}
