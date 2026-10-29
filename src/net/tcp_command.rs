use std::sync::Arc;
use tokio::{io::AsyncWriteExt, net::TcpListener};

use crate::{net::ACCEPT_RETRY_DELAY, state::AppState};

pub async fn start_command_listener(listener: TcpListener, state: Arc<AppState>) {
    tracing::info!("TCP command server started on port 5002");

    loop {
        let mut socket = match listener.accept().await {
            Ok((socket, _)) => socket,
            Err(err) => {
                tracing::warn!("TCP command accept failed: {}", err);
                tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
                continue;
            }
        };
        let mut rx = state.cmd_tx.subscribe();
        tokio::spawn(async move {
            while let Ok(msg) = rx.recv().await {
                if socket.write_all(msg.as_bytes()).await.is_err() {
                    break;
                }
            }
        });
    }
}


