use std::sync::Arc;
use tokio::{io::AsyncWriteExt, net::TcpListener, sync::broadcast::error::RecvError};

use crate::{
    net::{handshake::authenticate, ACCEPT_RETRY_DELAY},
    state::AppState,
};

pub async fn start_command_listener(listener: TcpListener, state: Arc<AppState>) {
    tracing::info!("TCP command server started on port 5002");

    loop {
        let (mut socket, peer_addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(err) => {
                tracing::warn!("TCP command accept failed: {}", err);
                tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
                continue;
            }
        };
        let state = state.clone();
        tokio::spawn(async move {
            if !authenticate(&mut socket, state.tcp_secret.as_deref()).await {
                tracing::warn!("TCP command handshake failed from {}", peer_addr);
                return;
            }
            tracing::info!("TCP command client connected from {}", peer_addr);
            let mut rx = state.cmd_tx.subscribe();
            state.resync_tx.send_modify(|generation| *generation += 1);
            loop {
                let msg = match rx.recv().await {
                    Ok(msg) => msg,
                    // Lost commands would desync the sessions; closing makes the preview
                    // service drop them and reconnect, which triggers a full resync.
                    Err(RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            "TCP command client {} lagged by {} messages, closing for resync",
                            peer_addr,
                            skipped
                        );
                        break;
                    }
                    Err(RecvError::Closed) => break,
                };
                if socket.write_all(msg.as_bytes()).await.is_err() {
                    break;
                }
            }
            tracing::info!("TCP command client disconnected from {}", peer_addr);
        });
    }
}


