use std::sync::Arc;
use tokio::{io::AsyncWriteExt, net::TcpListener};

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
            while let Ok(msg) = rx.recv().await {
                if socket.write_all(msg.as_bytes()).await.is_err() {
                    break;
                }
            }
        });
    }
}


