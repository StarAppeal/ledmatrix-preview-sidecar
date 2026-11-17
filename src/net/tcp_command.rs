use std::sync::Arc;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::broadcast::error::RecvError,
};

use crate::{
    net::{handshake::authenticate, ACCEPT_RETRY_DELAY},
    state::{AppState, PreviewLink},
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
            let _link = state.track_preview_link(PreviewLink::Command);
            let mut rx = state.cmd_tx.subscribe();
            state.reset_device_sessions();
            state.resync_tx.send_modify(|generation| *generation += 1);
            let (mut reader, mut writer) = socket.into_split();
            let mut probe = [0u8; 64];
            loop {
                let received = tokio::select! {
                    received = rx.recv() => received,
                    // The preview service never writes after the handshake; EOF means it is gone,
                    // which the health check has to see even while no command is being sent
                    read = reader.read(&mut probe) => match read {
                        Ok(0) | Err(_) => break,
                        Ok(_) => continue,
                    },
                };
                let msg = match received {
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
                if writer.write_all(msg.as_bytes()).await.is_err() {
                    break;
                }
            }
            tracing::info!("TCP command client disconnected from {}", peer_addr);
        });
    }
}
