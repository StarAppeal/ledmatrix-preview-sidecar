use serde_json::json;
use std::sync::Arc;
use tokio::{io::AsyncReadExt, net::TcpListener};

use crate::{
    net::{handshake::authenticate, ACCEPT_RETRY_DELAY},
    state::AppState,
    util::encode_fast_png,
};

pub async fn start_frame_listener(listener: TcpListener, state: Arc<AppState>) {
    tracing::info!("TCP frame server started on port 5001");

    loop {
        let (mut socket, peer_addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(err) => {
                tracing::warn!("TCP frame accept failed: {}", err);
                tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
                continue;
            }
        };
        let state = state.clone();
        tokio::spawn(async move {
            if !authenticate(&mut socket, state.tcp_secret.as_deref()).await {
                tracing::warn!("TCP frame handshake failed from {}", peer_addr);
                return;
            }
            tracing::info!("TCP frame client connected from {}", peer_addr);
            let mut len_buf = [0u8; 1];
            loop {
                // Protocol: 1 byte user-id length, N bytes user-id, 12288 bytes RGB data
                if let Err(err) = socket.read_exact(&mut len_buf).await {
                    tracing::warn!(
                        "TCP frame read failed (user-id length) from {}: {}",
                        peer_addr,
                        err
                    );
                    break;
                }
                let len = len_buf[0] as usize;
                if len == 0 {
                    tracing::warn!("TCP frame received empty user-id length from {}", peer_addr);
                }

                let mut id_buf = vec![0u8; len];
                if let Err(err) = socket.read_exact(&mut id_buf).await {
                    tracing::warn!(
                        "TCP frame read failed (user-id bytes, len {}) from {}: {}",
                        len,
                        peer_addr,
                        err
                    );
                    break;
                }
                let user_id = String::from_utf8_lossy(&id_buf).to_string();

                let mut frame_buf = vec![0u8; 12288];
                if let Err(err) = socket.read_exact(&mut frame_buf).await {
                    tracing::warn!(
                        "TCP frame read failed (frame bytes) from {} for user_id {}: {}",
                        peer_addr,
                        user_id,
                        err
                    );
                    break;
                }

                if let Some(room) = state.ws_clients.get(&user_id) {
                    let tx = room.tx.clone();
                    let state_clone = state.clone();
                    let uid = user_id.clone();

                    // Offload CPU-heavy encoding to the blocking thread pool.
                    tokio::task::spawn_blocking(move || {
                        let base64_png = encode_fast_png(&frame_buf);

                        let payload = json!({
                            "type": "PREVIEW_FRAME",
                            "payload": format!("data:image/png;base64,{}", base64_png)
                        })
                        .to_string();

                        if let Some(mut room) = state_clone.ws_clients.get_mut(&uid) {
                            room.last_frame = Some(payload.clone());
                        }

                        let _ = tx.send(payload);
                    });
                } else {
                    tracing::warn!("No ws client for user_id {}, dropping frame", user_id);
                }
            }
            tracing::info!("TCP frame client disconnected from {}", peer_addr);
        });
    }
}
