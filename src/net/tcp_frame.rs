use axum::body::Bytes;
use serde_json::json;
use std::{collections::HashMap, sync::Arc};
use tokio::{io::AsyncReadExt, net::TcpListener, sync::watch};

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
            let mut encoders: HashMap<String, watch::Sender<Bytes>> = HashMap::new();
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

                let frame = Bytes::from(frame_buf);
                let wants_png = match state.ws_clients.get_mut(&user_id) {
                    Some(mut room) => {
                        room.last_frame = Some(frame.clone());
                        if room.raw_viewers > 0 {
                            let _ = room.raw_tx.send(frame.clone());
                        }
                        room.png_viewers > 0
                    }
                    None => {
                        encoders.remove(&user_id);
                        tracing::warn!("No ws client for user_id {}, dropping frame", user_id);
                        continue;
                    }
                };

                // PNG is only encoded while someone in the room wants it
                if wants_png {
                    let frame_tx = encoders
                        .entry(user_id.clone())
                        .or_insert_with(|| spawn_encoder(state.clone(), user_id.clone()));
                    if let Err(watch::error::SendError(frame)) = frame_tx.send(frame) {
                        // The encoder stopped after the room was gone for a moment; start a new one
                        let frame_tx = spawn_encoder(state.clone(), user_id.clone());
                        let _ = frame_tx.send(frame);
                        encoders.insert(user_id, frame_tx);
                    }
                }
            }
            tracing::info!("TCP frame client disconnected from {}", peer_addr);
        });
    }
}

fn spawn_encoder(state: Arc<AppState>, user_id: String) -> watch::Sender<Bytes> {
    let (frame_tx, mut frame_rx) = watch::channel(Bytes::new());
    tokio::spawn(async move {
        while frame_rx.changed().await.is_ok() {
            let frame = frame_rx.borrow_and_update().clone();
            // Offload CPU-heavy encoding to the blocking thread pool.
            let Ok(payload) = tokio::task::spawn_blocking(move || frame_payload(&frame)).await else {
                continue;
            };

            let Some(room) = state.ws_clients.get(&user_id) else {
                break;
            };
            let _ = room.png_tx.send(payload);
        }
    });
    frame_tx
}

pub fn frame_payload(raw_rgb: &[u8]) -> String {
    json!({
        "type": "PREVIEW_FRAME",
        "payload": format!("data:image/png;base64,{}", encode_fast_png(raw_rgb))
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{test_state, FrameFormat, ViewerRx};
    use std::time::Duration;

    fn frame(value: u8) -> Bytes {
        Bytes::from(vec![value; 12288])
    }

    #[tokio::test]
    async fn the_newest_frame_wins() {
        let (state, _) = test_state();
        let ViewerRx::Png(mut rx) = state.join_room("u1", FrameFormat::Png).unwrap().rx else {
            unreachable!()
        };
        let frame_tx = spawn_encoder(state.clone(), "u1".to_string());

        for value in 0..50 {
            frame_tx.send(frame(value)).unwrap();
        }

        let newest = frame_payload(&frame(49));
        let mut received = Vec::new();
        while received.last() != Some(&newest) {
            let msg = tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .expect("newest frame was not sent")
                .unwrap();
            received.push(msg);
        }

        assert!(received.len() < 50);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err(), "an older frame arrived after the newest");
    }

    #[tokio::test]
    async fn stops_once_the_room_is_gone() {
        let (state, _) = test_state();
        let frame_tx = spawn_encoder(state.clone(), "nobody".to_string());

        frame_tx.send(frame(1)).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(frame_tx.send(frame(2)).is_err());
    }
}
