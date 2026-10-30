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
            // One encoder per user for this connection; dropped (and stopped) with it
            let mut encoders: HashMap<String, watch::Sender<Vec<u8>>> = HashMap::new();
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

                if state.ws_clients.contains_key(&user_id) {
                    let frame_tx = encoders
                        .entry(user_id.clone())
                        .or_insert_with(|| spawn_encoder(state.clone(), user_id.clone()));
                    if let Err(watch::error::SendError(frame)) = frame_tx.send(frame_buf) {
                        // The encoder stopped after the room was gone for a moment; start a new one
                        let frame_tx = spawn_encoder(state.clone(), user_id.clone());
                        let _ = frame_tx.send(frame);
                        encoders.insert(user_id, frame_tx);
                    }
                } else {
                    encoders.remove(&user_id);
                    tracing::warn!("No ws client for user_id {}, dropping frame", user_id);
                }
            }
            tracing::info!("TCP frame client disconnected from {}", peer_addr);
        });
    }
}

/// Encodes one user's frames one at a time. The watch channel only keeps the newest frame, so
/// frames arriving faster than they can be encoded are skipped instead of piling up as tasks, and
/// results can't overtake each other (which made animations jump back and could leave an older
/// frame as `last_frame`).
fn spawn_encoder(state: Arc<AppState>, user_id: String) -> watch::Sender<Vec<u8>> {
    let (frame_tx, mut frame_rx) = watch::channel(Vec::new());
    tokio::spawn(async move {
        // Ends when the frame connection drops its sender
        while frame_rx.changed().await.is_ok() {
            let frame = frame_rx.borrow_and_update().clone();
            // Offload CPU-heavy encoding to the blocking thread pool.
            let Ok(payload) = tokio::task::spawn_blocking(move || frame_payload(&frame)).await else {
                continue;
            };

            let Some(mut room) = state.ws_clients.get_mut(&user_id) else {
                break;
            };
            room.last_frame = Some(payload.clone());
            let _ = room.tx.send(payload);
        }
    });
    frame_tx
}

fn frame_payload(raw_rgb: &[u8]) -> String {
    json!({
        "type": "PREVIEW_FRAME",
        "payload": format!("data:image/png;base64,{}", encode_fast_png(raw_rgb))
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use dashmap::DashMap;
    use std::time::Duration;
    use tokio::sync::broadcast;

    fn state() -> Arc<AppState> {
        Arc::new(AppState {
            ws_clients: DashMap::new(),
            cmd_tx: broadcast::channel(16).0,
            backend_url: String::new(),
            http: reqwest::Client::new(),
            tcp_secret: None,
            resync_tx: watch::channel(0).0,
        })
    }

    fn frame(value: u8) -> Vec<u8> {
        vec![value; 12288]
    }

    #[tokio::test]
    async fn the_newest_frame_wins_and_is_cached() {
        let state = state();
        let (mut rx, _) = state.join_room("u1");
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

        // Skipped frames are fine, frames arriving out of order are not
        assert!(received.len() < 50);
        assert_eq!(
            state.ws_clients.get("u1").unwrap().last_frame.as_ref(),
            Some(&newest)
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err(), "an older frame arrived after the newest");
    }

    #[tokio::test]
    async fn stops_once_the_room_is_gone() {
        let state = state();
        let frame_tx = spawn_encoder(state.clone(), "nobody".to_string());

        frame_tx.send(frame(1)).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(frame_tx.send(frame(2)).is_err());
    }
}
