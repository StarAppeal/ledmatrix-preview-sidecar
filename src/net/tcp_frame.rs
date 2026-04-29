use axum::extract::ws::Message;
use std::sync::Arc;
use tokio::{io::AsyncReadExt, net::TcpListener};

use crate::{state::AppState, util::encode_fast_png};

// TCP frame listener (Python -> Rust).
pub async fn start_frame_listener(state: Arc<AppState>) {
    let listener = TcpListener::bind("0.0.0.0:5001").await.unwrap();
    tracing::info!("TCP frame server started on port 5001");

    while let Ok((mut socket, _)) = listener.accept().await {
        let state = state.clone();
        tokio::spawn(async move {
            let mut len_buf = [0u8; 1];
            loop {
                // Protocol: 1 byte user-id length, N bytes user-id, 12288 bytes RGB data
                if socket.read_exact(&mut len_buf).await.is_err() {
                    break;
                }
                let len = len_buf[0] as usize;

                let mut id_buf = vec![0u8; len];
                if socket.read_exact(&mut id_buf).await.is_err() {
                    break;
                }
                let user_id = String::from_utf8_lossy(&id_buf).to_string();

                let mut frame_buf = vec![0u8; 12288];
                if socket.read_exact(&mut frame_buf).await.is_err() {
                    break;
                }

                if let Some(tx) = state.ws_clients.get(&user_id) {
                    let tx = tx.clone();
                    // Offload CPU-heavy encoding to the blocking thread pool.
                    tokio::task::spawn_blocking(move || {
                        let base64_png = encode_fast_png(&frame_buf);
                        let payload = format!(
                            r#"{{"type":"PREVIEW_FRAME","payload":"data:image/png;base64,{}"}}"#,
                            base64_png
                        );
                        let _ = tx.try_send(Message::Text(payload.into()));
                    });
                }
            }
        });
    }
}
