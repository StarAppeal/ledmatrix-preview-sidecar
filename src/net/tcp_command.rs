use std::sync::Arc;
use tokio::{io::AsyncWriteExt, net::TcpListener};

use crate::state::AppState;

pub async fn start_command_listener(state: Arc<AppState>) {
    let listener = TcpListener::bind("0.0.0.0:5002").await.unwrap();
    tracing::info!("TCP command server started on port 5002");

    while let Ok((mut socket, _)) = listener.accept().await {
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


