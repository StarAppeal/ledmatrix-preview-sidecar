use axum::{
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use std::{sync::Arc, time::Duration};

use crate::state::AppState;

pub const HEALTH_PATH: &str = "/healthz";
const HEALTHCHECK_URL: &str = "http://127.0.0.1:8765/healthz";

/// Status for the backend's deep health check and Docker. Requests through Cloudflare carry
/// `CF-Connecting-IP` and get a 404, so the load figures stay internal.
pub async fn health_handler(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    if headers.contains_key("cf-connecting-ip") {
        return StatusCode::NOT_FOUND.into_response();
    }
    Json(state.health()).into_response()
}

/// `rust-preview healthcheck`: the image has no curl, so Docker probes with the binary itself.
pub async fn run_healthcheck() -> std::io::Result<()> {
    let healthy = reqwest::Client::new()
        .get(HEALTHCHECK_URL)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .is_ok_and(|res| res.status().is_success());
    if healthy {
        Ok(())
    } else {
        Err(std::io::Error::other("health check failed"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        state::{test_state_with, PreviewLink},
        util::ip_limits::IpLimits,
    };
    use axum::{routing::get, Router};
    use serde_json::{json, Value};

    async fn serve(state: Arc<AppState>) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}{}", listener.local_addr().unwrap(), HEALTH_PATH);
        let app = Router::new()
            .route(HEALTH_PATH, get(health_handler))
            .with_state(state);
        tokio::spawn(async move { axum::serve(listener, app).await });
        url
    }

    #[tokio::test]
    async fn reports_whether_the_preview_service_is_connected() {
        let (state, _cmd_rx) = test_state_with(IpLimits::new(10, 30));
        let url = serve(state.clone()).await;

        let body: Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
        assert_eq!(body["status"], json!("degraded"));
        assert_eq!(body["preview_connected"], json!(false));

        let command = state.track_preview_link(PreviewLink::Command);
        let body: Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
        assert_eq!(body["status"], json!("ok"));
        assert_eq!(body["frame_link"], json!(false));
        assert_eq!(body["max_device_sessions"], json!(2));

        drop(command);
        let body: Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
        assert_eq!(body["preview_connected"], json!(false));
    }

    #[tokio::test]
    async fn notices_when_the_preview_service_closes_its_command_socket() {
        use crate::net::start_command_listener;
        use tokio::io::AsyncWriteExt;

        let (state, _cmd_rx) = test_state_with(IpLimits::new(10, 30));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(start_command_listener(listener, state.clone()));

        let wait_for = |connected: bool| {
            let state = state.clone();
            async move {
                tokio::time::timeout(Duration::from_secs(2), async {
                    while state.preview_links.connected() != connected {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                })
                .await
                .is_ok()
            }
        };

        let mut preview = tokio::net::TcpStream::connect(addr).await.unwrap();
        // test_state has no TCP secret, so the handshake is an empty line
        preview.write_all(b"\n").await.unwrap();
        assert!(wait_for(true).await, "command link not counted");

        drop(preview);
        assert!(wait_for(false).await, "closed command link still counted");
    }

    #[tokio::test]
    async fn hides_the_status_from_requests_through_cloudflare() {
        let (state, _cmd_rx) = test_state_with(IpLimits::new(10, 30));
        let url = serve(state).await;

        let res = reqwest::Client::new()
            .get(&url)
            .header("CF-Connecting-IP", "203.0.113.7")
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::NOT_FOUND);
    }
}
