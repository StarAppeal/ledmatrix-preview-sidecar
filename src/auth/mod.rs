use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize)]
pub struct AuthMsg {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub token: String,
}

/// Shared by all connections, so the connection pool is reused; the timeout keeps a hanging
/// backend from holding every login attempt forever.
pub fn http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("static reqwest config is valid")
}

pub async fn verify_token(client: &reqwest::Client, token: &str, backend_url: &str) -> String {
    let url = format!("{}/api/user/me", backend_url);
    if let Ok(res) = client.get(&url).bearer_auth(token).send().await {
        if let Ok(json) = res.json::<serde_json::Value>().await {
            if let Some(id) = json["data"]["_id"].as_str() {
                return id.to_string();
            }
        }
    }
    String::new()
}
