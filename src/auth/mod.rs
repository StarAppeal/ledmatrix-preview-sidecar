use serde::Deserialize;
use std::time::Duration;

#[derive(Deserialize)]
pub struct AuthMsg {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub token: String,
    /// Watch this virtual device instead of the user's own preview.
    pub device: Option<String>,
    /// `"raw"` for binary 64x64 RGB frames, anything else keeps PNG.
    pub format: Option<String>,
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

pub fn is_object_id(id: &str) -> bool {
    id.len() == 24 && id.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Checked for every viewer; unlike the session call it doesn't rotate the device token.
pub async fn owns_virtual_device(
    client: &reqwest::Client,
    token: &str,
    backend_url: &str,
    device_id: &str,
) -> bool {
    let url = format!("{}/api/devices", backend_url);
    let Ok(res) = client.get(&url).bearer_auth(token).send().await else {
        return false;
    };
    let Ok(json) = res.json::<serde_json::Value>().await else {
        return false;
    };
    json["data"].as_array().is_some_and(|devices| {
        devices
            .iter()
            .any(|device| device["id"] == device_id && device["kind"] == "virtual")
    })
}

/// Fetches a fresh device token. Rotating it closes a running session of the device,
/// so this is only called when a session starts.
pub async fn start_device_session(
    client: &reqwest::Client,
    token: &str,
    backend_url: &str,
    device_id: &str,
) -> Option<String> {
    let url = format!("{}/api/devices/{}/session", backend_url, device_id);
    let res = client.post(&url).bearer_auth(token).send().await.ok()?;
    let json = res.json::<serde_json::Value>().await.ok()?;
    json["data"]["token"].as_str().map(str::to_string)
}
