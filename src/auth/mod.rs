use serde::Deserialize;

#[derive(Deserialize)]
pub struct AuthMsg {
    #[serde(rename = "type")]
    pub msg_type: String,
    pub token: String,
}

pub async fn verify_token(token: &str, backend_url: &str) -> String {
    let client = reqwest::Client::new();
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
