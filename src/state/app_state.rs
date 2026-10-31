use dashmap::DashMap;
use serde_json::json;
use tokio::sync::{broadcast, watch};

pub struct UserRoom {
    pub tx: broadcast::Sender<String>,
    pub connection_count: usize,
    pub last_frame: Option<String>,
}

// Shared state for WebSocket clients and command broadcasting.
pub struct AppState {
    // user_id -> UserRoom
    pub ws_clients: DashMap<String, UserRoom>,
    pub cmd_tx: broadcast::Sender<String>,
    pub backend_url: String,
    pub http: reqwest::Client,
    pub tcp_secret: Option<String>,
    // Bumped whenever a command client connects; browser sockets close on change
    // and reconnect, which re-creates their session in the new preview instance.
    pub resync_tx: watch::Sender<u64>,
}

impl AppState {
    /// Registers a socket in the user's room and returns its frame receiver plus the cached frame.
    // connect/disconnect are sent while holding the entry lock so they reach the
    // preview service in the same order as the room changes.
    pub fn join_room(&self, user_id: &str) -> (broadcast::Receiver<String>, Option<String>) {
        let mut room = self
            .ws_clients
            .entry(user_id.to_string())
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(16);
                UserRoom {
                    tx,
                    connection_count: 0,
                    last_frame: None,
                }
            });

        room.connection_count += 1;
        if room.connection_count == 1 {
            let _ = self.cmd_tx.send(command("connect", user_id));
        }
        (room.tx.subscribe(), room.last_frame.clone())
    }

    /// Unregisters a socket; the last one removes the room and ends the preview session.
    pub fn leave_room(&self, user_id: &str) -> bool {
        self.ws_clients
            .remove_if_mut(user_id, |_, room| {
                room.connection_count -= 1;
                if room.connection_count > 0 {
                    return false;
                }
                let _ = self.cmd_tx.send(command("disconnect", user_id));
                true
            })
            .is_some()
    }
}

fn command(event: &str, user_id: &str) -> String {
    json!({"event": event, "user_id": user_id}).to_string() + "\n"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> (AppState, broadcast::Receiver<String>) {
        let (cmd_tx, cmd_rx) = broadcast::channel(16);
        let state = AppState {
            ws_clients: DashMap::new(),
            cmd_tx,
            backend_url: String::new(),
            http: reqwest::Client::new(),
            tcp_secret: None,
            resync_tx: watch::channel(0).0,
        };
        (state, cmd_rx)
    }

    fn next_event(rx: &mut broadcast::Receiver<String>) -> Option<String> {
        let msg = rx.try_recv().ok()?;
        let value: serde_json::Value = serde_json::from_str(&msg).unwrap();
        Some(value["event"].as_str().unwrap().to_string())
    }

    #[test]
    fn first_join_and_last_leave_send_commands_once() {
        let (state, mut rx) = state();

        state.join_room("u1");
        state.join_room("u1");
        assert_eq!(next_event(&mut rx).as_deref(), Some("connect"));
        assert_eq!(next_event(&mut rx), None);

        assert!(!state.leave_room("u1"));
        assert_eq!(next_event(&mut rx), None);
        assert!(state.leave_room("u1"));
        assert_eq!(next_event(&mut rx).as_deref(), Some("disconnect"));
        assert!(state.ws_clients.is_empty());
    }

    #[test]
    fn rejoin_after_last_leave_reconnects() {
        let (state, mut rx) = state();

        state.join_room("u1");
        state.leave_room("u1");
        state.join_room("u1");

        assert_eq!(next_event(&mut rx).as_deref(), Some("connect"));
        assert_eq!(next_event(&mut rx).as_deref(), Some("disconnect"));
        assert_eq!(next_event(&mut rx).as_deref(), Some("connect"));
        assert_eq!(state.ws_clients.get("u1").unwrap().connection_count, 1);
    }

    #[test]
    fn leave_without_room_is_noop() {
        let (state, mut rx) = state();
        assert!(!state.leave_room("u1"));
        assert_eq!(next_event(&mut rx), None);
    }

    #[test]
    fn join_returns_cached_frame() {
        let (state, _rx) = state();
        state.join_room("u1");
        state.ws_clients.get_mut("u1").unwrap().last_frame = Some("frame".into());
        let (_, cached) = state.join_room("u1");
        assert_eq!(cached.as_deref(), Some("frame"));
    }
}
