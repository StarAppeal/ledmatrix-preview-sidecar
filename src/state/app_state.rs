use axum::body::Bytes;
use dashmap::DashMap;
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, watch};

// Rooms of virtual devices; user ids are Mongo ids and never contain ':'
pub const DEVICE_ROOM_PREFIX: &str = "dev:";

static NEXT_TICKET: AtomicU64 = AtomicU64::new(1);

fn next_ticket() -> u64 {
    NEXT_TICKET.fetch_add(1, Ordering::Relaxed)
}

pub fn is_device_room(room_id: &str) -> bool {
    room_id.starts_with(DEVICE_ROOM_PREFIX)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameFormat {
    Png,
    Raw,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeviceSession {
    Idle,
    Starting(u64),
    Running,
}

pub struct Room {
    pub png_tx: broadcast::Sender<String>,
    pub raw_tx: broadcast::Sender<Bytes>,
    pub png_viewers: usize,
    pub raw_viewers: usize,
    pub last_frame: Option<Bytes>,
    session: DeviceSession,
    // Ticket of the latest join, so a grace timer only closes the room it was started for
    last_join: u64,
}

impl Room {
    fn new() -> Self {
        Room {
            png_tx: broadcast::channel(16).0,
            raw_tx: broadcast::channel(16).0,
            png_viewers: 0,
            raw_viewers: 0,
            last_frame: None,
            session: DeviceSession::Idle,
            last_join: 0,
        }
    }

    pub fn viewers(&self) -> usize {
        self.png_viewers + self.raw_viewers
    }
}

pub enum ViewerRx {
    Png(broadcast::Receiver<String>),
    Raw(broadcast::Receiver<Bytes>),
}

pub struct Joined {
    pub rx: ViewerRx,
    pub cached_frame: Option<Bytes>,
    /// Set for the viewer that has to start the device session, see `finish_device_start`.
    pub start_ticket: Option<u64>,
}

#[derive(Debug)]
pub struct Busy;

// Shared state for WebSocket clients and command broadcasting.
pub struct AppState {
    // room id (user id or dev:<device id>) -> Room
    pub ws_clients: DashMap<String, Room>,
    pub cmd_tx: broadcast::Sender<String>,
    pub backend_url: String,
    pub http: reqwest::Client,
    pub tcp_secret: Option<String>,
    // Bumped whenever a command client connects; browser sockets close on change
    // and reconnect, which re-creates their session in the new preview instance.
    pub resync_tx: watch::Sender<u64>,
    pub max_device_rooms: usize,
    pub device_grace: Duration,
}

impl AppState {
    /// Registers a socket in the room. Preview rooms start their session on the first viewer;
    /// device rooms hand a start ticket to the viewer that has to fetch a device token.
    // Commands are sent while holding the entry lock so they reach the
    // preview service in the same order as the room changes.
    pub fn join_room(&self, room_id: &str, format: FrameFormat) -> Result<Joined, Busy> {
        let device = is_device_room(room_id);
        if device
            && !self.ws_clients.contains_key(room_id)
            && self.device_room_count() >= self.max_device_rooms
        {
            return Err(Busy);
        }

        let mut room = self
            .ws_clients
            .entry(room_id.to_string())
            .or_insert_with(Room::new);

        room.last_join = next_ticket();
        let rx = match format {
            FrameFormat::Png => {
                room.png_viewers += 1;
                ViewerRx::Png(room.png_tx.subscribe())
            }
            FrameFormat::Raw => {
                room.raw_viewers += 1;
                ViewerRx::Raw(room.raw_tx.subscribe())
            }
        };

        let mut start_ticket = None;
        if device {
            if room.session == DeviceSession::Idle {
                let ticket = next_ticket();
                room.session = DeviceSession::Starting(ticket);
                start_ticket = Some(ticket);
            }
        } else if room.viewers() == 1 {
            let _ = self.cmd_tx.send(command("connect", room_id));
        }

        Ok(Joined {
            rx,
            cached_frame: room.last_frame.clone(),
            start_ticket,
        })
    }

    /// Sends `device_connect` with the fetched token, or puts the room back to idle without one,
    /// so the next viewer tries again. Returns whether the session was started.
    pub fn finish_device_start(&self, room_id: &str, ticket: u64, token: Option<&str>) -> bool {
        let Some(mut room) = self.ws_clients.get_mut(room_id) else {
            return false;
        };
        if room.session != DeviceSession::Starting(ticket) {
            return false;
        }
        match token {
            Some(token) => {
                let cmd = json!({"event": "device_connect", "session_id": room_id, "token": token});
                let _ = self.cmd_tx.send(cmd.to_string() + "\n");
                room.session = DeviceSession::Running;
                true
            }
            None => {
                room.session = DeviceSession::Idle;
                false
            }
        }
    }

    /// Unregisters a socket. The last one ends a preview session at once; a device session
    /// keeps running for `device_grace`, so a reload or tab switch doesn't restart it.
    /// Returns whether the room was removed.
    pub fn leave_room(self: &Arc<Self>, room_id: &str, format: FrameFormat) -> bool {
        if is_device_room(room_id) {
            let Some(mut room) = self.ws_clients.get_mut(room_id) else {
                return false;
            };
            remove_viewer(&mut room, format);
            if room.viewers() == 0 {
                let last_join = room.last_join;
                drop(room);
                let state = self.clone();
                let room_id = room_id.to_string();
                tokio::spawn(async move {
                    tokio::time::sleep(state.device_grace).await;
                    if state.close_idle_device_room(&room_id, last_join) {
                        tracing::info!("Device session {} ended after the grace period", room_id);
                    }
                });
            }
            return false;
        }

        self.ws_clients
            .remove_if_mut(room_id, |_, room| {
                remove_viewer(room, format);
                if room.viewers() > 0 {
                    return false;
                }
                let _ = self.cmd_tx.send(command("disconnect", room_id));
                true
            })
            .is_some()
    }

    fn close_idle_device_room(&self, room_id: &str, last_join: u64) -> bool {
        self.ws_clients
            .remove_if(room_id, |_, room| {
                if room.viewers() > 0 || room.last_join != last_join {
                    return false;
                }
                if room.session != DeviceSession::Idle {
                    let cmd = json!({"event": "device_disconnect", "session_id": room_id});
                    let _ = self.cmd_tx.send(cmd.to_string() + "\n");
                }
                true
            })
            .is_some()
    }

    /// A new preview service has no sessions: drop empty device rooms and let the next
    /// viewer of the others start a fresh session.
    pub fn reset_device_sessions(&self) {
        self.ws_clients.retain(|room_id, room| {
            if !is_device_room(room_id) {
                return true;
            }
            room.session = DeviceSession::Idle;
            room.viewers() > 0
        });
    }

    fn device_room_count(&self) -> usize {
        self.ws_clients
            .iter()
            .filter(|entry| is_device_room(entry.key()))
            .count()
    }
}

fn remove_viewer(room: &mut Room, format: FrameFormat) {
    match format {
        FrameFormat::Png => room.png_viewers -= 1,
        FrameFormat::Raw => room.raw_viewers -= 1,
    }
}

fn command(event: &str, user_id: &str) -> String {
    json!({"event": event, "user_id": user_id}).to_string() + "\n"
}

#[cfg(test)]
pub fn test_state() -> (Arc<AppState>, broadcast::Receiver<String>) {
    let (cmd_tx, cmd_rx) = broadcast::channel(16);
    let state = AppState {
        ws_clients: DashMap::new(),
        cmd_tx,
        backend_url: String::new(),
        http: reqwest::Client::new(),
        tcp_secret: None,
        resync_tx: watch::channel(0).0,
        max_device_rooms: 2,
        device_grace: Duration::from_millis(50),
    };
    (Arc::new(state), cmd_rx)
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEV: &str = "dev:64b7f0c2a1b2c3d4e5f60718";

    fn next_command(rx: &mut broadcast::Receiver<String>) -> Option<serde_json::Value> {
        let msg = rx.try_recv().ok()?;
        Some(serde_json::from_str(&msg).unwrap())
    }

    fn next_event(rx: &mut broadcast::Receiver<String>) -> Option<String> {
        next_command(rx).map(|value| value["event"].as_str().unwrap().to_string())
    }

    #[test]
    fn first_join_and_last_leave_send_commands_once() {
        let (state, mut rx) = test_state();

        state.join_room("u1", FrameFormat::Png).unwrap();
        state.join_room("u1", FrameFormat::Raw).unwrap();
        assert_eq!(next_event(&mut rx).as_deref(), Some("connect"));
        assert_eq!(next_event(&mut rx), None);

        assert!(!state.leave_room("u1", FrameFormat::Png));
        assert_eq!(next_event(&mut rx), None);
        assert!(state.leave_room("u1", FrameFormat::Raw));
        assert_eq!(next_event(&mut rx).as_deref(), Some("disconnect"));
        assert!(state.ws_clients.is_empty());
    }

    #[test]
    fn rejoin_after_last_leave_reconnects() {
        let (state, mut rx) = test_state();

        state.join_room("u1", FrameFormat::Png).unwrap();
        state.leave_room("u1", FrameFormat::Png);
        state.join_room("u1", FrameFormat::Png).unwrap();

        assert_eq!(next_event(&mut rx).as_deref(), Some("connect"));
        assert_eq!(next_event(&mut rx).as_deref(), Some("disconnect"));
        assert_eq!(next_event(&mut rx).as_deref(), Some("connect"));
        assert_eq!(state.ws_clients.get("u1").unwrap().viewers(), 1);
    }

    #[test]
    fn leave_without_room_is_noop() {
        let (state, mut rx) = test_state();
        assert!(!state.leave_room("u1", FrameFormat::Png));
        assert_eq!(next_event(&mut rx), None);
    }

    #[test]
    fn join_returns_cached_frame() {
        let (state, _rx) = test_state();
        state.join_room("u1", FrameFormat::Png).unwrap();
        state.ws_clients.get_mut("u1").unwrap().last_frame = Some(Bytes::from_static(b"frame"));
        let joined = state.join_room("u1", FrameFormat::Raw).unwrap();
        assert_eq!(joined.cached_frame.as_deref(), Some(&b"frame"[..]));
    }

    #[test]
    fn only_the_first_device_viewer_starts_the_session() {
        let (state, mut rx) = test_state();

        let first = state.join_room(DEV, FrameFormat::Raw).unwrap();
        let second = state.join_room(DEV, FrameFormat::Png).unwrap();
        assert!(first.start_ticket.is_some());
        assert!(second.start_ticket.is_none());
        assert_eq!(next_event(&mut rx), None);

        assert!(state.finish_device_start(DEV, first.start_ticket.unwrap(), Some("dev_token")));
        let cmd = next_command(&mut rx).unwrap();
        assert_eq!(cmd["event"], "device_connect");
        assert_eq!(cmd["session_id"], DEV);
        assert_eq!(cmd["token"], "dev_token");
    }

    #[test]
    fn a_failed_start_lets_the_next_viewer_try_again() {
        let (state, mut rx) = test_state();

        let first = state.join_room(DEV, FrameFormat::Raw).unwrap();
        assert!(!state.finish_device_start(DEV, first.start_ticket.unwrap(), None));
        assert_eq!(next_event(&mut rx), None);

        let second = state.join_room(DEV, FrameFormat::Raw).unwrap();
        assert!(second.start_ticket.is_some());
    }

    #[tokio::test]
    async fn device_session_ends_after_the_grace_period() {
        let (state, mut rx) = test_state();
        let joined = state.join_room(DEV, FrameFormat::Raw).unwrap();
        state.finish_device_start(DEV, joined.start_ticket.unwrap(), Some("t"));
        next_event(&mut rx);

        assert!(!state.leave_room(DEV, FrameFormat::Raw));
        assert!(state.ws_clients.contains_key(DEV));
        assert_eq!(next_event(&mut rx), None);

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(!state.ws_clients.contains_key(DEV));
        assert_eq!(next_event(&mut rx).as_deref(), Some("device_disconnect"));
    }

    #[tokio::test]
    async fn rejoining_within_the_grace_period_keeps_the_session() {
        let (state, mut rx) = test_state();
        let joined = state.join_room(DEV, FrameFormat::Raw).unwrap();
        state.finish_device_start(DEV, joined.start_ticket.unwrap(), Some("t"));
        next_event(&mut rx);

        state.leave_room(DEV, FrameFormat::Raw);
        let rejoined = state.join_room(DEV, FrameFormat::Raw).unwrap();
        assert!(rejoined.start_ticket.is_none());

        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(state.ws_clients.contains_key(DEV));
        assert_eq!(next_event(&mut rx), None);
    }

    #[tokio::test]
    async fn a_start_that_finishes_after_the_room_closed_is_dropped() {
        let (state, mut rx) = test_state();
        let joined = state.join_room(DEV, FrameFormat::Raw).unwrap();

        state.leave_room(DEV, FrameFormat::Raw);
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert_eq!(next_event(&mut rx).as_deref(), Some("device_disconnect"));

        assert!(!state.finish_device_start(DEV, joined.start_ticket.unwrap(), Some("t")));
        assert_eq!(next_event(&mut rx), None);
    }

    #[test]
    fn new_device_rooms_beyond_the_limit_are_busy() {
        let (state, _rx) = test_state();

        state.join_room("dev:a", FrameFormat::Raw).unwrap();
        state.join_room("dev:b", FrameFormat::Raw).unwrap();
        assert!(state.join_room("dev:c", FrameFormat::Raw).is_err());
        assert!(state.join_room("dev:a", FrameFormat::Raw).is_ok());
        assert!(state.join_room("u1", FrameFormat::Png).is_ok());
    }

    #[test]
    fn reset_restarts_device_sessions_and_drops_empty_rooms() {
        let (state, _rx) = test_state();
        let active = state.join_room("dev:a", FrameFormat::Raw).unwrap();
        state.finish_device_start("dev:a", active.start_ticket.unwrap(), Some("t"));
        let idle = state.join_room("dev:b", FrameFormat::Raw).unwrap();
        state.finish_device_start("dev:b", idle.start_ticket.unwrap(), Some("t"));
        state.ws_clients.get_mut("dev:b").unwrap().raw_viewers = 0;
        state.join_room("u1", FrameFormat::Png).unwrap();

        state.reset_device_sessions();

        assert!(!state.ws_clients.contains_key("dev:b"));
        assert!(state.ws_clients.contains_key("u1"));
        let next = state.join_room("dev:a", FrameFormat::Raw).unwrap();
        assert!(next.start_ticket.is_some());
    }
}
