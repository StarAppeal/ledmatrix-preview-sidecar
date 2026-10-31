pub mod app_state;

#[cfg(test)]
pub use app_state::test_state;
pub use app_state::{is_device_room, AppState, FrameFormat, ViewerRx, DEVICE_ROOM_PREFIX};
