pub mod app_state;

pub use app_state::{is_device_room, AppState, FrameFormat, ViewerRx, DEVICE_ROOM_PREFIX};
#[cfg(test)]
pub use app_state::{test_state, test_state_with};
