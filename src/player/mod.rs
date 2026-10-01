//! An aggregator task keeps per-player state, picks the active player,
//! and sends the resulting `DisplayState` to the display thread.

use std::sync::Arc;
use std::time::Instant;

mod aggregator;
pub use aggregator::PlayerAggregator;

mod mpd;
pub use mpd::MpdPlayer;

mod mpris;
pub use mpris::MprisPlayer;

pub const DEFAULT_TITLE: &str = "";

/// Player state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaybackState {
    Playing,
    Paused,
    Stopped,
}

impl PlaybackState {
    pub fn is_playing(self) -> bool {
        matches!(self, PlaybackState::Playing)
    }
    pub fn is_paused(self) -> bool {
        matches!(self, PlaybackState::Paused)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PlayerData {
    pub title: Arc<str>,
    pub base_position_us: u64,
    pub base_position_instant: Instant,
    pub duration_us: u64,
    pub rate: f64,
}

impl PlayerData {
    pub fn clear(&mut self) {
        *self = Default::default();
    }
}

impl Default for PlayerData {
    fn default() -> Self {
        PlayerData {
            title: DEFAULT_TITLE.into(),
            base_position_us: 0,
            base_position_instant: Instant::now(),
            duration_us: 0,
            rate: 1.0,
        }
    }
}

#[derive(Debug)]
pub enum PlayerNotification {
    Update {
        state: PlaybackState,
        data: PlayerData,
    },
    BasePosition {
        position_us: u64,
        instant: Instant,
    },
}

#[derive(Debug)]
pub struct NamedPlayerNotification {
    pub player_name: &'static str,
    pub notif: PlayerNotification,
}
