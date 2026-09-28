//! Configuration for the raspdac-display service.

use std::time::Duration;

/// Top-level configuration.
#[derive(Debug, Clone, Default)]
pub struct Config {
    pub display: DisplayConfig,
    pub mpris: MprisConfig,
    pub scroll: ScrollConfig,
    pub idle: IdleConfig,
}

/// GPIO pin configuration for the HD44780 display.
#[derive(Debug, Clone)]
pub struct DisplayConfig {
    pub rs: u8,
    pub en: u8,
    pub d4: u8,
    pub d5: u8,
    pub d6: u8,
    pub d7: u8,
}

impl Default for DisplayConfig {
    fn default() -> Self {
        DisplayConfig {
            rs: 7,
            en: 8,
            d4: 25,
            d5: 24,
            d6: 23,
            d7: 27,
        }
    }
}

/// MPRIS bus name configuration.
#[derive(Debug, Clone)]
pub struct MprisConfig {
    pub mpd_bus: &'static str,
    pub pibuz_bus: &'static str,
}

impl Default for MprisConfig {
    fn default() -> Self {
        MprisConfig {
            mpd_bus: "org.mpris.MediaPlayer2.mpd",
            pibuz_bus: "org.mpris.MediaPlayer2.pibuz",
        }
    }
}

/// Scrolling configuration.
#[derive(Debug, Clone)]
pub struct ScrollConfig {
    /// Scroll speed in cells per second (applied to both artist and title).
    pub speed: f32,
    /// Seconds to pause at each edge before reversing direction.
    pub dwell_secs: f32,
}

impl Default for ScrollConfig {
    fn default() -> Self {
        ScrollConfig {
            speed: 2.0,
            dwell_secs: 1.5,
        }
    }
}

/// Idle timer configuration.
#[derive(Debug, Clone)]
pub struct IdleConfig {
    /// Seconds before clearing the display when no player is active.
    pub timeout: Duration,
}

impl Default for IdleConfig {
    fn default() -> Self {
        IdleConfig {
            timeout: Duration::from_secs(300),
        }
    }
}
