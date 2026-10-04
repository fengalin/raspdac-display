//! Configuration for the raspdac-display service.

/// Top-level configuration.
#[derive(Debug, Clone, Default)]
pub struct Config {
    pub display: DisplayConfig,
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
