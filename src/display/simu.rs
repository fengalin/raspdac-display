//! HD44780 simulator.

use tracing::info;

use crate::config::DisplayConfig;

#[path = "driver.rs"]
#[allow(unused)]
mod driver;
pub use driver::{DISPLAY_WIDTH, DriverError, LineNb};

/// HD44780 driver for 4-bit parallel mode.
#[derive(Debug)]
pub struct Hd44780 {
    last_len_line1: u8,
    last_len_line2: u8,
}

impl Hd44780 {
    pub async fn new(_config: &DisplayConfig) -> Result<Self, DriverError> {
        Ok(Hd44780 {
            last_len_line1: 0,
            last_len_line2: 0,
        })
    }

    pub async fn clear(&mut self) {
        info!("clear");
    }

    pub async fn clear_on(&mut self) {
        info!("clear on");
    }

    pub async fn on(&mut self) {
        info!("on");
    }

    pub async fn off(&mut self) {
        info!("off");
    }

    /// Write to a specific line.
    pub async fn write_line(&mut self, line: LineNb, data: impl Iterator<Item = char>) {
        let (line, last_len) = match line {
            LineNb::One => (1, self.last_len_line1),
            LineNb::Two => (2, self.last_len_line2),
        };

        let text = data.take(DISPLAY_WIDTH).collect::<String>();
        let overide = last_len.checked_sub(text.len() as u8);

        info!(%line, %text, ?overide);
    }
}
