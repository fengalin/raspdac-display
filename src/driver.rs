//! Hand-rolled HD44780 4-bit mode driver using rpi-pal.

use rpi_pal::gpio::{Gpio, Level, OutputPin};
use std::time::Duration;
use tokio::time::sleep;

use crate::config::DisplayConfig;

/// Number of display cells per row.
pub const WIDTH: usize = 16;

#[derive(Debug, thiserror::Error)]
pub enum DriverError {
    #[error("failed to open GPIO: {0}")]
    Gpio(#[from] rpi_pal::gpio::Error),
}

/// HD44780 commands.
mod cmd {
    pub(super) const CLEAR: u8 = 0x01;
    pub(super) const ENTRY_MODE_SET: u8 = 0x04; // exec time 37us
    pub(super) const DISPLAY_OFF: u8 = 0x08; // exec time 37us
    pub(super) const DISPLAY_ON: u8 = 0x0C; // Display on, cursor off, blink off, exec time 37us
    pub(super) const SET_DDRAM_ADDR: u8 = 0x80; // exec time 37us
    pub(super) const FUNCTION_SET: u8 = 0x20; // exec time 37us
}

mod args {
    pub(super) const ENTRY_MODE_INCREMENT: u8 = 0x02; // Increment (no shift)
    pub(super) const FUNCTION_SET_4_BITS: u8 = 0x00; // DL
    pub(super) const FUNCTION_SET_2_LINES: u8 = 0x08; // N
    pub(super) const FUNCTION_SET_5X8_FONT: u8 = 0x00; // F
    // https://www.winstar.com.tw/built-in-font-library-ws0010.html
    pub(super) const FUNCTION_SET_FONT_BANK_EU1: u8 = 0x01;
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum LineNb {
    One,
    Two,
}

impl LineNb {
    fn get_addr(self) -> u8 {
        match self {
            LineNb::One => 0,
            LineNb::Two => 64,
        }
    }
}

/// HD44780 driver for 4-bit parallel mode.
#[derive(Debug)]
pub struct Hd44780 {
    rs: OutputPin,
    en: OutputPin,
    d4: OutputPin,
    d5: OutputPin,
    d6: OutputPin,
    d7: OutputPin,
    last_len_line1: u8,
    last_len_line2: u8,
}

impl Hd44780 {
    /// Initialize the HD44780 display with the given pin numbers (BCM GPIO).
    pub async fn new(config: &DisplayConfig) -> Result<Self, DriverError> {
        let gpio = Gpio::new()?;

        let mut display = Hd44780 {
            rs: gpio.get(config.rs)?.into_output(),
            en: gpio.get(config.en)?.into_output(),
            d4: gpio.get(config.d4)?.into_output(),
            d5: gpio.get(config.d5)?.into_output(),
            d6: gpio.get(config.d6)?.into_output(),
            d7: gpio.get(config.d7)?.into_output(),
            last_len_line1: 0,
            last_len_line2: 0,
        };
        display.init().await?;
        Ok(display)
    }

    /// Full HD44780 initialization sequence (4-bit mode).
    async fn init(&mut self) -> Result<(), DriverError> {
        self.rs.set_low();

        // placing display in 4 bit mode
        self.send_nibble(0x02).await;
        sleep(Duration::from_millis(1)).await;

        self.command(
            cmd::FUNCTION_SET
                | args::FUNCTION_SET_4_BITS
                | args::FUNCTION_SET_2_LINES
                | args::FUNCTION_SET_5X8_FONT
                | args::FUNCTION_SET_FONT_BANK_EU1,
        )
        .await;

        self.off().await;

        // (no shift)
        self.command(cmd::ENTRY_MODE_SET | args::ENTRY_MODE_INCREMENT)
            .await;

        self.clear_on().await;

        Ok(())
    }

    /// Set the cursor position (0-15 for first row, 64-79 for second row).
    async fn set_cursor(&mut self, pos: u8) {
        self.command(cmd::SET_DDRAM_ADDR | pos).await;
    }

    /// Clear the display.
    pub async fn clear(&mut self) {
        self.command(cmd::CLEAR).await;
        self.last_len_line1 = 0;
        self.last_len_line2 = 0;
        // winstar OLEDs require 6.2ms for this command, according to spec
        sleep(Duration::from_micros(6_200)).await;
    }

    pub async fn clear_on(&mut self) {
        self.clear().await;
        self.on().await;
    }

    pub async fn on(&mut self) {
        self.command(cmd::DISPLAY_ON).await;
    }

    pub async fn off(&mut self) {
        self.command(cmd::DISPLAY_OFF).await;
    }

    /// Write to a specific line.
    pub async fn write_line(&mut self, line: LineNb, data: impl Iterator<Item = char>) {
        self.set_cursor(line.get_addr()).await;

        self.rs.set_high();

        let mut cur_len = 0u8;
        for byte in data.map(map_char).take(WIDTH) {
            self.send_nibble((byte >> 4) & 0x0F).await;
            sleep(Duration::from_micros(1)).await;
            self.send_nibble(byte & 0x0F).await;
            cur_len += 1;
            sleep(Duration::from_micros(40)).await;
        }

        // overide remaining characters from previous line, if any
        let last_len = match line {
            LineNb::One => self.last_len_line1,
            LineNb::Two => self.last_len_line2,
        };
        if let Some(delta) = last_len.checked_sub(cur_len)
            && delta > 0
        {
            for _ in 0..delta {
                self.send_nibble((b' ' >> 4) & 0x0F).await;
                sleep(Duration::from_micros(1)).await;
                self.send_nibble(b' ' & 0x0F).await;
                sleep(Duration::from_micros(40)).await;
            }
        }

        match line {
            LineNb::One => self.last_len_line1 = cur_len,
            LineNb::Two => self.last_len_line2 = cur_len,
        }
    }

    /// Send a full command byte (two 4-bit nibbles).
    async fn command(&mut self, data: u8) {
        self.rs.set_low();
        self.send_nibble((data >> 4) & 0x0F).await;
        sleep(Duration::from_micros(1)).await;
        self.send_nibble(data & 0x0F).await;
        sleep(Duration::from_micros(40)).await;
    }

    async fn send_nibble(&mut self, nibble: u8) {
        // Pulse the enable pin: HIGH → write → LOW
        self.en.set_high();

        self.d4.write(if (nibble & 0x01) != 0 {
            Level::High
        } else {
            Level::Low
        });
        self.d5.write(if (nibble & 0x02) != 0 {
            Level::High
        } else {
            Level::Low
        });
        self.d6.write(if (nibble & 0x04) != 0 {
            Level::High
        } else {
            Level::Low
        });
        self.d7.write(if (nibble & 0x08) != 0 {
            Level::High
        } else {
            Level::Low
        });

        sleep(Duration::from_micros(1)).await;
        self.en.set_low();
    }
}

/// Maps UTF-8 char to English-European font table 1 (FT[1:0]=01)
/// https://www.winstar.com.tw/built-in-font-library-ws0010.html
fn map_char(c: char) -> u8 {
    match c {
        '\\' => 0xca,
        ' '..'}' => c as u8, // matching ascii chars except for '\\' which maps to the yen symbol
        '→' => 0x7e,
        '←' => 0x7f,
        'Û' => 0x80,
        'Ù' => 0x81,
        'Ú' => 0x82,
        'Ü' => 0x83,
        'û' => 0x84,
        'ù' => 0x85,
        'ú' => 0x86,
        'Ô' => 0x87,
        'Ò' => 0x88,
        'Ó' => 0x89,
        // 0x9a is underlined O
        'ô' => 0x8b,
        'ò' => 0x8c,
        'ó' => 0x8d,
        'ö' => 0x8e,
        '¿' => 0x8f,
        'Ê' => 0x90,
        'È' => 0x91,
        'É' => 0x92,
        'Ë' => 0x93,
        'ê' => 0x94,
        'è' => 0x95,
        'é' => 0x96,
        'ë' => 0x97,
        'Å' => 0x98,
        'Ä' => 0x99,
        'å' => 0x9a,
        'â' => 0x9b,
        'à' => 0x9c,
        'á' => 0x9d,
        'ä' => 0x9e,
        // 0x9f is underlined A
        // 0xa0 is underlined a
        'î' => 0xa1,
        'ì' => 0xa2,
        'í' => 0xa3,
        'ï' => 0xa4,
        '¡' => 0xa5,
        'Ñ' => 0xa6,
        'ñ' => 0xa7,
        // 0xa8..=0xae => don't know their UTF-8
        'Æ' => 0xaf,
        '§' => 0xb0,
        '±' => 0xb1,
        // 0xb2 ??
        '↑' => 0xb3,
        '↓' => 0xb4,
        '↵' => 0xb5,
        'ƒ' => 0xb6,
        '£' => 0xb7,
        '⇥' => 0xb8,
        '⤈' => 0xb9,
        '⤉' => 0xba,
        '⤓' => 0xbb,
        '¶' => 0xbc,
        // 0xbd 1/2 exponent
        // 0xbe 1/3 exponent
        // 0xbf 1/4 exponent
        // 0xc0 ??
        'Ŀ' => 0xc1,
        'Đ' => 0xc2,
        'ß' => 0xc3,
        'ç' => 0xc4,
        // 0xc5 ??
        '¤' => 0xc6,
        '⛶' => 0xc7,
        'µ' => 0xc8,
        'ø' => 0xc9,
        'œ' => 0xc9, // no œ in the font => use ø
        'ÿ' => 0xca,
        'Ã' => 0xcb,
        '¢' => 0xcc,
        'ã' => 0xcd,
        'Õ' => 0xce,
        'õ' => 0xcf,
        '˙' => 0xd0,
        // 0xd1 => double dot above (non printable alone)
        '˚' => 0xd2,
        // 0xd3 => combining reversed coma above (non printable alone)
        // 0xd4 => combining turned coma above (non printable alone)
        // '~' => 0xd5,
        '÷' => 0xd6,
        '«' => 0xd7,
        '»' => 0xd8,
        'ŀ' => 0xd9,
        // '\\' => 0xa,
        '×' => 0xdb,
        '®' => 0xdc,
        '©' => 0xdd,
        // 0xde T on a black square
        '≡' => 0xdf,
        'α' => 0xe0,
        '⅓' => 0xe1,
        '½' => 0xe2,
        '¼' => 0xe3,
        '⅔' => 0xe4,
        '¾' => 0xe5,
        // TODO the last ones...
        '\0'..' ' => b' ',
        _ => b'?',
    }
}
