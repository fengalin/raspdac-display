//! Display thread: owns the HD44780 driver, runs the animation loop,
//! processes commands from the mpsc channel, and manages the idle timer.
//!
//! Layout:
//! - Line 1: state symbol (> playing, || paused) + position / duration.
//! - Line 2: track title, ping-pong scrolling when it exceeds 16 cells.
//! - No active player: line 1 shows the clock (`HH:MM`), line 2 blank.
//! - Idle timer: after the configured timeout in the paused state the
//!   display is blanked until the next activity.

use std::fmt::Write;
use std::ops::ControlFlow;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::time::sleep;
use tracing::{debug, info};

use crate::config::Config;
use crate::driver::{DriverError, Hd44780, LineNb, WIDTH};
use crate::mpris::{PlaybackState, PlayerData};
use crate::scroll::ScrollState;

const PLAYING_TICK: Duration = Duration::from_millis(750);

/// Commands sent from the async runtime to the display thread.
#[derive(Debug)]
pub enum DisplayCmd {
    /// Update display state with new content.
    Update {
        state: PlaybackState,
        data: PlayerData,
    },
    /// Update base position.
    BasePosition { position_us: u64, instant: Instant },
    /// Clear the display immediately.
    Stop,
}

/// Display state as reported by the MPRIS aggregator.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DisplayState {
    /// Display is off.
    Off,
    /// A track is playing.
    Playing,
    /// A track is loaded but not playing (paused, or stopped with a track).
    Paused,
}

impl DisplayState {
    fn is_off(self) -> bool {
        matches!(self, DisplayState::Off)
    }
    fn is_paused(self) -> bool {
        matches!(self, DisplayState::Paused)
    }
    fn is_playing(self) -> bool {
        matches!(self, DisplayState::Playing)
    }
}

#[derive(Debug)]
pub struct Display {
    oled: Hd44780,
    data: PlayerData,
    state: DisplayState,
    position_str: String,
    duration_str: String,
    update_line1: bool,
    update_line2: bool,
    line2: RowDisplay,
    tick_period: Duration,
    idle_timeout: Duration,
    last_tick: Instant,
}

impl Display {
    pub async fn new(config: &Config) -> Result<Self, DriverError> {
        Ok(Display {
            oled: Hd44780::new(&config.display).await?,
            data: Default::default(),
            state: DisplayState::Off,
            position_str: String::with_capacity(5),
            duration_str: String::with_capacity(5),
            update_line1: false,
            update_line2: false,
            line2: RowDisplay::new(config.scroll.speed, config.scroll.dwell_secs),
            tick_period: Duration::from_secs(1),
            idle_timeout: config.idle.timeout,
            last_tick: Instant::now(),
        })
    }

    async fn on_command(&mut self, cmd: DisplayCmd) -> ControlFlow<()> {
        match cmd {
            DisplayCmd::Update { state, data } => {
                if self.is_off() {
                    if state == PlaybackState::Stopped {
                        // unchanged
                        return ControlFlow::Continue(());
                    }

                    self.oled.clear_on().await;
                }

                match state {
                    PlaybackState::Playing => {
                        self.update(state, data).await;
                        self.tick_period = PLAYING_TICK;
                    }
                    PlaybackState::Paused => {
                        // only got to paused if we were playing
                        self.update(state, data).await;

                        self.tick_period = self.idle_timeout;
                    }
                    PlaybackState::Stopped => {
                        self.update(state, data).await;
                        self.off().await;
                    }
                }
            }
            DisplayCmd::BasePosition {
                position_us,
                instant,
            } => {
                debug!(base_pos_us = %position_us, "got base position update");
                self.data.base_position_us = position_us;
                self.data.base_position_instant = instant;
                self.update_line1 = true;
            }
            DisplayCmd::Stop => {
                info!("got stop command");
                return ControlFlow::Break(());
            }
        }

        ControlFlow::Continue(())
    }

    async fn on_tick(&mut self) {
        match self.state {
            DisplayState::Playing => {
                let now = Instant::now();
                // let dt = (now - self.last_tick).as_secs_f32();
                self.last_tick = now;
                // FIXME switch update_line2 depending on animation
                self.update_line1 = true;
                self.update_oled(now).await;
            }
            DisplayState::Paused => {
                info!("going blank due to inactivity");
                self.off().await;
            }
            DisplayState::Off => (),
        }
    }

    pub async fn into_task(mut self, mut cmd_rx: mpsc::Receiver<DisplayCmd>) {
        loop {
            if !self.is_off() {
                tokio::select! {
                    biased;
                    Some(cmd) = cmd_rx.recv() => {
                        if self.on_command(cmd).await.is_break() {
                            break;
                        }
                    }
                    _ = sleep(self.tick_period) => {
                        self.on_tick().await;
                    }
                    else => {
                        info!("command chan terminated");
                        break;
                    }
                }
            } else if let Some(cmd) = cmd_rx.recv().await {
                // off => only wait for commands
                if self.on_command(cmd).await.is_break() {
                    break;
                }
            } else {
                info!("command chan terminated");
                break;
            }
        }

        self.quit().await;
    }

    fn is_off(&self) -> bool {
        self.state.is_off()
    }

    async fn off(&mut self) {
        self.oled.off().await;
        self.state = DisplayState::Off;
        self.data.clear();
    }

    async fn quit(&mut self) {
        info!("quitting display task");
        self.oled.off().await;
    }

    /// Update internal state data
    ///
    /// Returns true if something has changed
    async fn update(&mut self, state: PlaybackState, data: PlayerData) {
        match state {
            PlaybackState::Playing if !self.state.is_playing() => {
                debug!(old = ?self.state, new = ?state, "state changed");
                self.state = DisplayState::Playing;
                self.update_line1 = true;
            }
            PlaybackState::Paused if !self.state.is_paused() => {
                debug!(old = ?self.state, new = ?state, "state changed");
                self.state = DisplayState::Paused;
                self.update_line1 = true;
            }
            PlaybackState::Stopped => {
                if !self.state.is_off() {
                    debug!(old = ?self.state, new = ?state, "state changed");
                    self.state = DisplayState::Off;
                    self.update_line1 = false;
                    self.update_line2 = false;
                }
                return;
            }
            _ => (),
        }

        let mut data_updated = self.data.base_position_us != data.base_position_us
            || self.data.base_position_instant != data.base_position_instant
            || self.data.duration_us != data.duration_us;

        self.update_line1 |= data_updated;

        if self.data.title != data.title {
            self.line2.set_title(data.title.as_ref());
            self.update_line2 |= true;
            data_updated = true;
        }

        if data_updated {
            debug!(old = ?self.data, new = ?data, "data updated");
            self.data = data;
        }

        self.update_oled(Instant::now()).await;
    }

    async fn update_oled(&mut self, now: Instant) {
        fn format_time(time_str: &mut String, usecs: u64) {
            let total_secs = (usecs + 500_000) / 1_000_000;
            let minutes = total_secs / 60;
            let seconds = total_secs % 60;
            time_str.clear();
            time_str
                .write_fmt(format_args!("{:02}:{:02}", minutes, seconds))
                .unwrap();
        }

        let Self {
            oled,
            data,
            state,
            position_str,
            duration_str,
            line2,
            ..
        } = self;

        if self.update_line1 {
            self.update_line1 = false;
            format_time(
                position_str,
                data.base_position_us
                    + now.duration_since(data.base_position_instant).as_micros() as u64,
            );
            // might want to skip if duration did not change
            format_time(duration_str, data.duration_us);

            use std::iter::once;
            oled.write_line(
                LineNb::One,
                once('|')
                    .chain(once(if state.is_playing() { '>' } else { '|' }))
                    .chain(once(' '))
                    .chain(position_str.chars())
                    .chain(" / ".chars())
                    .chain(duration_str.chars()),
            )
            .await;
        }

        if !self.update_line2 {
            return;
        }
        self.update_line2 = false;

        oled.write_line(LineNb::Two, line2.full_text.chars()).await;
    }
}

/// A single displayable row with its scroll state.
#[derive(Debug, PartialEq)]
struct RowDisplay {
    full_text: String,
    scroll: Option<ScrollState>,
}

impl RowDisplay {
    /// Sanitize and prepare a row. Empty text renders as blank.
    fn new(speed: f32, dwell_secs: f32) -> Self {
        // let mut sanitized = sanitize(text, WIDTH * 3).into_bytes();
        // let scroll = if sanitized.len() > WIDTH {
        // FIXME
        // Some(ScrollState::new(
        //     sanitized.clone(),
        //     WIDTH,
        //     speed,
        //     dwell_secs,
        // ))
        //     None
        // } else {
        //     None
        // };
        RowDisplay {
            full_text: String::with_capacity(WIDTH * 2),
            scroll: None,
        }
    }

    fn tick(&mut self, dt: f32) {
        if let Some(ref mut scroll) = self.scroll {
            scroll.tick(dt);
        }
    }

    // FIXME and call from playing mode
    // async fn render(&mut self, oled: &mut Hd44780, line_pos: u8) {
    //     let content: Vec<u8> = if let Some(ref scroll) = self.scroll {
    //         scroll.visible().to_vec()
    //     } else {
    //         self.sanitized.clone()
    //     };
    //     oled.write_row(line_pos, &content).await;
    // }

    fn set_title(&mut self, title: &str) {
        self.full_text.clear();
        self.full_text
            .push_str(&title[..usize::min(title.len(), 1 + WIDTH * 2)]);
    }
}
