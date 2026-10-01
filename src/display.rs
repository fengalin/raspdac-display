//! Display thread: owns the HD44780 driver, runs the animation loop,
//! processes commands from the mpsc channel, and manages the idle timer.
//!
//! Layout for player modes:
//!
//! - Line 1: state symbol (> playing, || paused) + position / duration.
//! - Line 2: track title, ping-pong scrolling when it exceeds 16 cells.
//! - No active player: line 1 shows the clock (`HH:MM`), line 2 blank.
//! - Idle timer: after the configured timeout in the paused state the
//!   display is blanked until the next activity.

use std::fmt::Write;
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, mpsc};
use tokio::time::sleep;
use tracing::{debug, info};

use crate::charge_point_notif::ChargeState;
use crate::config::Config;
use crate::scroll::ScrollState;
use crate::{
    DISPLAY_WIDTH, DriverError, Hd44780, LineNb, PlaybackState, PlayerData, PlayerNotification,
};

const PLAYING_TICK: Duration = Duration::from_secs(1);

/// Display Commands.
#[derive(Debug)]
pub enum DisplayCmd {
    Player(PlayerNotification),
    ChargePoint(ChargeState),
}

/// Display state as reported by the MPRIS aggregator or Charge Point listener.
#[derive(Debug, Clone, Copy, PartialEq)]
enum DisplayState {
    /// Display is off.
    Off,
    /// Displaying Player data.
    Player,
    /// Displaying Charge Point data.
    ChargePoint,
}

impl DisplayState {
    fn is_off(self) -> bool {
        matches!(self, DisplayState::Off)
    }
    fn is_player(self) -> bool {
        matches!(self, DisplayState::Player)
    }
    fn is_charge_point(self) -> bool {
        matches!(self, DisplayState::ChargePoint)
    }
}

#[derive(Debug)]
pub struct Display {
    cmd_rx: mpsc::Receiver<DisplayCmd>,
    oled: Hd44780,
    state: DisplayState,
    position_str: String,
    duration_str: String,
    update_line1: bool,
    update_line2: bool,
    line2: RowDisplay,
    tick_period: Option<Duration>,
    idle_timeout: Duration,
    last_tick: Instant,
    player_state: PlaybackState,
    player_data: PlayerData,
    charge_state: ChargeState,
}

impl Display {
    pub async fn new(
        config: &Config,
        cmd_rx: mpsc::Receiver<DisplayCmd>,
    ) -> Result<Self, DriverError> {
        Ok(Display {
            cmd_rx,
            oled: Hd44780::new(&config.display).await?,
            state: DisplayState::Off,
            position_str: String::with_capacity(5),
            duration_str: String::with_capacity(5),
            update_line1: false,
            update_line2: false,
            line2: RowDisplay::new(config.scroll.speed, config.scroll.dwell_secs),
            tick_period: None,
            idle_timeout: config.idle.timeout,
            last_tick: Instant::now(),
            player_state: PlaybackState::Stopped,
            player_data: Default::default(),
            charge_state: ChargeState::Available,
        })
    }

    async fn on_command(&mut self, cmd: DisplayCmd) {
        match cmd {
            DisplayCmd::Player(PlayerNotification::Update {
                state: player_state,
                data,
            }) => {
                match self.state {
                    DisplayState::Player => (),
                    DisplayState::ChargePoint => {
                        if !self.charge_state.is_charging()
                            && !self.charge_state.is_error()
                            && player_state.is_playing()
                            && !self.player_state.is_playing()
                        {
                            info!(
                                charge_state = %self.charge_state.name(),
                                ?player_state, data = ?data,
                                "switching to player mode",
                            );

                            self.state = DisplayState::Player;
                            self.update_line1 = true;
                            self.update_line2 = true;
                        } else {
                            debug!(
                                ?player_state, data = ?data,
                                charge_state = %self.charge_state.name(),
                                "updating (not displaying)",
                            );

                            self.player_state = player_state;
                            self.player_data = data;
                            return;
                        }
                    }
                    DisplayState::Off => {
                        if player_state == PlaybackState::Stopped {
                            // unchanged
                            return;
                        }

                        self.oled.clear_on().await;
                    }
                }

                match player_state {
                    PlaybackState::Playing => {
                        self.update_player(player_state, data).await;
                        self.tick_period = Some(PLAYING_TICK);
                    }
                    PlaybackState::Paused => {
                        // only got to paused if we were playing
                        self.update_player(player_state, data).await;

                        self.tick_period = Some(self.idle_timeout);
                    }
                    PlaybackState::Stopped => {
                        self.update_player(player_state, data).await;
                        self.off().await;
                    }
                }
            }
            DisplayCmd::Player(PlayerNotification::BasePosition {
                position_us,
                instant,
            }) => {
                debug!(base_pos_us = %position_us, state = ?self.state, "update");
                self.player_data.base_position_us = position_us;
                self.player_data.base_position_instant = instant;
                self.update_line1 = self.state.is_player();
            }
            DisplayCmd::ChargePoint(charge_state) => self.update_charge_point(charge_state).await,
        }
    }

    /// Update internal state data
    ///
    /// Returns true if something has changed
    async fn update_player(&mut self, player_state: PlaybackState, data: PlayerData) {
        assert!(!self.state.is_charge_point());

        // Off or Player mode
        match player_state {
            PlaybackState::Playing
                if !self.state.is_player() || !self.player_state.is_playing() =>
            {
                debug!(old = ?self.player_state, new = ?player_state, "player state changed");
                self.state = DisplayState::Player;
                self.player_state = PlaybackState::Playing;
                self.update_line1 = true;
            }
            PlaybackState::Paused if !self.state.is_player() || !self.player_state.is_paused() => {
                debug!(old = ?self.player_state, new = ?player_state, "player state changed");
                self.state = DisplayState::Player;
                self.player_state = PlaybackState::Paused;
                self.update_line1 = true;
            }
            PlaybackState::Stopped => {
                if !self.state.is_off() {
                    debug!(old = ?self.player_state, new = ?player_state, "player state changed");
                    self.state = DisplayState::Off;
                    self.update_line1 = false;
                    self.update_line2 = false;
                }
                return;
            }
            _ => (),
        }

        let mut data_updated = self.player_data.base_position_us != data.base_position_us
            || self.player_data.base_position_instant != data.base_position_instant
            || self.player_data.duration_us != data.duration_us;

        self.update_line1 |= data_updated;

        if self.player_data.title != data.title {
            self.line2.set_text(data.title.as_ref());
            self.update_line2 |= true;
            data_updated = true;
        }

        if data_updated {
            debug!(old = ?self.player_data, new = ?data, "data updated");
            self.player_data = data;
        }

        self.update_oled_player(Instant::now()).await;
    }

    async fn update_oled_player(&mut self, now: Instant) {
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
            player_data: data,
            player_state,
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

            oled.write_line(
                LineNb::One,
                if player_state.is_playing() {
                    "|> "
                } else {
                    "|| "
                }
                .chars()
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

    /// Update internal state data
    ///
    /// Returns true if something has changed
    async fn update_charge_point(&mut self, charge_state: ChargeState) {
        info!(msg = ?charge_state, "charge point");

        match self.state {
            DisplayState::Off => {
                self.state = DisplayState::ChargePoint;
                self.oled.on().await;
            }
            DisplayState::Player => {
                self.state = DisplayState::ChargePoint;
            }
            DisplayState::ChargePoint => (),
        }

        self.charge_state = charge_state;

        use ChargeState::*;
        match &self.charge_state {
            Charging(progress) | SuspendedEvse(progress) => {
                self.tick_period = None;
                self.oled
                    .write_line(
                        LineNb::One,
                        if self.charge_state.is_charging() {
                            "|>  "
                        } else {
                            "||  "
                        }
                        .chars()
                        .chain(
                            format!("{:>2} / {:>2}% SoC", progress.soc, progress.target_soc)
                                .chars(),
                        ),
                    )
                    .await;

                let minutes = progress.seconds_left / 60;
                let secs = progress.seconds_left % 60;
                self.oled
                    .write_line(
                        LineNb::Two,
                        format!("    {minutes:02}:{secs:02} left").chars(),
                    )
                    .await;
            }
            _ => {
                // show notification for a bit, except for error
                self.tick_period = if self.charge_state.is_error() {
                    None
                } else {
                    Some(self.idle_timeout)
                };
                self.oled
                    .write_line(LineNb::One, "⛶  charge point".chars())
                    .await;
                self.oled
                    .write_line(LineNb::Two, self.charge_state.name().chars())
                    .await;
            }
        }
    }

    async fn on_tick(&mut self) {
        use DisplayState::*;
        use PlaybackState::*;
        match (self.state, self.player_state) {
            (Player, Playing) => {
                let now = Instant::now();
                // let dt = (now - self.last_tick).as_secs_f32();
                self.last_tick = now;
                // FIXME switch update_line2 depending on animation
                self.update_line1 = true;
                self.update_oled_player(now).await;
            }
            (ChargePoint, player_state) if player_state.is_playing() => {
                // we only tick in ChargePoint mode when going back to Player mode
                info!(
                    charge_state = %self.charge_state.name(),
                    "switching back to player mode",
                );
                self.state = DisplayState::Player;
                self.update_line1 = true;
                self.update_line2 = true;
                self.tick_period = Some(PLAYING_TICK);
            }
            (Player, _) | (ChargePoint, _) => {
                info!("going blank due to inactivity");
                self.off().await;
                self.tick_period = None;
            }
            (Off, _) => {
                self.tick_period = None;
            }
        }
    }

    async fn run(&mut self) {
        loop {
            if let Some(tick_period) = self.tick_period {
                tokio::select! {
                    biased;

                    cmd = self.cmd_rx.recv() => match cmd {
                        Some(cmd) => self.on_command(cmd).await,
                        None => {
                            info!("command chan terminated");
                            break;
                        }
                    },

                    _ = sleep(Instant::now() + tick_period - self.last_tick) => self.on_tick().await,
                }
            } else {
                // no periodic refresh / timeout
                match self.cmd_rx.recv().await {
                    Some(cmd) => self.on_command(cmd).await,
                    None => {
                        info!("command chan terminated");
                        break;
                    }
                }
            }
        }
    }

    pub async fn into_task(mut self, mut stop_rx: broadcast::Receiver<()>) {
        tokio::select! {
            biased;
            _ = stop_rx.recv() => {
                info!("shutting down due to stop request");
            }
            _ = self.run() => (),
        }

        self.quit().await;
    }

    async fn off(&mut self) {
        self.oled.off().await;
        self.state = DisplayState::Off;
        self.player_data.clear();
    }

    async fn quit(&mut self) {
        info!("quitting display task");
        self.oled.off().await;
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
    #[expect(unused)]
    fn new(speed: f32, dwell_secs: f32) -> Self {
        // let mut sanitized = sanitize(text, DISPLAY_WIDTH * 3).into_bytes();
        // let scroll = if sanitized.len() > DISPLAY_WIDTH {
        // FIXME
        // Some(ScrollState::new(
        //     sanitized.clone(),
        //     DISPLAY_WIDTH,
        //     speed,
        //     dwell_secs,
        // ))
        //     None
        // } else {
        //     None
        // };
        RowDisplay {
            full_text: String::with_capacity(DISPLAY_WIDTH * 2),
            scroll: None,
        }
    }

    #[expect(unused)]
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

    fn set_text(&mut self, title: &str) {
        self.full_text.clear();
        self.full_text
            .push_str(&title[..usize::min(title.len(), 1 + DISPLAY_WIDTH * 2)]);
    }
}
