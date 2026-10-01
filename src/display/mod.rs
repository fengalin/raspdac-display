//! Display task handling
//!
//! Display can be in one of three states:
//!
//! * Off: display is blank
//! * Player: displaying current song
//! * Charge Point: displaying charge point status

use std::fmt::Write;
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, mpsc};
use tokio::time;
use tracing::{debug, info};

use crate::charge_point::ChargeState;
use crate::config::Config;
use crate::{PlaybackState, PlayerData, PlayerNotification};

cfg_select! {
    all(target_arch = "aarch64", target_os = "linux") => {
        // expecting we target the raspberry pi
        mod driver;
        pub use driver::*;
    }
    _ => {
        mod simu;
        pub use simu::*;
    }
}

mod scroll;
use scroll::ScrollState;

const PLAYING_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const CHARGING_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// Display Commands.
#[derive(Debug)]
pub enum DisplayCmd {
    Player(PlayerNotification),
    ChargePoint {
        state: ChargeState,
        instant: Instant,
    },
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
    fn is_player(self) -> bool {
        matches!(self, DisplayState::Player)
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
    idle_timeout: Duration,
    tick_timeout: Option<Duration>,
    last_tick: Instant,
    player_state: PlaybackState,
    player_data: PlayerData,
    charge_state: ChargeState,
    charge_state_base_instant: Instant,
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
            idle_timeout: config.idle.timeout,
            tick_timeout: None,
            last_tick: Instant::now(),
            player_state: PlaybackState::Stopped,
            player_data: Default::default(),
            charge_state: ChargeState::Available,
            charge_state_base_instant: Instant::now(),
        })
    }

    /// Timer based display update
    async fn on_tick(&mut self) {
        use DisplayState::*;
        use PlaybackState::*;
        match (self.state, self.player_state) {
            (Player, Playing) => {
                // let dt = self.last_tick.elaspsed().as_secs_f32();
                // FIXME switch update_line2 depending on animation
                self.update_line1 = true;
                self.update_oled_in_player_state().await;
            }
            (ChargePoint, _) if self.charge_state.is_charging() => {
                self.update_oled_in_charge_point_state().await;
            }
            (ChargePoint, player_state) if player_state.is_playing() => {
                info!(
                    charge_state = %self.charge_state.name(),
                    "switching back to player state",
                );
                self.switch_to_player_state(player_state);
            }
            (Player, _) | (ChargePoint, _) => {
                info!("going blank due to inactivity");
                self.off().await;
                self.tick_timeout = None;
            }
            (Off, _) => {
                self.tick_timeout = None;
            }
        }
    }
}

/// Player specific
impl Display {
    async fn on_command_in_player_state(&mut self, cmd: DisplayCmd) {
        match cmd {
            DisplayCmd::Player(PlayerNotification::Update {
                state: player_state,
                data,
            }) => self.update_player(player_state, data).await,
            DisplayCmd::Player(PlayerNotification::BasePosition {
                position_us,
                instant,
            }) => {
                debug!(base_pos_us = %position_us, state = ?self.state, "update");
                self.player_data.base_position_us = position_us;
                self.player_data.base_position_instant = instant;
            }
            DisplayCmd::ChargePoint { state, instant } => {
                self.state = DisplayState::ChargePoint;
                self.update_charge_point(state, instant).await;
            }
        }
    }

    async fn update_player(&mut self, player_state: PlaybackState, data: PlayerData) {
        let can_display = self.state.is_player();

        match player_state {
            PlaybackState::Playing if !self.player_state.is_playing() => {
                debug!(old = ?self.player_state, new = ?player_state, "player state changed");
                self.player_state = PlaybackState::Playing;
                self.update_line1 |= can_display;

                if self.state.is_player() {
                    self.tick_timeout = Some(PLAYING_REFRESH_INTERVAL);
                }
            }
            PlaybackState::Paused if !self.player_state.is_paused() => {
                debug!(old = ?self.player_state, new = ?player_state, "player state changed");
                self.player_state = PlaybackState::Paused;
                self.update_line1 |= can_display;

                if self.state.is_player() {
                    self.tick_timeout = Some(self.idle_timeout);
                }
            }
            PlaybackState::Stopped => {
                self.player_state = PlaybackState::Stopped;
                self.update_line1 |= can_display;

                if self.state.is_player() {
                    self.tick_timeout = Some(self.idle_timeout);
                }
            }
            _ => (),
        }

        let mut data_updated = self.player_data.base_position_us != data.base_position_us
            || self.player_data.base_position_instant != data.base_position_instant
            || self.player_data.duration_us != data.duration_us;

        self.update_line1 |= can_display && data_updated;

        if self.player_data.title != data.title {
            data_updated = true;

            if can_display {
                self.line2.set_text(data.title.as_ref());
                self.update_line2 |= true;
            }
        }

        if data_updated {
            debug!(old = ?self.player_data, new = ?data, "data updated");
            self.player_data = data;
        }

        if can_display {
            self.update_oled_in_player_state().await;
        }
    }

    async fn update_oled_in_player_state(&mut self) {
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
                data.base_position_us + data.base_position_instant.elapsed().as_micros() as u64,
            );
            // might want to skip if duration did not change
            format_time(duration_str, data.duration_us);

            let state_prefix = match player_state {
                PlaybackState::Playing => "|> ",
                PlaybackState::Paused => "|| ",
                PlaybackState::Stopped => "⛶  music",
            };

            if !player_state.is_stopped() {
                oled.write_line(
                    LineNb::One,
                    state_prefix
                        .chars()
                        .chain(position_str.chars())
                        .chain(" / ".chars())
                        .chain(duration_str.chars()),
                )
                .await;
            } else {
                oled.write_line(LineNb::One, state_prefix.chars()).await;
            }
        }

        if !self.update_line2 {
            return;
        }
        self.update_line2 = false;

        oled.write_line(LineNb::Two, line2.full_text.chars()).await;
    }
}

/// ChargePoint specific
impl Display {
    async fn on_command_in_charge_point_state(&mut self, cmd: DisplayCmd) {
        match cmd {
            DisplayCmd::Player(PlayerNotification::Update {
                state: player_state,
                data,
            }) => {
                if !self.charge_state.is_charging()
                    && !self.charge_state.is_error()
                    && player_state.is_playing()
                    && !self.player_state.is_playing()
                {
                    info!(
                        charge_state = %self.charge_state.name(),
                        "switching to Player state",
                    );
                    self.switch_to_player_state(player_state);
                } else {
                    debug!(
                        charge_state = %self.charge_state.name(),
                        "updating (not displaying)",
                    );
                }

                self.update_player(player_state, data).await;
            }
            DisplayCmd::Player(PlayerNotification::BasePosition {
                position_us,
                instant,
            }) => {
                debug!(base_pos_us = %position_us, state = ?self.state);
                self.player_data.base_position_us = position_us;
                self.player_data.base_position_instant = instant;
            }
            DisplayCmd::ChargePoint { state, instant } => {
                self.update_charge_point(state, instant).await;
            }
        }
    }

    /// Update internal state data
    async fn update_charge_point(&mut self, charge_state: ChargeState, instant: Instant) {
        info!(msg = ?charge_state, "charge point");

        use ChargeState::*;
        match charge_state {
            Charging(_) => self.tick_timeout = Some(CHARGING_REFRESH_INTERVAL),
            SuspendedEvse(_) | Error => self.tick_timeout = None,
            _ => self.tick_timeout = Some(self.idle_timeout),
        }

        self.charge_state = charge_state;
        self.charge_state_base_instant = instant;

        self.update_oled_in_charge_point_state().await;
    }

    async fn update_oled_in_charge_point_state(&mut self) {
        use ChargeState::*;
        match &self.charge_state {
            Charging(progress) | SuspendedEvse(progress) => {
                self.tick_timeout = None;
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

                let seconds_left = if self.charge_state.is_charging() {
                    progress
                        .seconds_left
                        .saturating_sub(self.charge_state_base_instant.elapsed().as_secs() as u16)
                } else {
                    progress.seconds_left
                };
                let minutes = seconds_left / 60;
                let secs = seconds_left % 60;
                self.oled
                    .write_line(
                        LineNb::Two,
                        format!("    {minutes:02}:{secs:02} left").chars(),
                    )
                    .await;
            }
            _ => {
                // show notification for a bit, except for error
                self.tick_timeout = if self.charge_state.is_error() {
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
}

/// Off specific
impl Display {
    async fn on_command_in_off_state(&mut self, cmd: DisplayCmd) {
        match cmd {
            DisplayCmd::Player(PlayerNotification::Update {
                state: player_state,
                data,
            }) => {
                if player_state.is_playing() {
                    info!("switching to Player state from Off state",);
                    self.oled.clear_on().await;
                    self.switch_to_player_state(player_state);
                } else {
                    debug!("updating in Off state (not displaying)");
                }

                self.update_player(player_state, data).await;
            }
            DisplayCmd::Player(PlayerNotification::BasePosition {
                position_us,
                instant,
            }) => {
                debug!(base_pos_us = %position_us, state = ?self.state, "update in Off state");
                self.player_data.base_position_us = position_us;
                self.player_data.base_position_instant = instant;
            }
            DisplayCmd::ChargePoint { state, instant } => {
                self.oled.clear_on().await;
                self.state = DisplayState::ChargePoint;
                self.update_charge_point(state, instant).await;
            }
        }
    }
}

/// General machinery
impl Display {
    async fn run(&mut self) {
        loop {
            if let Some(tick_period) = self.tick_timeout {
                tokio::select! {
                    biased;
                    cmd = self.cmd_rx.recv() => match cmd {
                        Some(cmd) => self.on_command(cmd).await,
                        None => {
                            info!("command chan terminated");
                            break;
                        }
                    },
                    _ = time::sleep(
                        tick_period.saturating_sub(self.last_tick.elapsed())
                    ) => {
                        self.last_tick = Instant::now();
                        self.on_tick().await;
                    }
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

    async fn on_command(&mut self, cmd: DisplayCmd) {
        match self.state {
            DisplayState::Player => self.on_command_in_player_state(cmd).await,
            DisplayState::ChargePoint => self.on_command_in_charge_point_state(cmd).await,
            DisplayState::Off => self.on_command_in_off_state(cmd).await,
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

    fn switch_to_player_state(&mut self, new_player_state: PlaybackState) {
        self.state = DisplayState::Player;

        self.update_line1 = true;
        self.line2.set_text(self.player_data.title.as_ref());
        self.update_line2 = true;

        self.tick_timeout = Some(if new_player_state.is_playing() {
            PLAYING_REFRESH_INTERVAL
        } else {
            self.idle_timeout
        });
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
