//! Display task handling
//!
//! Display can be in one of three states:
//!
//! * Off: display is blank
//! * Player: displaying current song
//! * Charge System: displaying charge system status

use std::fmt::Write;
use std::time::{Duration, Instant};

use tokio::sync::{broadcast, mpsc};
use tokio::time;
use tracing::{debug, info, warn};

use crate::charge_system::{
    self, AppliedChargingPlan, ChargeStatus, ChargeSystemConnectionState,
    Notification as ChargeSystemNotification,
};
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

const IDLE_TICK_TIMEOUT: Duration = Duration::from_secs(cfg_select! {
    all(target_arch = "aarch64", target_os = "linux") => {
        // expecting we target the raspberry pi
        300
    }
    _ => {
        5
    }
});

const PLAYING_REFRESH_INTERVAL: Duration = Duration::from_secs(1);
const CHARGING_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

/// Display Commands.
#[derive(Debug)]
pub enum DisplayCmd {
    Player(PlayerNotification),
    ChargeSystem {
        notification: ChargeSystemNotification,
        instant: Instant,
    },
}

/// Display state as reported by the MPRIS aggregator or Charge System listener.
#[derive(Debug, Clone, Copy, PartialEq)]
enum DisplayState {
    /// Display is off.
    Off,
    /// Displaying Player data.
    Player,
    /// Displaying Charge System data.
    ChargeSystem,
}

impl DisplayState {
    fn is_player(self) -> bool {
        matches!(self, DisplayState::Player)
    }
}

#[derive(Debug)]
struct Player {
    state: PlaybackState,
    data: PlayerData,
}

#[derive(Debug)]
struct ChargeSystem {
    charge_status: ChargeStatus,
    target_soc: Option<u8>,
    update_base_instant: Instant,
    connection_alert: Option<ChargeSystemConnectionState>,
}

impl ChargeSystem {
    fn must_alert(&self) -> bool {
        self.connection_alert.is_some_and(|a| a.is_alert()) || self.charge_status.is_error()
    }
    fn is_ok_and_idle(&self) -> bool {
        !self.must_alert() && !self.charge_status.is_charging()
    }
    fn is_significant(&self) -> bool {
        self.must_alert() || self.charge_status.is_significant()
    }
    fn clear_connection_alert(&mut self, notification: &ChargeSystemNotification) -> bool {
        if let Some(alert) = self.connection_alert.take() {
            info!(
                notification = %notification.name(),
                charge_state = ?self.charge_status,
                ?alert,
                "cleared charge system connection alert",
            );
            true
        } else {
            false
        }
    }
    fn get_tick_timeout(&self) -> Option<Duration> {
        if let Some(alert) = self.connection_alert {
            debug_assert!(!alert.is_heartbeat());
            debug!("disabling ticks due to alert");
            return None;
        }

        match self.charge_status {
            ChargeStatus::Charging(progress) if progress.remaining_time > 0 => {
                Some(CHARGING_REFRESH_INTERVAL)
            }
            ChargeStatus::Charging(_) | ChargeStatus::SuspendedEvse(_) | ChargeStatus::Error => {
                None
            }
            _ => Some(IDLE_TICK_TIMEOUT),
        }
    }
    fn state_name(&self) -> &'static str {
        self.connection_alert
            .map_or_else(|| self.charge_status.name(), |a| a.name())
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
    line2: String,
    tick_timeout: Option<Duration>,
    last_tick: Instant,
    player: Player,
    charge_system: ChargeSystem,
}

impl Display {
    pub async fn new(cmd_rx: mpsc::Receiver<DisplayCmd>) -> Result<Self, DriverError> {
        Ok(Display {
            cmd_rx,
            oled: Hd44780::new().await?,
            state: DisplayState::Off,
            position_str: String::with_capacity(5),
            duration_str: String::with_capacity(5),
            update_line1: false,
            update_line2: false,
            line2: String::with_capacity(DISPLAY_WIDTH),
            tick_timeout: None,
            last_tick: Instant::now(),
            player: Player {
                state: PlaybackState::Stopped,
                data: Default::default(),
            },
            charge_system: ChargeSystem {
                charge_status: ChargeStatus::Available,
                target_soc: None,
                update_base_instant: Instant::now(),
                connection_alert: None,
            },
        })
    }

    /// Timer based display update
    async fn on_tick(&mut self) {
        use DisplayState::*;
        use PlaybackState::*;
        match (self.state, self.player.state) {
            (Player, Playing) => {
                self.update_line1 = true;
                self.update_oled_in_player_state().await;
            }
            (ChargeSystem, _) if self.charge_system.charge_status.is_charging() => {
                self.update_oled_in_charge_system_state().await;
            }
            (ChargeSystem, player_state) if player_state.is_playing() => {
                info!(
                    charge_system_state = %self.charge_system.state_name(),
                    "charge system timeout, player is playing => switching back to player state",
                );
                self.switch_to_player_state(player_state);
            }
            (ChargeSystem, _) => {
                info!("charge system timeout, going blank due to inactivity");
                self.off().await;
                self.disable_tick();
            }
            (Player, _) => {
                if self.charge_system.is_significant() {
                    info!(
                        charge_system_state = %self.charge_system.state_name(),
                        "player timeout => switching back to charge system state",
                    );
                    self.switch_back_to_charge_system_state().await;
                    return;
                }

                info!("player timeout => going blank due to inactivity");
                self.off().await;
                self.disable_tick();
            }
            (Off, _) => {
                self.disable_tick();
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
            }) => {
                if player_state.is_stopped() && self.charge_system.is_significant() {
                    self.switch_back_to_charge_system_state().await;
                    debug!(
                        charge_system_state = %self.charge_system.state_name(),
                        "updating (not displaying)",
                    );
                }

                self.update_player(player_state, data).await;
            }
            DisplayCmd::Player(PlayerNotification::BasePosition {
                position_s,
                instant,
            }) => {
                debug!(base_pos_s = %position_s, state = ?self.state, "update");
                self.player.data.base_position_s = position_s;
                self.player.data.base_position_instant = instant;
            }
            DisplayCmd::ChargeSystem {
                notification,
                instant,
            } => {
                self.update_charge_system(notification, instant).await;
            }
        }
    }

    async fn update_player(&mut self, player_state: PlaybackState, data: PlayerData) {
        let can_display = self.state.is_player();

        match player_state {
            PlaybackState::Playing if !self.player.state.is_playing() => {
                debug!(old = ?self.player.state, new = ?player_state, "player state changed");
                self.player.state = PlaybackState::Playing;
                self.update_line1 |= can_display;

                if self.state.is_player() {
                    self.set_tick_timeout(PLAYING_REFRESH_INTERVAL);
                }
            }
            PlaybackState::Paused if !self.player.state.is_paused() => {
                debug!(old = ?self.player.state, new = ?player_state, "player state changed");
                self.player.state = PlaybackState::Paused;
                self.update_line1 |= can_display;

                if self.state.is_player() {
                    self.set_tick_timeout(IDLE_TICK_TIMEOUT);
                }
            }
            PlaybackState::Stopped => {
                self.player.state = PlaybackState::Stopped;
                self.update_line1 |= can_display;

                if self.state.is_player() {
                    self.set_tick_timeout(IDLE_TICK_TIMEOUT);
                }
            }
            _ => (),
        }

        let data_updated = self.player.data.base_position_s != data.base_position_s
            || self.player.data.base_position_instant != data.base_position_instant
            || self.player.data.duration_s != data.duration_s;

        self.update_line1 |= can_display && data_updated;

        if data_updated {
            debug!(old = ?self.player.data, new = ?data, "data updated");

            let title_updated = self.player.data.title != data.title;
            self.player.data = data;

            if title_updated && can_display {
                self.set_line2_with_player_title();
            }
        }

        if can_display {
            self.update_oled_in_player_state().await;
        }
    }

    async fn update_oled_in_player_state(&mut self) {
        fn format_time(time_str: &mut String, total_secs: u16) {
            let minutes = total_secs / 60;
            let seconds = total_secs % 60;
            time_str.clear();
            time_str
                .write_fmt(format_args!("{:02}:{:02}", minutes, seconds))
                .unwrap();
        }

        let Self {
            oled,
            player,
            position_str,
            duration_str,
            line2,
            ..
        } = self;

        if self.update_line1 {
            self.update_line1 = false;
            format_time(
                position_str,
                player.data.base_position_s
                    + player.data.base_position_instant.elapsed().as_secs() as u16,
            );
            // might want to skip if duration did not change
            format_time(duration_str, player.data.duration_s);

            let state_prefix = match player.state {
                PlaybackState::Playing => "|> ",
                PlaybackState::Paused => "|| ",
                PlaybackState::Stopped => "⛶  Musique",
            };

            if !player.state.is_stopped() {
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

        oled.write_line(LineNb::Two, line2.chars()).await;
    }
}

/// ChargeSystem specific
impl Display {
    async fn on_command_in_charge_system_state(&mut self, cmd: DisplayCmd) {
        match cmd {
            DisplayCmd::Player(PlayerNotification::Update {
                state: new_player_state,
                data,
            }) => {
                // only way to switch to player state from
                // a charge system alert or a charging state
                // is to get the player to play from the stopped state
                // This ensures we don't switch to player state
                // when changing track, which can trigger
                // playing -> paused -> playing.
                if new_player_state.is_playing()
                    && (self.player.state.is_stopped()
                        || self.charge_system.is_ok_and_idle() && !self.player.state.is_playing())
                {
                    info!(
                        charge_system_state = %self.charge_system.state_name(),
                        old_player_state = ?self.player.state,
                        "switching to Player state",
                    );
                    self.switch_to_player_state(new_player_state);
                } else {
                    debug!(
                        charge_system_state = %self.charge_system.state_name(),
                        "updating (not displaying)",
                    );
                }

                self.update_player(new_player_state, data).await;
            }
            DisplayCmd::Player(PlayerNotification::BasePosition {
                position_s,
                instant,
            }) => {
                debug!(base_pos_s = %position_s, state = ?self.state);
                self.player.data.base_position_s = position_s;
                self.player.data.base_position_instant = instant;
            }
            DisplayCmd::ChargeSystem {
                notification: state,
                instant,
            } => {
                self.update_charge_system(state, instant).await;
            }
        }
    }

    /// Update internal state data
    async fn update_charge_system(
        &mut self,
        notification: ChargeSystemNotification,
        instant: Instant,
    ) {
        if notification.is_critical() {
            warn!(?notification, "charge system");
        } else {
            debug!(?notification, "charge system");
        }

        use ChargeSystemNotification::*;
        match &notification {
            Status(charge_status) => {
                self.state = DisplayState::ChargeSystem;
                self.charge_system.clear_connection_alert(&notification);

                if let ChargeStatus::Charging(progress) = charge_status {
                    if let ChargeStatus::Charging(ref cur_progress)
                    | ChargeStatus::SuspendedEvse(ref cur_progress) =
                        self.charge_system.charge_status
                    {
                        self.update_line1 |= ((progress.soc * 100.0).floor()
                            - (cur_progress.soc * 100.0).floor())
                        .abs()
                            >= 1.0;

                        self.update_line2 = progress.remaining_time != cur_progress.remaining_time;
                    } else {
                        self.update_line1 = true;
                        self.update_line2 = true;
                    }
                } else {
                    self.update_line1 = true;
                    self.update_line2 = true;
                }

                self.charge_system.charge_status = *charge_status;
                self.charge_system.update_base_instant = instant;
                self.set_tick_timeout(self.charge_system.get_tick_timeout());
            }
            BmsUpdate(bms) => {
                // TODO: handle relative SoC
                if let Some(cur_soc) = bms.current_soc().inner() {
                    if let ChargeStatus::Charging(ref mut cur_progress)
                    | ChargeStatus::SuspendedEvse(ref mut cur_progress) =
                        self.charge_system.charge_status
                        && ((cur_progress.soc * 100.0).floor() - (cur_soc * 100.0).floor()).abs()
                            >= 1.0
                    {
                        cur_progress.soc = cur_soc;
                        self.update_line1 = true;
                    }
                }

                let target_soc = bms.target_soc.map(|bts| (bts * 100.0).round() as u8);
                if self.charge_system.target_soc != target_soc {
                    self.charge_system.target_soc = target_soc;
                    self.update_line1 = true;
                }
            }
            Connection(ChargeSystemConnectionState::Heartbeat) => {
                if self.charge_system.clear_connection_alert(&notification) {
                    // recovered connection => get back to charge state
                    self.set_tick_timeout(self.charge_system.get_tick_timeout());
                } else {
                    // no connection alert just ignore
                    return;
                }
            }
            Connection(alert) => {
                self.state = DisplayState::ChargeSystem;
                self.charge_system.connection_alert = Some(*alert);
                self.disable_tick();
            }
            ChargingPlan(AppliedChargingPlan { plan, .. }) => {
                if let charge_system::ChargingPlan::Deadline { target_soc, .. }
                | charge_system::ChargingPlan::Asap { target_soc, .. } = plan
                {
                    let target_soc = (target_soc * 100.0).round() as u8;
                    if self
                        .charge_system
                        .target_soc
                        .is_none_or(|cur_target_soc| cur_target_soc != target_soc)
                    {
                        self.charge_system.target_soc = Some(target_soc);
                        self.update_line1 = true;
                    } else if self.charge_system.target_soc.is_some() {
                        self.charge_system.target_soc = None;
                        self.update_line1 = true;
                    }
                }
            }
            MeterValues(_) => {
                return;
            }
        }

        if self.state == DisplayState::ChargeSystem {
            self.update_oled_in_charge_system_state().await;
        }
    }

    async fn update_oled_in_charge_system_state(&mut self) {
        if let Some(alert) = self.charge_system.connection_alert {
            debug_assert!(!alert.is_heartbeat());

            self.oled
                .write_line(LineNb::One, "×  Charge System".chars())
                .await;
            self.oled
                .write_line(LineNb::Two, alert.name().chars())
                .await;

            return;
        }

        use ChargeStatus::*;
        match &self.charge_system.charge_status {
            Charging(progress) | SuspendedEvse(progress) => {
                if self.update_line1 {
                    self.update_line1 = false;

                    let target_soc_str = if let Some(target_soc) = self.charge_system.target_soc {
                        format!("{target_soc:>2}")
                    } else {
                        "??".to_string()
                    };

                    self.oled
                        .write_line(
                            LineNb::One,
                            if self.charge_system.charge_status.is_charging() {
                                "|> "
                            } else {
                                "|| "
                            }
                            .chars()
                            .chain(format!("{:>2}", (progress.soc * 100.0).floor()).chars())
                            .chain(" / ".chars())
                            .chain(target_soc_str.chars())
                            .chain("% bat".chars()),
                        )
                        .await;
                }

                if self.update_line2 {
                    let seconds_left = if self.charge_system.charge_status.is_charging() {
                        progress.remaining_time.saturating_sub(
                            self.charge_system.update_base_instant.elapsed().as_secs() as u32,
                        )
                    } else {
                        progress.remaining_time
                    };
                    let minutes = seconds_left / 60;
                    let secs = seconds_left % 60;
                    self.oled
                        .write_line(
                            LineNb::Two,
                            format!("{minutes:02}:{secs:02} prévues").chars(),
                        )
                        .await;
                }
            }
            other => {
                let symbol = if other.is_error() { '×' } else { '⛶' };
                self.oled
                    .write_line(
                        LineNb::One,
                        std::iter::once(symbol).chain("  Borne VE".chars()),
                    )
                    .await;
                self.oled
                    .write_line(LineNb::Two, other.name().chars())
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
                position_s,
                instant,
            }) => {
                debug!(base_pos_s = %position_s, state = ?self.state, "update in Off state");
                self.player.data.base_position_s = position_s;
                self.player.data.base_position_instant = instant;
            }
            DisplayCmd::ChargeSystem {
                notification: state,
                instant,
            } => {
                self.oled.clear_on().await;
                self.state = DisplayState::ChargeSystem;
                self.update_charge_system(state, instant).await;
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
                    cmd = self.cmd_rx.recv() => {
                        if self.cmd_rx.len() + 3 > self.cmd_rx.capacity() {
                            warn!(len = %self.cmd_rx.len(), capa = %self.cmd_rx.capacity(), "command chan");
                        }

                        if let Some(cmd) = cmd {
                            self.on_command(cmd).await;
                        } else {
                            info!("command chan terminated");
                            break;
                        }
                    }
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

    fn disable_tick(&mut self) {
        debug!("disabling ticks");
        self.tick_timeout = None;
    }

    fn set_tick_timeout(&mut self, timeout: impl Into<Option<Duration>>) {
        let timeout = timeout.into();

        if let Some(timeout) = timeout {
            debug!(?timeout, "setting tick");
            self.tick_timeout = Some(timeout);
            self.last_tick = Instant::now();
        } else {
            self.disable_tick();
        }
    }

    async fn on_command(&mut self, cmd: DisplayCmd) {
        match self.state {
            DisplayState::Player => self.on_command_in_player_state(cmd).await,
            DisplayState::ChargeSystem => self.on_command_in_charge_system_state(cmd).await,
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
        self.set_line2_with_player_title();

        self.set_tick_timeout(if new_player_state.is_playing() {
            PLAYING_REFRESH_INTERVAL
        } else {
            IDLE_TICK_TIMEOUT
        });
    }

    async fn switch_back_to_charge_system_state(&mut self) {
        debug!(
            charge_system_state = %self.charge_system.state_name(),
            "switching back to charge system state",
        );

        self.state = DisplayState::ChargeSystem;

        self.update_line1 = true;
        self.update_line2 = true;

        // This is different from the regular charge system timeouts
        // since user explicitly went to Player stopped, then playing again,
        // discarding the charge system state / alert.
        // Let's just display the charge system state / alert until
        // timeout, except if a charge is in progress.
        self.set_tick_timeout(Some(if self.charge_system.charge_status.is_charging() {
            CHARGING_REFRESH_INTERVAL
        } else {
            IDLE_TICK_TIMEOUT
        }));

        self.update_oled_in_charge_system_state().await;
    }

    fn set_line2_with_player_title(&mut self) {
        self.line2.clear();
        self.line2.push_str(
            &self.player.data.title
                [..usize::min(self.player.data.title.chars().count(), 1 + DISPLAY_WIDTH)],
        );
        self.update_line2 = true;
    }

    async fn off(&mut self) {
        self.oled.off().await;
        self.state = DisplayState::Off;
        self.player.data.clear();
    }

    async fn quit(&mut self) {
        info!("quitting display task");
        self.oled.off().await;
    }
}

pub trait ChargeSystemNotificationExt {
    fn is_critical(&self) -> bool;
}
impl ChargeSystemNotificationExt for ChargeSystemNotification {
    fn is_critical(&self) -> bool {
        matches!(
            self,
            ChargeSystemNotification::Status(ChargeStatus::Error)
                | ChargeSystemNotification::Connection(ChargeSystemConnectionState::Error)
                | ChargeSystemNotification::Connection(
                    ChargeSystemConnectionState::MissedHeartbeat
                )
        )
    }
}

trait ChargeStatusExt {
    fn name(&self) -> &'static str;
    fn is_charging(&self) -> bool;
    fn is_error(&self) -> bool;
    fn is_significant(&self) -> bool;
}
impl ChargeStatusExt for ChargeStatus {
    fn name(&self) -> &'static str {
        use ChargeStatus::*;
        match self {
            Available => "disponible",
            Preparing => "en préparation",
            Charging(_) => "en charge",
            SuspendedEvse(_) => "pause programmée",
            SuspendedEv => "arrêt véhicule",
            StoppedByUser => "arrêt externe",
            Finishing => "VE déconnecté",
            UnknownSession => "session inconnue",
            Error => "erreur en charge",
        }
    }

    fn is_charging(&self) -> bool {
        matches!(self, Self::Charging(_))
    }
    fn is_error(&self) -> bool {
        matches!(self, Self::Error)
    }
    fn is_significant(&self) -> bool {
        matches!(
            self,
            ChargeStatus::Charging(_)
                | ChargeStatus::SuspendedEvse(_)
                | ChargeStatus::SuspendedEv
                | ChargeStatus::StoppedByUser
                | ChargeStatus::Finishing
                | ChargeStatus::UnknownSession
                | ChargeStatus::Error
        )
    }
}

trait ChargeSystemConnectionStateExt {
    fn name(&self) -> &'static str;
    fn is_heartbeat(&self) -> bool;
    fn is_alert(&self) -> bool;
}
impl ChargeSystemConnectionStateExt for ChargeSystemConnectionState {
    fn name(&self) -> &'static str {
        use ChargeSystemConnectionState::*;
        match self {
            Heartbeat => "pulsation",
            MissedHeartbeat => "absence de pouls",
            ServerDisconnected => "pas de connecθ",
            Error => "erreur connecθ",
        }
    }

    fn is_heartbeat(&self) -> bool {
        matches!(self, ChargeSystemConnectionState::Heartbeat)
    }
    fn is_alert(&self) -> bool {
        matches!(
            self,
            ChargeSystemConnectionState::Error
                | ChargeSystemConnectionState::MissedHeartbeat
                | ChargeSystemConnectionState::ServerDisconnected
        )
    }
}
