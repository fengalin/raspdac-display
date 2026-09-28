//! Signal-based MPRIS media player listeners.
//!
//! Each configured player (MPD, pibuz) is tracked on the session D-Bus:
//! - `NameOwnerChanged` detects the player appearing on / leaving the bus.
//! - `PropertiesChanged` (interface `org.mpris.MediaPlayer2.Player`) drives
//!   updates for playback status, metadata, rate, and position.
//! - The `Seeked` signal triggers an immediate position resync.
//! - The `Position` property is resampled periodically, because the MPRIS
//!   specification does not emit `PropertiesChanged` for it.
//!
//! An aggregator task keeps per-player state, picks the active player,
//! and sends the resulting `DisplayState` to the display thread.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::StreamExt;
use thiserror::Error;
use tokio::sync::mpsc;
use tracing::{debug, error, info, trace, warn};
use zbus::Message;
use zbus::fdo::NameOwnerChanged;
use zbus::fdo::PropertiesChanged;
use zbus::fdo::{DBusProxy, PropertiesProxy};
use zbus::names::{BusName, InterfaceName};
use zbus::zvariant::Value;

use crate::display::DisplayCmd;

const OBJECT_PATH: &str = "/org/mpris/MediaPlayer2";
const PLAYER_IFACE: &str = "org.mpris.MediaPlayer2.Player";
/// Interval between `Position` property resamples (drift correction).
const POSITION_RESYNC_SECS: u64 = 30;
/// Delay before reconnecting after the listener loop ends.
const RECONNECT_SECS: u64 = 5;

const DEFAULT_TITLE: &str = "";

#[derive(Debug, Error)]
pub enum MprisError {
    #[error("D-Bus error: {0}")]
    Dbus(#[from] zbus::Error),
    #[error("D-Bus error: {0}")]
    Fdo(#[from] zbus::fdo::Error),
    #[error("invalid bus name: {0}")]
    BusName(#[from] zbus::names::Error),
    #[error("channel error: {0}")]
    Channel(#[from] tokio::sync::mpsc::error::SendError<PlayerNotification>),
}

/// MPRIS playback status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaybackState {
    Playing,
    Paused,
    Stopped,
}

impl<'a> From<&'a Value<'a>> for PlaybackState {
    fn from(value: &'a Value<'a>) -> Self {
        match value {
            Value::Str(v) => match v.as_str() {
                "Playing" => PlaybackState::Playing,
                "Paused" => PlaybackState::Paused,
                "Stopped" => PlaybackState::Stopped,
                other => {
                    debug!(status = %other, "unhandled str status");
                    PlaybackState::Stopped
                }
            },
            other => {
                info!(status = ?other, "unhandled non-str status");
                PlaybackState::Stopped
            }
        }
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
        bus_name: Arc<BusName<'static>>,
        state: PlaybackState,
        data: PlayerData,
    },
    BasePosition {
        bus_name: Arc<BusName<'static>>,
        position_us: u64,
        instant: Instant,
    },
}

/// Per-player state update sent to the aggregator.
#[derive(Debug, Clone)]
pub struct Player {
    bus_name: Arc<BusName<'static>>,
    state: PlaybackState,
    data: PlayerData,
}

impl Player {
    pub fn new(bus_name: &'static str) -> Result<Self, MprisError> {
        Ok(Player {
            bus_name: BusName::try_from(bus_name)?.into(),
            state: PlaybackState::Stopped,
            data: Default::default(),
        })
    }

    /// Applies one `PropertiesChanged` batch for the Player interface.
    ///
    /// Returns true when display should be notified.
    fn apply_props<'a>(&mut self, values: impl Iterator<Item = (&'a str, &'a Value<'a>)>) -> bool {
        let mut must_notify = false;

        for (key, val) in values {
            match key {
                "PlaybackStatus" => {
                    let new_state = val.into();
                    if self.state != new_state {
                        debug!(player = %self.bus_name,
                            old = ?self.state, new = ?new_state,
                            "new status",
                        );
                        self.state = new_state;
                        if new_state == PlaybackState::Playing {
                            self.data.base_position_instant = Instant::now();
                        }
                        must_notify = true;
                    } else {
                        // FIXME check whether this is still needed
                        must_notify = self.state == PlaybackState::Playing;
                    }
                }
                "Metadata" => {
                    let Value::Dict(metadata) = val else {
                        panic!("unexpected type for metadata");
                    };
                    let title = metadata
                        .get::<_, &str>(&"xesam:title")
                        .inspect_err(|err| {
                            error!(player = %self.bus_name, %err, "Metadata title");
                        })
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                    if self.data.title.as_ref() != title {
                        debug!(player = %self.bus_name,
                            old = %self.data.title, new = %title,
                            "new title",
                        );
                        self.data.title = title.into();
                        must_notify = true;
                    }

                    let duration: u64 = metadata
                        .get::<_, i64>(&"mpris:length")
                        .inspect_err(|err| {
                            error!(player = %self.bus_name, %err, "Metadata length");
                        })
                        .ok()
                        .flatten()
                        .unwrap_or_default()
                        .try_into()
                        .inspect_err(|err| {
                            error!(player = %self.bus_name, %err, "Metadata negative length");
                        })
                        .ok()
                        .unwrap_or_default();
                    if self.data.duration_us != duration {
                        debug!(player = %self.bus_name,
                            old = %self.data.duration_us, new = %duration,
                            "new duration",
                        );
                        self.data.duration_us = duration;
                        must_notify = true;
                    }
                }
                "TrackId" => must_notify = true,
                "Position" => {
                    // note: position is only set on initial props.get_all()
                    match i64::try_from(val) {
                        Ok(pos) => {
                            debug!(player = %self.bus_name, new = %pos, "new pos");
                            self.data.base_position_us = pos
                                .try_into()
                                .inspect_err(|err| {
                                    error!(player = %self.bus_name, %err, "negative Position");
                                })
                                .unwrap_or_default();
                            self.data.base_position_instant = Instant::now();
                        }
                        Err(err) => {
                            error!(player = %self.bus_name, %err, "type mismatch reading Position");
                        }
                    }
                }
                "Rate" => {
                    self.data.rate = val
                        .try_into()
                        .inspect_err(|err| {
                            error!(player = %self.bus_name, %err, "Rate");
                        })
                        .unwrap_or_default();
                }
                _ => {}
            }
        }

        must_notify
    }

    async fn on_props_changed_message<'a>(
        &mut self,
        player_iface: &'a InterfaceName<'a>,
        player_notif_tx: &mpsc::Sender<PlayerNotification>,
        props_changed: PropertiesChanged,
    ) -> Result<(), MprisError> {
        let args = props_changed.args()?;
        if args.interface_name() != player_iface {
            debug!(player = ?self.bus_name, iface = ?args.interface_name(), "rejecting msg for other iface");
            return Ok(());
        }
        if self.apply_props(args.changed_properties().iter().map(|(k, v)| (*k, v))) {
            player_notif_tx
                .send(PlayerNotification::Update {
                    bus_name: self.bus_name.clone(),
                    state: self.state,
                    data: self.data.clone(),
                })
                .await?;
        }

        Ok(())
    }

    fn on_name_owner_changed_message(
        &mut self,
        name_owner_changed: NameOwnerChanged,
    ) -> Result<(), MprisError> {
        if name_owner_changed.args()?.new_owner().is_none() {
            return Ok(());
        }
        debug!(player = ?self.bus_name, "owner changed");

        Ok(())
    }

    async fn on_seeked_message(
        &mut self,
        player_notif_tx: &mpsc::Sender<PlayerNotification>,
        seeked: Message,
    ) -> Result<(), MprisError> {
        if let Ok((pos,)) = seeked
            .body()
            .deserialize::<(u64,)>()
            .inspect(|err| error!(player = %self.bus_name, ?err, "seeked msg deser error"))
        {
            debug!(player = %self.bus_name, %pos, "seeked message");
            self.data.base_position_us = pos;
            self.data.base_position_instant = Instant::now();
            player_notif_tx
                .send(PlayerNotification::BasePosition {
                    bus_name: self.bus_name.clone(),
                    position_us: self.data.base_position_us,
                    instant: self.data.base_position_instant,
                })
                .await?;
        }

        Ok(())
    }

    fn clear(&mut self) {
        self.state = PlaybackState::Stopped;
        self.data.clear();
    }

    /// Run the listener loop for one player, reconnecting when it ends.
    pub async fn into_task(mut self, display_cmd_tx: mpsc::Sender<PlayerNotification>) {
        loop {
            match self.listen(&display_cmd_tx).await {
                Ok(()) => info!(player = %self.bus_name, "left the bus"),
                Err(err) => warn!(player = %self.bus_name, %err, "listener error"),
            }

            self.clear();
            let _ = display_cmd_tx
                .send(PlayerNotification::Update {
                    bus_name: self.bus_name.clone(),
                    state: self.state,
                    data: self.data.clone(),
                })
                .await;

            tokio::time::sleep(Duration::from_secs(RECONNECT_SECS)).await;
        }
    }

    /// Watch one player: initial snapshot, then `PropertiesChanged`, `Seeked`,
    /// `NameOwnerChanged`, and periodic `Position` resamples.
    ///
    /// Returns when the player leaves the bus, or on a fatal D-Bus error.
    async fn listen(
        &mut self,
        player_notif_tx: &mpsc::Sender<PlayerNotification>,
    ) -> Result<(), MprisError> {
        let conn = zbus::Connection::session().await?;
        let player_iface = InterfaceName::try_from(PLAYER_IFACE)?;
        let dbus = DBusProxy::new(&conn).await?;

        // Wait for the player to appear on the bus
        if !dbus.name_has_owner(self.bus_name.as_ref().into()).await? {
            info!(player = %self.bus_name, "waiting on the session bus...");
            let mut stream = dbus
                .receive_name_owner_changed_with_args(&[(0, self.bus_name.as_str())])
                .await?;
            while let Some(signal) = stream.next().await {
                if signal.args()?.new_owner().is_some() {
                    break;
                }
            }
        }

        let props = PropertiesProxy::builder(&conn)
            .destination(self.bus_name.as_ref())?
            .path(OBJECT_PATH)?
            .build()
            .await?;

        // Initial snapshot of the Player interface.
        let prop_values = props.get_all(player_iface.clone()).await?;
        self.apply_props(
            prop_values
                .iter()
                .map(|(k, v)| (k.as_str(), v.downcast_ref().unwrap())),
        );

        player_notif_tx
            .send(PlayerNotification::Update {
                bus_name: self.bus_name.clone(),
                state: self.state,
                data: self.data.clone(),
            })
            .await?;

        // Signal streams.
        let mut props_changed_stream = props.receive_properties_changed().await?;
        let mut name_owner_changed_stream = dbus
            .receive_name_owner_changed_with_args(&[(0, self.bus_name.as_str())])
            .await?;
        let player =
            zbus::proxy::Proxy::new(&conn, self.bus_name.as_ref(), OBJECT_PATH, PLAYER_IFACE)
                .await?;
        let mut seeked_stream = player.receive_signal("Seeked").await?;

        let mut resync = tokio::time::interval(Duration::from_secs(POSITION_RESYNC_SECS));
        resync.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        resync.tick().await; // first tick is immediate; consume it

        loop {
            if self.state == PlaybackState::Playing {
                tokio::select! {
                    Some(props_changed) = props_changed_stream.next() => {
                        self.on_props_changed_message(
                            &player_iface,
                            player_notif_tx,
                            props_changed,
                        ).await?;
                    }
                    Some(name_owner_changed) = name_owner_changed_stream.next() => {
                        self.on_name_owner_changed_message(name_owner_changed)?;
                    }
                    Some(seeked) = seeked_stream.next() => {
                        self.on_seeked_message(player_notif_tx, seeked).await?;
                    }
                    _ = resync.tick() => {
                        self.sample_position(&props, &player_iface, player_notif_tx).await?;
                    },
                    else => {
                        info!(player = %self.bus_name, "dbus streams terminated");
                        break;
                    }
                }
            } else {
                // not playing => don't observe ticks
                tokio::select! {
                    Some(props_changed) = props_changed_stream.next() => {
                        self.on_props_changed_message(
                            &player_iface,
                            player_notif_tx,
                            props_changed,
                        ).await?;
                    }
                    Some(name_owner_changed) = name_owner_changed_stream.next() => {
                        self.on_name_owner_changed_message(name_owner_changed)?;
                    }
                    Some(seeked) = seeked_stream.next() => {
                        self.on_seeked_message(player_notif_tx, seeked).await?;
                    }
                    else => {
                        info!(player = %self.bus_name, "dbus streams terminated");
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    async fn sample_position<'a>(
        &mut self,
        props: &'a PropertiesProxy<'a>,
        player_iface: &'a InterfaceName<'a>,
        player_notif_tx: &mpsc::Sender<PlayerNotification>,
    ) -> Result<(), MprisError> {
        let now = Instant::now();
        let Ok(val) = props.get(player_iface.clone(), "Position").await else {
            info!(player = %self.bus_name, "couldn't get Position");
            return Ok(());
        };
        let Ok(pos) = i64::try_from(val) else {
            error!(player = %self.bus_name, "unexpected type for Position");
            return Ok(());
        };

        debug!(player = %self.bus_name,
            old = %self.data.base_position_us, new = %pos,
            "sampled pos",
        );
        self.data.base_position_us = pos
            .try_into()
            .inspect_err(|_| {
                error!(%pos, "sampled negative position");
            })
            .unwrap_or_default();
        self.data.base_position_instant = now;

        player_notif_tx
            .send(PlayerNotification::BasePosition {
                bus_name: self.bus_name.clone(),
                position_us: self.data.base_position_us,
                instant: self.data.base_position_instant,
            })
            .await?;

        Ok(())
    }
}

#[derive(Debug)]
struct ActivePlayer {
    bus_name: Arc<BusName<'static>>,
    state: PlaybackState,
}

#[derive(Debug)]
pub struct PlayerAggregator {
    last_active: Option<ActivePlayer>,
    player_notif_rx: mpsc::Receiver<PlayerNotification>,
    display_cmd_tx: mpsc::Sender<DisplayCmd>,
}

impl PlayerAggregator {
    pub fn new(
        player_notif_rx: mpsc::Receiver<PlayerNotification>,
        display_cmd_tx: mpsc::Sender<DisplayCmd>,
    ) -> Self {
        PlayerAggregator {
            last_active: None,
            player_notif_rx,
            display_cmd_tx,
        }
    }

    pub async fn into_task(mut self) {
        while let Some(notif) = self.player_notif_rx.recv().await {
            match notif {
                PlayerNotification::Update {
                    bus_name,
                    state,
                    data,
                } => {
                    trace!(player = %bus_name, ?state, ?data, "update notif");

                    let active_player = self.last_active.get_or_insert_with(|| ActivePlayer {
                        bus_name: bus_name.clone(),
                        state,
                    });

                    if active_player.bus_name != bus_name {
                        use PlaybackState::*;
                        match (active_player.state, state) {
                            (Playing, _) => continue,
                            (_, Playing) => (),
                            (Stopped, Paused) => (),
                            _ => continue,
                        }

                        self.last_active = Some(ActivePlayer { bus_name, state });
                    }

                    if self
                        .display_cmd_tx
                        .send(DisplayCmd::Update { state, data })
                        .await
                        .is_err()
                    {
                        error!("Display channel closed");
                        break;
                    }
                }
                PlayerNotification::BasePosition {
                    bus_name,
                    position_us,
                    instant,
                } => {
                    trace!(player = %bus_name, %position_us, ?instant, "position notif");

                    let Some(ref active_player) = self.last_active else {
                        info!(player = %bus_name, "aggregator got position notif but no active player");
                        continue;
                    };

                    if active_player.bus_name != bus_name {
                        continue;
                    }

                    if self
                        .display_cmd_tx
                        .send(DisplayCmd::BasePosition {
                            position_us,
                            instant,
                        })
                        .await
                        .is_err()
                    {
                        error!("Display channel closed");
                        break;
                    }
                }
            }
        }
    }
}
