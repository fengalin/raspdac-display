//! Signal-based MPRIS media player listeners.
//!
//! Each configured player (MPD, pibuz) is tracked on the session D-Bus:
//! - `NameOwnerChanged` detects the player appearing on / leaving the bus.
//! - `PropertiesChanged` (interface `org.mpris.MediaPlayer2.Player`) drives
//!   updates for playback status, metadata, rate, and position.
//! - The `Seeked` signal triggers an immediate position resync.
//! - The `Position` property is resampled periodically, because the MPRIS
//!   specification does not emit `PropertiesChanged` for it.
use std::time::{Duration, Instant};

use futures_lite::stream::StreamExt;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};
use zbus::Message;
use zbus::fdo::{DBusProxy, NameOwnerChanged, PropertiesChanged, PropertiesProxy};
use zbus::names::{InterfaceName, OwnedBusName};
use zbus::zvariant::Value;

use crate::{NamedPlayerNotification, PlaybackState, PlayerData, PlayerNotification};

const OBJECT_PATH: &str = "/org/mpris/MediaPlayer2";
const PLAYER_IFACE: &str = "org.mpris.MediaPlayer2.Player";
/// Interval between `Position` property resamples (drift correction).
const POSITION_RESYNC: Duration = Duration::from_secs(30);

/// Delay before reconnecting after the listener loop ends.
const RECONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, thiserror::Error)]
pub enum MprisError {
    #[error("D-Bus error: {0}")]
    Dbus(#[from] zbus::Error),
    #[error("D-Bus error: {0}")]
    Fdo(#[from] zbus::fdo::Error),
    #[error("invalid bus name: {0}")]
    BusName(#[from] zbus::names::Error),
    #[error("channel error: {0}")]
    Channel(#[from] tokio::sync::mpsc::error::SendError<NamedPlayerNotification>),
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

/// Per-player state update sent to the aggregator.
#[derive(Debug)]
pub struct MprisPlayer {
    name: &'static str,
    player_notif_tx: mpsc::Sender<NamedPlayerNotification>,
    state: PlaybackState,
    data: PlayerData,
}

impl MprisPlayer {
    pub fn new(name: &'static str, player_notif_tx: mpsc::Sender<NamedPlayerNotification>) -> Self {
        MprisPlayer {
            name,
            player_notif_tx,
            state: PlaybackState::Stopped,
            data: Default::default(),
        }
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
                        debug!(player = %self.name,
                            old = ?self.state, new = ?new_state,
                            "new status",
                        );
                        self.state = new_state;
                        if new_state == PlaybackState::Playing {
                            self.data.base_position_instant = Instant::now();
                        }
                        must_notify = true;
                    }
                }
                "Metadata" => {
                    let Value::Dict(metadata) = val else {
                        panic!("unexpected type for metadata");
                    };
                    let title = metadata
                        .get::<_, &str>(&"xesam:title")
                        .inspect_err(|err| {
                            error!(player = %self.name, %err, "Metadata title");
                        })
                        .ok()
                        .flatten()
                        .unwrap_or_default();
                    if self.data.title.as_ref() != title {
                        debug!(player = %self.name,
                            old = %self.data.title, new = %title,
                            "new title",
                        );
                        self.data.title = title.into();
                        must_notify = true;
                    }

                    let duration: u64 = metadata
                        .get::<_, i64>(&"mpris:length")
                        .inspect_err(|err| {
                            error!(player = %self.name, %err, "Metadata length");
                        })
                        .ok()
                        .flatten()
                        .unwrap_or_default()
                        .try_into()
                        .inspect_err(|err| {
                            error!(player = %self.name, %err, "Metadata negative length");
                        })
                        .ok()
                        .unwrap_or_default();
                    if self.data.duration_us != duration {
                        debug!(player = %self.name,
                            old = %self.data.duration_us, new = %duration,
                            "new duration",
                        );
                        self.data.duration_us = duration;
                        must_notify = true;
                    }
                }
                "TrackId" => {
                    debug!(player = %self.name, new = ?val, "new track id");
                    // FIXME
                    // must_notify = true
                }
                "Position" => {
                    // note: position is only set on initial props.get_all()
                    match i64::try_from(val) {
                        Ok(pos) => {
                            debug!(player = %self.name, new = %pos, "new pos");
                            self.data.base_position_us = pos
                                .try_into()
                                .inspect_err(|err| {
                                    error!(player = %self.name, %err, "negative Position");
                                })
                                .unwrap_or_default();
                            self.data.base_position_instant = Instant::now();
                        }
                        Err(err) => {
                            error!(player = %self.name, %err, "type mismatch reading Position");
                        }
                    }
                }
                "Rate" => {
                    self.data.rate = val
                        .try_into()
                        .inspect_err(|err| {
                            error!(player = %self.name, %err, "Rate");
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
        props: &'a PropertiesProxy<'a>,
        player_iface: &'a InterfaceName<'a>,
        props_changed: PropertiesChanged,
    ) -> Result<(), MprisError> {
        let args = props_changed.args()?;
        if args.interface_name() != player_iface {
            debug!(player = ?self.name, iface = ?args.interface_name(), "rejecting msg for other iface");
            return Ok(());
        }
        if self.apply_props(args.changed_properties().iter().map(|(k, v)| (*k, v))) {
            self.sample_position(props, player_iface).await?;

            self.player_notif_tx
                .send(NamedPlayerNotification {
                    player_name: self.name,
                    notif: PlayerNotification::Update {
                        state: self.state,
                        data: self.data.clone(),
                    },
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
        debug!(player = ?self.name, "owner changed");

        Ok(())
    }

    async fn on_seeked_message(&mut self, seeked: Message) -> Result<(), MprisError> {
        if let Ok((pos,)) = seeked
            .body()
            .deserialize::<(u64,)>()
            .inspect(|err| error!(player = %self.name, ?err, "seeked msg deser error"))
        {
            debug!(player = %self.name, %pos, "seeked message");
            self.data.base_position_us = pos;
            self.data.base_position_instant = Instant::now();
            self.player_notif_tx
                .send(NamedPlayerNotification {
                    player_name: self.name,
                    notif: PlayerNotification::BasePosition {
                        position_us: self.data.base_position_us,
                        instant: self.data.base_position_instant,
                    },
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
    pub async fn into_task(mut self, mut stop_rx: broadcast::Receiver<()>) {
        loop {
            tokio::select! {
                biased;
                _  = stop_rx.recv() => {
                    info!(player = %self.name, "shutting down due to stop request");
                    break;
                }
                _ = self.connect() => (),
            }
        }
    }

    async fn connect(&mut self) -> Result<(), MprisError> {
        loop {
            match self.listen().await {
                Ok(()) => info!(player = %self.name, "left the bus"),
                Err(err) => warn!(player = %self.name, %err, "listener"),
            }

            self.clear();
            self.player_notif_tx
                .send(NamedPlayerNotification {
                    player_name: self.name,
                    notif: PlayerNotification::Update {
                        state: self.state,
                        data: self.data.clone(),
                    },
                })
                .await
                .inspect_err(|err| error!(%err, "Player notif channel closed"))?;

            tokio::time::sleep(RECONNECT_TIMEOUT).await;
        }
    }

    /// Watch one player: initial snapshot, then `PropertiesChanged`, `Seeked`,
    /// `NameOwnerChanged`, and periodic `Position` resamples.
    ///
    /// Returns when the player leaves the bus, or on a fatal D-Bus error.
    async fn listen(&mut self) -> Result<(), MprisError> {
        let conn = zbus::Connection::session().await?;
        let player_iface = InterfaceName::try_from(PLAYER_IFACE)?;
        let dbus = DBusProxy::new(&conn).await?;

        let bus_name = OwnedBusName::try_from(format!("org.mpris.MediaPlayer2.{}", self.name))?;
        // Wait for the player to appear on the bus
        if !dbus.name_has_owner(bus_name.as_ref()).await? {
            info!(player = %self.name, "waiting on the session bus...");
            let mut stream = dbus
                .receive_name_owner_changed_with_args(&[(0, bus_name.as_str())])
                .await?;
            while let Some(signal) = stream.next().await {
                if signal.args()?.new_owner().is_some() {
                    break;
                }
            }
        }

        let props = PropertiesProxy::builder(&conn)
            .destination(bus_name.as_ref())?
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

        self.player_notif_tx
            .send(NamedPlayerNotification {
                player_name: self.name,
                notif: PlayerNotification::Update {
                    state: self.state,
                    data: self.data.clone(),
                },
            })
            .await?;

        // Signal streams.
        let mut props_changed_stream = props.receive_properties_changed().await?;
        let mut name_owner_changed_stream = dbus
            .receive_name_owner_changed_with_args(&[(0, bus_name.as_str())])
            .await?;
        let player =
            zbus::proxy::Proxy::new(&conn, bus_name.as_ref(), OBJECT_PATH, PLAYER_IFACE).await?;
        let mut seeked_stream = player.receive_signal("Seeked").await?;

        let mut resync = tokio::time::interval(POSITION_RESYNC);
        resync.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        resync.tick().await; // first tick is immediate; consume it

        loop {
            if self.state == PlaybackState::Playing {
                tokio::select! {
                    Some(props_changed) = props_changed_stream.next() => {
                        self.on_props_changed_message(
                            &props,
                            &player_iface,
                            props_changed,
                        ).await?;
                    }
                    Some(name_owner_changed) = name_owner_changed_stream.next() => {
                        self.on_name_owner_changed_message(name_owner_changed)?;
                    }
                    Some(seeked) = seeked_stream.next() => {
                        self.on_seeked_message(seeked).await?;
                    }
                    _ = resync.tick() => {
                        self.sample_position(&props, &player_iface).await?;
                        self.player_notif_tx
                            .send(NamedPlayerNotification {
                                player_name: self.name,
                                notif: PlayerNotification::BasePosition {
                                    position_us: self.data.base_position_us,
                                    instant: self.data.base_position_instant,
                                }
                            })
                            .await?;
                    },
                    else => {
                        info!(player = %self.name, "dbus streams terminated");
                        break;
                    }
                }
            } else {
                // not playing => don't observe ticks
                tokio::select! {
                    Some(props_changed) = props_changed_stream.next() => {
                        self.on_props_changed_message(
                            &props,
                            &player_iface,
                            props_changed,
                        ).await?;
                    }
                    Some(name_owner_changed) = name_owner_changed_stream.next() => {
                        self.on_name_owner_changed_message(name_owner_changed)?;
                    }
                    Some(seeked) = seeked_stream.next() => {
                        self.on_seeked_message(seeked).await?;
                    }
                    else => {
                        info!(player = %self.name, "dbus streams terminated");
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
    ) -> Result<(), MprisError> {
        let now = Instant::now();
        let Ok(val) = props.get(player_iface.clone(), "Position").await else {
            info!(player = %self.name, "couldn't get Position");
            return Ok(());
        };
        let Ok(pos) = i64::try_from(val) else {
            error!(player = %self.name, "unexpected type for Position");
            return Ok(());
        };

        debug!(player = %self.name,
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

        Ok(())
    }
}
