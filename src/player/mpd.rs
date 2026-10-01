use mpd_client::{
    Client,
    client::{ConnectionEvent, Subsystem},
    commands,
    responses::PlayState,
};
use tokio::net::UnixStream;
use tokio::sync::{broadcast, mpsc};
use tokio::time;
use tracing::{debug, error, info, warn};

use std::time::{Duration, Instant};

use super::{
    DEFAULT_TITLE, NamedPlayerNotification, PlaybackState, PlayerData, PlayerNotification,
};

const PLAYER_NAME: &str = "mpd";

cfg_select! {
    all(target_arch = "aarch64", target_os = "linux") => {
        // expecting we target the raspberry pi
        const SOCKET_PATH: &str = "/var/run/mpd/socket";
    }
    _ => {
        const SOCKET_PATH: &str = "/run/user/1000/mpd/socket";
    }
}

const RECONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RESAMPLE_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum MpdError {
    #[error("MPD error: {0}")]
    Mpd(#[from] mpd_client::client::CommandError),
    #[error("MPD protocol error: {0}")]
    MpdProtocol(#[from] mpd_client::protocol::MpdProtocolError),
    #[error("channel error: {0}")]
    Channel(#[from] tokio::sync::mpsc::error::SendError<NamedPlayerNotification>),
}

impl From<PlayState> for PlaybackState {
    fn from(state: PlayState) -> Self {
        match state {
            PlayState::Playing => PlaybackState::Playing,
            PlayState::Paused => PlaybackState::Paused,
            PlayState::Stopped => PlaybackState::Stopped,
        }
    }
}

#[derive(Debug)]
pub struct MpdPlayer {
    player_notif_tx: mpsc::Sender<NamedPlayerNotification>,
    state: PlaybackState,
    data: PlayerData,
}

impl MpdPlayer {
    pub fn new(player_notif_tx: mpsc::Sender<NamedPlayerNotification>) -> Self {
        MpdPlayer {
            player_notif_tx,
            state: PlaybackState::Stopped,
            data: Default::default(),
        }
    }

    async fn connect(&mut self) -> Result<(), MpdError> {
        loop {
            match UnixStream::connect(SOCKET_PATH).await {
                Ok(stream) => match self.listen(stream).await {
                    Ok(()) => info!("left the socket"),
                    Err(err) => warn!(%err, "listener"),
                },
                Err(err) => warn!(%err, "connecting"),
            }

            self.clear();
            self.player_notif_tx
                .send(NamedPlayerNotification {
                    player_name: PLAYER_NAME,
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

    async fn listen(&mut self, stream: UnixStream) -> Result<(), MpdError> {
        let (mut client, mut state_changes) = Client::connect(stream).await?;
        info!("connected");

        self.on_changed(&mut client).await?;

        loop {
            if self.state == PlaybackState::Playing {
                tokio::select! {
                    biased;

                    state_changes_res = state_changes.next() => {
                        match state_changes_res {
                            Some(ConnectionEvent::SubsystemChange(Subsystem::Player)) => {
                                self.on_changed(&mut client).await?;
                            }
                            Some(ConnectionEvent::SubsystemChange(_)) => {
                                // something changed but we don't care (Option or Playlist)
                                continue;
                            }
                            _ => {
                                // connection was closed by the server
                                Err(mpd_client::protocol::MpdProtocolError::Io(
                                    std::io::ErrorKind::ConnectionAborted.into(),
                                ))?;
                            }
                        }
                    }

                    _ = time::sleep(Instant::now() + RESAMPLE_INTERVAL - self.data.base_position_instant) => {
                        self.update_status(&mut client).await?;

                        self.player_notif_tx
                            .send(NamedPlayerNotification {
                                player_name: PLAYER_NAME,
                                notif: PlayerNotification::BasePosition {
                                    position_us: self.data.base_position_us,
                                    instant: self.data.base_position_instant,
                                }
                            })
                            .await?;
                    }
                }
            } else {
                match state_changes.next().await {
                    Some(ConnectionEvent::SubsystemChange(Subsystem::Player)) => {
                        self.on_changed(&mut client).await?;
                    }
                    Some(ConnectionEvent::SubsystemChange(_)) => {
                        // something changed but we don't care (Option or Playlist)
                        continue;
                    }
                    _ => {
                        // connection was closed by the server
                        Err(mpd_client::protocol::MpdProtocolError::Io(
                            std::io::ErrorKind::ConnectionAborted.into(),
                        ))?;
                    }
                }
            }
        }
    }

    async fn on_changed(&mut self, client: &mut Client) -> Result<(), MpdError> {
        let Some(song_in_queue) = client.command(commands::CurrentSong).await? else {
            return Ok(());
        };

        let title = song_in_queue.song.title().unwrap_or(DEFAULT_TITLE);
        if self.data.title.as_ref() != title {
            debug!(old_title = %self.data.title, new = %title);
            self.data.title = title.into();
        }

        self.update_status(client).await?;

        self.player_notif_tx
            .send(NamedPlayerNotification {
                player_name: PLAYER_NAME,
                notif: PlayerNotification::Update {
                    state: self.state,
                    data: self.data.clone(),
                },
            })
            .await?;

        Ok(())
    }

    async fn update_status(&mut self, client: &mut Client) -> Result<(), MpdError> {
        let now = Instant::now();

        let status = client.command(commands::Status).await?;
        let state = status.state.into();
        if self.state != state {
            debug!(old_state = ?self.state, new = ?state);
            self.state = state;
        }

        self.data.base_position_us = status.elapsed.map_or(0, |t| t.as_micros() as u64);
        self.data.base_position_instant = now;

        debug!(position = %self.data.base_position_us, "sampled");

        self.data.duration_us = status.duration.map_or(0, |t| t.as_micros() as u64);

        Ok(())
    }

    fn clear(&mut self) {
        self.state = PlaybackState::Stopped;
        self.data.clear();
    }

    pub async fn into_task(mut self, mut stop_rx: broadcast::Receiver<()>) {
        loop {
            tokio::select! {
                biased;
                _  = stop_rx.recv() => {
                    info!("shutting down due to stop request");
                    break;
                }
                _ = self.connect() => (),
            }
        }
    }
}
