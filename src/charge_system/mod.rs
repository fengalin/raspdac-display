use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::sync::{broadcast, mpsc};
use tokio::time;
use tracing::{debug, error, info, trace, warn};

use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use crate::display::DisplayCmd;

pub use charge_system_common::{
    bms::Bms,
    notification::{
        ChargePointConnectionState, ChargeProgress, ChargeStatus, Notification, UNIX_SOCKET_PATH,
    },
    plan_model::{AppliedChargingPlan, ChargingPlan},
};

const RECONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_mins(cfg_select! {
    // expecting we target the raspberry pi
    all(target_arch = "aarch64", target_os = "linux") => 70,
    // simu
    _ => 1,
});

const MAX_MESSAGE_SIZE: usize = 512;

#[derive(Debug, thiserror::Error)]
pub enum ChargePointError {
    #[error("deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("socket terminated")]
    SocketTerminated,
    #[error("display chan error: {0}")]
    Channel(#[from] tokio::sync::mpsc::error::SendError<DisplayCmd>),
}

#[derive(Debug)]
pub struct ChargeSystemListener {
    display_cmd_tx: mpsc::Sender<DisplayCmd>,
}

impl ChargeSystemListener {
    pub fn new(display_cmd_tx: mpsc::Sender<DisplayCmd>) -> Self {
        ChargeSystemListener { display_cmd_tx }
    }

    pub async fn into_task(mut self, mut stop_rx: broadcast::Receiver<()>) {
        tokio::select! {
            biased;
            _  = stop_rx.recv() => {
                info!("shutting down due to stop request");
            }
            _ = self.listen() => (),
        }
    }

    async fn listen(&mut self) {
        let socket_path = Path::new(UNIX_SOCKET_PATH);
        let mut log_connection_failure = true;
        loop {
            match UnixStream::connect(&socket_path).await {
                Ok(stream) => {
                    info!("connected");
                    log_connection_failure = true;
                    if let Err(err) = self.handler(stream).await {
                        warn!(%err, "handler");
                    }
                }
                Err(err) => {
                    if log_connection_failure {
                        warn!(%err, socket = ?socket_path, "connecting");
                        if let Err(err) = self
                            .display_cmd_tx
                            .send(DisplayCmd::ChargeSystem {
                                notification: Notification::Connection(
                                    ChargePointConnectionState::ServerDisconnected,
                                ),
                                instant: Instant::now(),
                            })
                            .await
                        {
                            error!(%err, "display chan");
                        };
                        log_connection_failure = false;
                    }
                }
            }

            time::sleep(RECONNECT_TIMEOUT).await;
        }
    }

    async fn handler(&mut self, mut stream: UnixStream) -> Result<(), ChargePointError> {
        let mut buf = [0; MAX_MESSAGE_SIZE];
        let mut last_checkpoint_instant = Instant::now();

        loop {
            tokio::select! {
                biased;
                readable_rs = stream.readable() => {
                    readable_rs?;

                    let now = Instant::now();
                    last_checkpoint_instant = now;

                    let n = stream.read(&mut buf).await?;
                    if n == 0 {
                        return Err(ChargePointError::SocketTerminated);
                    }

                    let data = &buf[..n];
                    trace!(%n, ?data, "read");
                    let Ok(notif) = serde_json::from_slice::<Notification>(data).inspect_err(|err| {
                        warn!(%err, "error deserializing message");
                    }) else {
                        continue;
                    };

                    debug!(?notif);

                    // filter which notification we are interested in
                    match &notif {
                        Notification::MeterValues(_) => continue,
                        Notification::BmsUpdate(_) |
                        Notification::Connection(_) |
                        Notification::Status(_) |
                        Notification::ChargingPlan(_) => (),
                    }

                    self.display_cmd_tx
                        .send(DisplayCmd::ChargeSystem {
                            notification: notif,
                            instant: now,
                        })
                        .await?;
                }
                _ = time::sleep(HEARTBEAT_TIMEOUT.saturating_sub(last_checkpoint_instant.elapsed())) => {
                    warn!("heartbeat timeout");

                    let now = Instant::now();
                    last_checkpoint_instant = now;

                    self.display_cmd_tx
                        .send(DisplayCmd::ChargeSystem {
                            notification: Notification::Connection(ChargePointConnectionState::MissedHeartbeat),
                            instant: now,
                        })
                        .await?;
                }
            }
        }
    }
}
