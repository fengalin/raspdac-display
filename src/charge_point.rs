use tokio::io::AsyncReadExt;
use tokio::net::UnixStream;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, trace, warn};

use std::io;
use std::path::Path;
use std::time::{Duration, Instant};

use crate::charge_point_notif::{ChargeState, UNIX_SOCKET_PATH};
use crate::display::DisplayCmd;

const RECONNECT_TIMEOUT: Duration = Duration::from_secs(5);

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
pub struct ChargePointListener {
    display_cmd_tx: mpsc::Sender<DisplayCmd>,
}

impl ChargePointListener {
    pub fn new(display_cmd_tx: mpsc::Sender<DisplayCmd>) -> Self {
        ChargePointListener { display_cmd_tx }
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
        loop {
            match UnixStream::connect(&socket_path).await {
                Ok(stream) => {
                    info!("connected");
                    if let Err(err) = self.handler(stream).await {
                        warn!(%err, "handler");
                    }
                }
                Err(err) => trace!(%err, socket = ?socket_path, "connecting"),
            }

            tokio::time::sleep(RECONNECT_TIMEOUT).await;
        }
    }

    async fn handler(&mut self, mut stream: UnixStream) -> Result<(), ChargePointError> {
        let mut buf = [0; 1024];
        loop {
            stream.readable().await?;

            let n = stream.read(&mut buf).await?;
            if n == 0 {
                return Err(ChargePointError::SocketTerminated);
            }

            let data = &buf[..n];
            trace!(%n, ?data, "read");
            let Ok(state) = serde_json::from_slice::<ChargeState>(data) else {
                warn!("error deserializing message");
                continue;
            };
            debug!(?state);

            self.display_cmd_tx
                .send(DisplayCmd::ChargePoint {
                    state,
                    instant: Instant::now(),
                })
                .await?;
        }
    }
}
