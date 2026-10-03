use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, error, info, warn};

use std::collections::VecDeque;
use std::error::Error;
use std::io;
use std::path::Path;
use std::time::Duration;

use raspdac_display::{ChargePointNotification, ChargeProgress, ChargeState, UNIX_SOCKET_PATH};

async fn run(
    listener: UnixListener,
    mut msgs: Option<VecDeque<ChargePointNotification>>,
) -> io::Result<()> {
    loop {
        let (stream, addr) = listener
            .accept()
            .await
            .inspect_err(|err| error!(%err, "connecting socket"))?;

        info!(?addr, "accepted");

        if let Err(err) = notifier(stream, &mut msgs).await {
            warn!(%err, "notifier");
        }

        if msgs.as_mut().is_none_or(|m| m.is_empty()) {
            break;
        }
    }

    Ok(())
}

async fn notifier(
    mut stream: UnixStream,
    msgs: &mut Option<VecDeque<ChargePointNotification>>,
) -> Result<(), Box<dyn Error>> {
    let mut buf = vec![0; 1024];

    loop {
        stream.writable().await?;

        let Some(msgs) = msgs.as_mut() else {
            info!("no msgs defined => just wait");
            std::future::pending::<()>().await;
            break;
        };

        buf.clear();
        serde_json::to_writer(&mut buf, &msgs.pop_front())?;

        if let Err(err) = stream.write(buf.as_slice()).await {
            warn!(%err, "writing to socket");
            Err(err)?;
        }

        debug!("written");

        tokio::time::sleep(Duration::from_secs(5)).await;

        if msgs.is_empty() {
            info!("no more messages => disconnecting");
            break;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let msgs = std::env::args().nth(1).map(|arg1| {
        VecDeque::from_iter(arg1.split(',').map(|part| match part {
            "charging" => ChargePointNotification::Charge(ChargeState::Charging(ChargeProgress {
                soc: 30,
                target_soc: 50,
                seconds_left: 120,
            })),
            "sevse" => {
                ChargePointNotification::Charge(ChargeState::SuspendedEvse(ChargeProgress {
                    soc: 49,
                    target_soc: 50,
                    seconds_left: 0,
                }))
            }
            "sev" => ChargePointNotification::Charge(ChargeState::SuspendedEv),
            "suser" => ChargePointNotification::Charge(ChargeState::StoppedByUser),
            "available" => ChargePointNotification::Charge(ChargeState::Available),
            "preparing" => ChargePointNotification::Charge(ChargeState::Preparing),
            "charge_error" => ChargePointNotification::Charge(ChargeState::Error),
            "error" => ChargePointNotification::Error,
            "heartbeat" => ChargePointNotification::HeartBeat,
            "mheartbeat" => ChargePointNotification::MissingHeartBeat,
            other => panic!("unknown {other}"),
        }))
    });

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let socket_path = Path::new(UNIX_SOCKET_PATH);
    // not: this should be done using a ocpp-server.socket
    struct SocketGuard<'a>(&'a Path);
    impl<'a> SocketGuard<'a> {
        fn new(socket_path: &'a Path) -> Self {
            info!(path = ?socket_path, "unix socket");

            if socket_path.exists() {
                let _ = std::fs::remove_file(socket_path)
                    .inspect_err(|err| error!(%err, "removing socket"));
            }
            SocketGuard(socket_path)
        }
    }

    impl<'a> Drop for SocketGuard<'a> {
        fn drop(&mut self) {
            if self.0.exists() {
                let _ =
                    std::fs::remove_file(self.0).inspect_err(|err| error!(%err, "removing socket"));
            }
        }
    }
    let _guard = SocketGuard::new(socket_path);

    let listener =
        UnixListener::bind(socket_path).inspect_err(|err| error!(%err, "binding socket"))?;

    let listener_hdl = tokio::spawn(run(listener, msgs));

    info!("listener running");
    tokio::signal::ctrl_c().await?;

    listener_hdl.abort();

    Ok(())
}
