use charge_system_common::bms::SoC;
use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, error, info, warn};

use std::collections::VecDeque;
use std::error::Error;
use std::io;
use std::path::Path;
use std::time::Duration;

use raspdac_display::charge_system::{
    Bms, ChargeProgress, ChargeStatus, ChargeSystemConnectionState, Notification, UNIX_SOCKET_PATH,
};

async fn run(listener: UnixListener, mut msgs: Option<VecDeque<Notification>>) -> io::Result<()> {
    loop {
        let (stream, _) = listener
            .accept()
            .await
            .inspect_err(|err| error!(%err, "accepting socket"))?;

        info!("accepted");

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
    msgs: &mut Option<VecDeque<Notification>>,
) -> Result<(), Box<dyn Error>> {
    let mut buf = vec![0; 1024];

    loop {
        stream.writable().await?;

        let Some(msgs) = msgs.as_mut() else {
            info!("no msgs defined => just wait");
            std::future::pending::<()>().await;
            break;
        };

        let msg = msgs.pop_front();
        info!(?msg);

        buf.clear();
        serde_json::to_writer(&mut buf, &msg)?;

        if let Err(err) = stream.write(buf.as_slice()).await {
            warn!(%err, "writing to socket");
            Err(err)?;
        }

        debug!("written");

        tokio::time::sleep(Duration::from_secs(5)).await;

        if msgs.is_empty() {
            info!("no more messages => waiting for SIGINT");
            std::future::pending::<()>().await;
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let msgs = std::env::args().nth(1).map(|arg1| {
        VecDeque::from_iter(arg1.split(',').map(|part| match part {
            "bms_update" => {
                Notification::BmsUpdate(Bms::builder(48_100).target_soc(0.50).build().unwrap())
            }
            "charging" => Notification::Status(ChargeStatus::Charging(ChargeProgress {
                soc: SoC::new_absolute(0.30),
                remaining_time: 120,
            })),
            "charging2" => Notification::Status(ChargeStatus::Charging(ChargeProgress {
                soc: SoC::new_absolute(0.306),
                remaining_time: 110,
            })),
            "charging3" => Notification::Status(ChargeStatus::Charging(ChargeProgress {
                soc: SoC::new_absolute(0.311),
                remaining_time: 100,
            })),
            "sevse" => Notification::Status(ChargeStatus::SuspendedEvse(ChargeProgress {
                soc: SoC::new_absolute(0.49),
                remaining_time: 0,
            })),
            "sev" => Notification::Status(ChargeStatus::SuspendedEv),
            "suser" => Notification::Status(ChargeStatus::StoppedByUser),
            "available" => Notification::Status(ChargeStatus::Available),
            "preparing" => Notification::Status(ChargeStatus::Preparing),
            "charge_error" => Notification::Status(ChargeStatus::Error),
            "error" => Notification::Connection(ChargeSystemConnectionState::Error),
            "heartbeat" => Notification::Connection(ChargeSystemConnectionState::Heartbeat),
            "mheartbeat" => Notification::Connection(ChargeSystemConnectionState::MissedHeartbeat),
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
