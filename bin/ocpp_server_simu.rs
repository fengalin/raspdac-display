use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, error, info, warn};

use std::error::Error;
use std::io;
use std::path::Path;

use raspdac_display::{ChargeProgress, ChargeState, UNIX_SOCKET_PATH};

async fn run(listener: UnixListener) -> io::Result<()> {
    loop {
        let (stream, addr) = listener
            .accept()
            .await
            .inspect_err(|err| error!(%err, "connecting socket"))?;

        info!(?addr, "accepted");

        if let Err(err) = notifier(stream).await {
            warn!(%err, "notifier");
        }
    }
}

async fn notifier(mut stream: UnixStream) -> Result<(), Box<dyn Error>> {
    let mut buf = vec![0; 1024];
    stream.writable().await?;

    let arg1 = std::env::args().nth(1);
    let msg = match arg1.as_deref() {
        Some("charging") | None => ChargeState::Charging(ChargeProgress {
            soc: 30,
            target_soc: 50,
            seconds_left: 120,
        }),
        Some("sevse") => ChargeState::SuspendedEvse(ChargeProgress {
            soc: 49,
            target_soc: 50,
            seconds_left: 0,
        }),
        Some("sev") => ChargeState::SuspendedEv,
        Some("suser") => ChargeState::StoppedByUser,
        Some("available") => ChargeState::Available,
        Some("preparing") => ChargeState::Preparing,
        Some("error") => ChargeState::Error,
        Some(other) => panic!("unknown {other}"),
    };
    buf.clear();
    serde_json::to_writer(&mut buf, &msg)?;

    if let Err(err) = stream.write(buf.as_slice()).await {
        warn!(%err, "writing to socket");
        Err(err)?;
    }

    debug!("written");
    std::future::pending::<()>().await;

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

    let listener_hdl = tokio::spawn(run(listener));

    info!("listener running");
    tokio::signal::ctrl_c().await?;

    listener_hdl.abort();

    Ok(())
}
