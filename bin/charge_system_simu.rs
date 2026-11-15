use chrono::{TimeDelta, Utc};
use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};
use tracing::{debug, error, info, warn};

use std::collections::VecDeque;
use std::error::Error;
use std::io;
use std::path::Path;
use std::time::Duration;

use raspdac_display::charge_system::{
    AppliedChargingPlan, Bms, ChargeStatus, ChargeSystemConnectionState, ChargingPlan,
    Notification, UNIX_SOCKET_PATH, bms, plan_model::ChargingPeriod,
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
        let mut bms = Bms::builder(48_100)
            .initial_energy(1000)
            .initial_soc(0.35)
            .current_energy(1000)
            .target_soc(0.50)
            .build();

        let added_energy_by_interval = (bms::DEFAULT_CAPACITY as f32
            * (bms.target_soc().unwrap() - bms.initial_soc().unwrap())
            / 4.0) as u64;
        let mut energy = bms.initial_energy().unwrap();

        let mut now = Utc::now();
        let bms_interval = TimeDelta::seconds(5);

        VecDeque::from_iter(arg1.split(',').map(|part| match part {
            "plan" => Notification::ChargingPlan(AppliedChargingPlan {
                plan: ChargingPlan::Asap {
                    target_soc: 0.50,
                    power_limit: bms::MAX_POWER,
                },
                periods: vec![ChargingPeriod {
                    start: now.into(),
                    duration_s: (4 + 3) * 5,
                    power_limit: bms::MAX_POWER,
                }],
            }),
            "bms_update" => Notification::BmsUpdate {
                ts: now.into(),
                bms: bms.clone(),
            },
            "charging" => Notification::Status {
                ts: now.into(),
                status: ChargeStatus::Charging,
            },
            "bms_update1" => {
                energy += added_energy_by_interval;
                bms.update_energy(energy);
                now += bms_interval;

                Notification::BmsUpdate {
                    ts: now.into(),
                    bms: bms.clone(),
                }
            }
            "bms_update2" => {
                energy += added_energy_by_interval;
                bms.update_energy(energy);
                now += bms_interval;

                Notification::BmsUpdate {
                    ts: now.into(),
                    bms: bms.clone(),
                }
            }
            "bms_update3" => {
                energy += added_energy_by_interval;
                bms.update_energy(energy);
                now += bms_interval;

                Notification::BmsUpdate {
                    ts: now.into(),
                    bms: bms.clone(),
                }
            }
            "sevse" => Notification::Status {
                ts: now.into(),
                status: ChargeStatus::SuspendedEvse,
            },
            "sev" => Notification::Status {
                ts: now.into(),
                status: ChargeStatus::SuspendedEv,
            },
            "suser" => Notification::Status {
                ts: now.into(),
                status: ChargeStatus::StoppedByUser,
            },
            "available" => Notification::Status {
                ts: now.into(),
                status: ChargeStatus::Available,
            },
            "preparing" => Notification::Status {
                ts: now.into(),
                status: ChargeStatus::Preparing,
            },
            "charge_error" => Notification::Status {
                ts: now.into(),
                status: ChargeStatus::Error,
            },
            "error" => Notification::Connection {
                ts: now.into(),
                state: ChargeSystemConnectionState::Error,
            },
            "heartbeat" => Notification::Connection {
                ts: now.into(),
                state: ChargeSystemConnectionState::Heartbeat,
            },
            "mheartbeat" => Notification::Connection {
                ts: now.into(),
                state: ChargeSystemConnectionState::MissedHeartbeat,
            },
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
