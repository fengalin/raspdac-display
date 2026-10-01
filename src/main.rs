//! RaspDAC Display Service: monitors MPRIS players and drives a Winstar 2x16 HD44780-compatible OLED.

use tokio::sync::{broadcast, mpsc};
use tracing::info;

mod config;
mod display;
use display::Display;

cfg_select! {
    feature = "simu" => {
        mod simu;
        pub use simu::*;
    }
    _ => {
        mod driver;
        pub use driver::*;
    }
}

mod charge_point;
mod charge_point_notif;
use charge_point::ChargePointListener;

mod player;
use player::*;

mod scroll;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = config::Config::default();

    let (stop_tx, stop_rx) = broadcast::channel(8);

    let (display_cmd_tx, display_cmd_rx) = mpsc::channel(8);
    let display = Display::new(&config, display_cmd_rx).await?;

    let (player_update_tx, player_update_rx) = mpsc::channel(8);
    let agg = PlayerAggregator::new(player_update_rx, display_cmd_tx.clone());
    let pibuz = MprisPlayer::new("pibuz", player_update_tx.clone());
    // let mpd = MpdPlayer::new(player_update_tx);

    let mut task_handles = vec![
        tokio::spawn(display.into_task(stop_rx)),
        tokio::spawn(agg.into_task(stop_tx.subscribe())),
        tokio::spawn(pibuz.into_task(stop_tx.subscribe())),
        // tokio::spawn(mpd.into_task(stop_tx.subscribe())),
    ];

    let charge_point_listener = ChargePointListener::new(display_cmd_tx);
    task_handles.push(tokio::spawn(
        charge_point_listener.into_task(stop_tx.subscribe()),
    ));

    info!("RaspDAC Display Service started");
    tokio::signal::ctrl_c().await?;

    info!("Shutting down...");

    let _ = stop_tx.send(());

    for handle in task_handles {
        let _ = handle.await;
    }

    info!("Shut down complete.");
    Ok(())
}
