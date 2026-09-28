//! RaspDAC Display Service: monitors MPRIS players and drives a Winstar 2x16 HD44780-compatible OLED.

use tokio::sync::mpsc;
use tracing::info;

mod config;
mod display;
mod driver;
mod mpris;
mod scroll;

use display::{Display, DisplayCmd};
use mpris::{Player, PlayerAggregator};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config = config::Config::default();

    let (display_cmd_tx, display_cmd_rx) = mpsc::channel(8);

    let display = Display::new(&config).await?;
    let display_handle = tokio::spawn(display.into_task(display_cmd_rx));

    let (player_update_tx, player_update_rx) = mpsc::channel(8);

    let agg = PlayerAggregator::new(player_update_rx, display_cmd_tx.clone());
    let pibuz = Player::new(config.mpris.pibuz_bus)?;
    let mpd = Player::new(config.mpris.mpd_bus)?;

    let mpris_handles = vec![
        tokio::spawn(agg.into_task()),
        tokio::spawn(pibuz.into_task(player_update_tx.clone())),
        tokio::spawn(mpd.into_task(player_update_tx)),
    ];

    info!("RaspDAC Display Service started");
    tokio::signal::ctrl_c().await?;

    info!("Shutting down...");

    let _ = display_cmd_tx.send(DisplayCmd::Stop).await;

    for handle in mpris_handles {
        handle.abort();
    }

    let _ = display_handle.await;

    info!("Shut down complete.");
    Ok(())
}
