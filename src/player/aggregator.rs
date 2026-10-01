use std::ops::ControlFlow;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, trace, warn};

use super::{NamedPlayerNotification, PlaybackState, PlayerNotification};
use crate::display::DisplayCmd;

#[derive(Debug)]
struct ActivePlayer {
    name: &'static str,
    state: PlaybackState,
}

#[derive(Debug)]
pub struct PlayerAggregator {
    last_active: Option<ActivePlayer>,
    player_notif_rx: mpsc::Receiver<NamedPlayerNotification>,
    display_cmd_tx: mpsc::Sender<DisplayCmd>,
}

impl PlayerAggregator {
    pub fn new(
        player_notif_rx: mpsc::Receiver<NamedPlayerNotification>,
        display_cmd_tx: mpsc::Sender<DisplayCmd>,
    ) -> Self {
        PlayerAggregator {
            last_active: None,
            player_notif_rx,
            display_cmd_tx,
        }
    }

    pub async fn into_task(mut self, mut stop_rx: broadcast::Receiver<()>) {
        tokio::select! {
            biased;
            _  = stop_rx.recv() => {
                info!("shutting down due to stop request (aggregator)");
            }
            _ = self.listen() => (),
        }
    }

    async fn listen(&mut self) {
        loop {
            match self.player_notif_rx.recv().await {
                Some(notif) => {
                    if self.handle(notif).await.is_break() {
                        break;
                    }
                }
                None => {
                    warn!("player notif chan terminated");
                    break;
                }
            }
        }
    }

    async fn handle(&mut self, notif: NamedPlayerNotification) -> ControlFlow<()> {
        match notif {
            NamedPlayerNotification {
                player_name,
                notif: PlayerNotification::Update { state, data },
            } => {
                trace!(player = %player_name, ?state, ?data, "update");

                let active_player = self.last_active.get_or_insert(ActivePlayer {
                    name: player_name,
                    state,
                });

                if active_player.name != player_name {
                    use PlaybackState::*;
                    match (active_player.state, state) {
                        (Playing, _) => return ControlFlow::Continue(()),
                        (_, Playing) => (),
                        (Stopped, Paused) => (),
                        _ => return ControlFlow::Continue(()),
                    }

                    info!(old_active_player = %active_player.name, new = %player_name);
                    self.last_active = Some(ActivePlayer {
                        name: player_name,
                        state,
                    });
                } else if active_player.state != state {
                    info!(active_player = %active_player.name, old_state = ?active_player.state, new = ?state);
                    active_player.state = state;
                }

                if self
                    .display_cmd_tx
                    .send(DisplayCmd::Player(PlayerNotification::Update {
                        state,
                        data,
                    }))
                    .await
                    .is_err()
                {
                    error!("Display channel closed");
                    return ControlFlow::Break(());
                }
            }
            NamedPlayerNotification { player_name, notif } => {
                trace!(player = %player_name, ?notif);

                let Some(ref active_player) = self.last_active else {
                    info!(player = %player_name, "aggregator got position notif but no active player");
                    return ControlFlow::Continue(());
                };

                if active_player.name != player_name {
                    return ControlFlow::Continue(());
                }

                if self
                    .display_cmd_tx
                    .send(DisplayCmd::Player(notif))
                    .await
                    .is_err()
                {
                    error!("Display channel closed");
                    return ControlFlow::Break(());
                }
            }
        }

        ControlFlow::Continue(())
    }
}
