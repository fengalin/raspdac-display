pub const UNIX_SOCKET_PATH: &str = "/run/charge_point/socket";

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ChargePointNotification {
    Charge(ChargeState),
    Connection(ChargePointConnectionState),
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ChargeProgress {
    pub soc: u8,
    pub target_soc: u8,
    pub seconds_left: u16,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ChargeState {
    Available,
    Preparing,
    Charging(ChargeProgress),
    SuspendedEvse(ChargeProgress),
    SuspendedEv,
    StoppedByUser,
    Finishing,
    UnknownSession,
    Error,
}

#[derive(Debug, Copy, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ChargePointConnectionState {
    Heartbeat,
    MissedHeartbeat,
    ServerDisconnected,
    Error,
}
