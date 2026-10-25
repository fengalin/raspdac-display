pub const UNIX_SOCKET_PATH: &str = "/run/charge_point/socket";

// FIXME do we want to export this to the interface?
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum ChargePointNotification {
    Charge(ChargeState),
    Connection(ChargePointConnectionState),
    // FIXME
    MeterValues(Sample),
}

// FIXME this is from evce
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Sample {
    pub ts: DateTime<Utc>,
    pub energy: u64,
    pub power: Option<u32>,
    pub voltage: f64,
    pub temperature: f64,
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
