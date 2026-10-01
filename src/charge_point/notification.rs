pub const UNIX_SOCKET_PATH: &str = "/run/charge_point/socket";

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

impl ChargeState {
    pub fn name(&self) -> &'static str {
        use ChargeState::*;
        match self {
            Available => "available",
            Preparing => "preparing",
            Charging(_) => "charging",
            SuspendedEvse(_) => "suspended EVSE",
            SuspendedEv => "suspended EV",
            StoppedByUser => "stopped user",
            Finishing => "finishing",
            UnknownSession => "unknown session",
            Error => "error",
        }
    }

    pub fn is_charging(&self) -> bool {
        matches!(self, Self::Charging(_))
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error)
    }
}
