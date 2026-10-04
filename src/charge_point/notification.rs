pub const UNIX_SOCKET_PATH: &str = "/run/charge_point/socket";

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ChargePointNotification {
    Charge(ChargeState),
    Connection(ChargePointConnectionState),
}

impl ChargePointNotification {
    pub fn name(&self) -> &'static str {
        use ChargePointNotification::*;
        match self {
            Charge(charge_state) => charge_state.name(),
            Connection(connection) => connection.name(),
        }
    }

    pub fn is_error(&self) -> bool {
        matches!(
            self,
            ChargePointNotification::Connection(ChargePointConnectionState::Error)
                | ChargePointNotification::Charge(ChargeState::Error)
        )
    }
    pub fn is_critical(&self) -> bool {
        matches!(
            self,
            ChargePointNotification::Charge(ChargeState::Error)
                | ChargePointNotification::Connection(ChargePointConnectionState::Error)
                | ChargePointNotification::Connection(ChargePointConnectionState::MissedHeartbeat)
        )
    }
    pub fn is_charging(&self) -> bool {
        matches!(
            self,
            ChargePointNotification::Charge(ChargeState::Charging(_))
        )
    }
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

impl ChargeState {
    pub fn name(&self) -> &'static str {
        use ChargeState::*;
        match self {
            Available => "disponible",
            Preparing => "en préparation",
            Charging(_) => "en charge",
            SuspendedEvse(_) => "pause programmée",
            SuspendedEv => "arrêt véhicule",
            StoppedByUser => "arrêt externe",
            Finishing => "VE déconnecté",
            UnknownSession => "session inconnue",
            Error => "erreur en charge",
        }
    }

    pub fn is_charging(&self) -> bool {
        matches!(self, Self::Charging(_))
    }

    pub fn is_error(&self) -> bool {
        matches!(self, Self::Error)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ChargePointConnectionState {
    Heartbeat,
    MissedHeartbeat,
    ServerDisconnected,
    Error,
}

impl ChargePointConnectionState {
    pub fn name(&self) -> &'static str {
        use ChargePointConnectionState::*;
        match self {
            Heartbeat => "pulsation",
            MissedHeartbeat => "absence de pouls",
            ServerDisconnected => "pas de connecθ",
            Error => "erreur connecθ",
        }
    }

    pub fn is_hearbeat(&self) -> bool {
        matches!(self, ChargePointConnectionState::Heartbeat)
    }
    pub fn is_alert(&self) -> bool {
        matches!(
            self,
            ChargePointConnectionState::Error
                | ChargePointConnectionState::MissedHeartbeat
                | ChargePointConnectionState::ServerDisconnected
        )
    }
}
