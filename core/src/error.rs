use thiserror::Error;
use crate::ids::{PartyId, SessionId};

/// Domain errors produced by the farm routing core.
///
/// Using typed errors lets callers match on specific failure cases, e.g.
/// when deciding whether to retry after a `SessionNotFound` vs. a
/// `WorkerNotFound`.
#[derive(Debug, Error)]
pub enum Error {
    #[error("worker {0:?} not found")]
    WorkerNotFound(server::WorkerId),

    #[error("slot {slot} not found for worker {wid:?}")]
    SlotNotFound { wid: server::WorkerId, slot: usize },

    #[error("session {0:?} not found")]
    SessionNotFound(SessionId),

    #[error("session {0:?} has no friend code")]
    NoFriendCode(SessionId),

    #[error("party {0:?} not found")]
    PartyNotFound(PartyId),

    #[error("engine not started")]
    NoEngine,

    #[error("bot {0:?} not found")]
    BotNotFound(path_engine::engine::BotId),

    /// Wraps any lower-level transport / actor-framework errors.
    #[error(transparent)]
    Send(anyhow::Error),
}

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        Error::Send(e)
    }
}

/// Convenience result alias used throughout the crate.
pub type Result<T = ()> = std::result::Result<T, Error>;
