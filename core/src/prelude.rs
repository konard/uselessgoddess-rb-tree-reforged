/// Re-exports of the most commonly used items within `migo-core`.
///
/// Internal modules can do `use crate::prelude::*` to get access to keys,
/// the error type, and the most-used external traits without spelling out
/// long import paths each time.
pub use crate::error::{Error, Result};
pub use crate::ids::{GroupKey, PartyKey, SessionKey, SlotMap, WorkerSlot};
