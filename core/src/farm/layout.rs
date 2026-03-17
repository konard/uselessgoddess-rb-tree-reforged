/// Dynamic configuration for a single farm party, received from the server.
///
/// Previously these values were hardcoded constants scattered across the
/// actor code; grouping them here makes the contract explicit and allows the
/// server to vary them per lobby without touching the actor logic.
#[derive(Debug, Clone)]
pub struct PartyLayout {
    /// CS2 map identifier, e.g. `"de_vertigo"`.
    pub map: String,
    /// Number of players that must be in the party.
    pub party_size: usize,
}
