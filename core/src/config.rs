use std::time::Duration;

/// Game-level configuration shared across the entire farm hierarchy.
///
/// A single `FarmConfig` is created once at startup and passed (by clone) to
/// every `Ratchet` and `PartyActor` that is spawned.  This means every
/// tunable knob lives in one place and is easy to adjust without touching
/// the logic code.
#[derive(Debug, Clone)]
pub struct FarmConfig {
    /// How many ready sessions must accumulate before a party is formed and
    /// started automatically.
    pub party_size: usize,

    /// The CS2 map that every party session must be loaded into before the
    /// `GameReady` sync point is declared.
    pub map: String,

    /// How long to wait after all group masters have received their match
    /// before instructing every member to accept the match.
    pub accept_match_delay: Duration,
}

impl Default for FarmConfig {
    fn default() -> Self {
        Self {
            party_size: 4,
            map: "de_vertigo".to_string(),
            accept_match_delay: Duration::from_secs(3),
        }
    }
}
