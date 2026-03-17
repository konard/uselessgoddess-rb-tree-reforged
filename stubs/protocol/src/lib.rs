pub mod migo {
    #[derive(Debug, Clone)]
    pub enum ClientType {
        Worker,
        Client,
    }

    pub mod worker {
        #[derive(Debug, Clone)]
        pub enum Event {
            Session(usize, session::Event),
        }

        #[derive(Debug, Clone)]
        pub enum Command {
            Session(usize, session::Command),
        }

        pub mod session {
            #[derive(Debug, Clone)]
            pub struct User {
                pub username: String,
                pub password: String,
                pub secret: String,
            }

            #[derive(Debug, Clone)]
            pub enum State {
                Starting,
                Running,
            }

            #[derive(Debug, Clone)]
            pub enum Command {
                Create { user: User },
                Game(game::Command),
            }

            #[derive(Debug, Clone)]
            pub enum Event {
                StateUpdate(State),
                Game(game::Event),
            }

            pub mod game {
                #[derive(Debug, Clone)]
                pub struct GameplayTask {
                    pub id: u32,
                }

                pub mod menu_task {
                    #[derive(Debug, Clone)]
                    pub enum MenuTask {
                        Clear,
                        SendInvite { code: String },
                        AcceptInvite,
                        AcceptMatch,
                    }
                }

                #[derive(Debug, Clone, PartialEq)]
                pub enum Team {
                    CounterTerrorists,
                    Terrorists,
                    Spectators,
                }

                #[derive(Debug, Clone)]
                pub enum SignOnState {
                    None,
                    Full,
                }

                #[derive(Debug, Clone)]
                pub struct PlayerState {
                    pub team: Team,
                    pub name: String,
                }

                #[derive(Debug, Clone)]
                pub enum Map {
                    None,
                    Base { name: String },
                }

                #[derive(Debug, Clone)]
                pub enum Phase {
                    Warmup,
                    Round { number: u32 },
                }

                #[derive(Debug, Clone)]
                pub struct MatchState {
                    pub phase: Phase,
                    pub freeze: bool,
                }

                #[derive(Debug, Clone)]
                pub enum Event {
                    InviteCodeUpdate {
                        code: String,
                    },
                    InviteReceived,
                    MatchReceived {
                        match_id: u64,
                    },
                    StateUpdate {
                        sign_on_state: SignOnState,
                        player: Option<PlayerState>,
                        map: Map,
                        match_state: Option<MatchState>,
                    },
                    BotTaskCompleted {
                        id: usize,
                    },
                    MenuTaskCompleted {
                        id: usize,
                    },
                }

                #[derive(Debug, Clone)]
                pub enum Command {
                    BotMenuSetTasks { tasks: Vec<menu_task::MenuTask>, offset: usize },
                    BotMenuStart,
                    BotMenuStop,
                    BotGameplaySetTasks { tasks: Vec<GameplayTask>, offset: usize },
                    BotGameplayStart,
                    BotGameplayStop,
                    BotGameplaySignal,
                }
            }
        }
    }
}
