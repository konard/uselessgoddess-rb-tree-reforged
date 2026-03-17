use std::sync::Arc;
use std::time::Duration;

use bimap::BiMap;
use indexmap::IndexMap;
use slotmap::SlotMap;
use kameo::Actor;
use kameo::actor::{ActorRef, Recipient};
use kameo::prelude::{Context, Message};
use path_engine::engine::{self, BotEvent, BotId, CBotEvent, Engine, PathEngine};
use protocol::migo::worker::session::game::menu_task::MenuTask;
use protocol::migo::worker::session::game::{
    self, Map, MatchState, Phase, PlayerState, SignOnState, Team,
};
use protocol::migo::worker::session::{self};
use tokio::time::sleep;

use crate::error::Error;
use crate::farm::layout::PartyLayout;
use crate::prelude::*;

// ---------------------------------------------------------------------------
// Public messages
// ---------------------------------------------------------------------------

pub struct MemberCreate {
    pub sid: SessionKey,
    pub friend_code: String,
}

#[derive(Debug)]
pub enum Command {
    Member(SessionKey, session::Event),
    MemberDead(SessionKey),
    Start,
    AcceptMatchNow,
}

#[derive(Debug)]
pub enum Event {
    Session(SessionKey, session::Command),
    RoundComplete,
    Done,
}

// ---------------------------------------------------------------------------
// Party phase — explicit state machine replacing SyncPoint + sync_epoch
//
// Each variant represents a collective state: the party only transitions
// when *all* members have reached the required individual state.
// Transitions:
//
//   Forming ──start()──► Inviting ──all invited──► Lobby
//   Lobby ──all recv match──► MatchPending ──3 s──► Accepting
//   Accepting ──all accepted──► GameReady ──engine starts──► InGame
//   InGame ──engine stops──► Forming  (next round)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum MemberPhase {
    /// Waiting for their individual sync event.
    Waiting,
    /// Member has received a lobby invite (slave only; master skips to Lobby).
    InviteReceived,
    /// Member is in the lobby.
    Lobby,
    /// Member has received the match token.
    MatchReceived,
    /// Member is fully loaded in the game map.
    InGame,
}

#[derive(Debug, Clone, PartialEq)]
enum PartyPhase {
    /// Party not yet started; collecting members.
    Forming,
    /// Master is sending invites; slaves are waiting.
    Inviting,
    /// All members are in the lobby; waiting for match.
    InLobby,
    /// Match token received; waiting for all members then accepting.
    MatchPending { match_id: u64 },
    /// Accept-match menu task dispatched to all members.
    Accepting,
    /// All members loaded in the game; engine is running.
    InGame,
}

// ---------------------------------------------------------------------------
// Member & Group records
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct MenuTaskInfo {
    /// Total number of tasks in the current batch.
    total: usize,
    /// Phase to advance to once the final task completes.
    on_done: MemberPhase,
}

#[derive(Debug, Clone)]
struct Member {
    phase: MemberPhase,
    menu_task: Option<MenuTaskInfo>,
    friend_code: String,
    team: Option<Team>,
    name: Option<String>,
}

struct Group {
    master: SessionKey,
    slaves: Vec<SessionKey>,
    match_id: Option<u64>,
}

// ---------------------------------------------------------------------------
// PartyActor
// ---------------------------------------------------------------------------

pub struct PartyActor {
    pid: PartyKey,
    layout: PartyLayout,
    parent: Recipient<(PartyKey, Event)>,
    engine: Option<Arc<dyn PathEngine>>,
    round: usize,
    party_phase: PartyPhase,

    // IndexMap preserves insertion order, which guarantees a deterministic
    // master selection (first inserted session = master of the group).
    members: IndexMap<SessionKey, Member>,
    groups: SlotMap<GroupKey, Group>,

    bot_to_session: BiMap<BotId, SessionKey>,
}

impl Actor for PartyActor {
    type Args = (PartyKey, PartyLayout, Recipient<(PartyKey, Event)>);
    type Error = anyhow::Error;

    async fn on_start(
        args: Self::Args,
        _: ActorRef<Self>,
    ) -> anyhow::Result<Self, anyhow::Error> {
        let (pid, layout, parent) = args;
        Ok(Self {
            pid,
            layout,
            parent,
            engine: None,
            round: 0,
            party_phase: PartyPhase::Forming,
            members: IndexMap::new(),
            groups: SlotMap::with_key(),
            bot_to_session: BiMap::new(),
        })
    }
}

// ---------------------------------------------------------------------------
// Message handlers
// ---------------------------------------------------------------------------

impl Message<Command> for PartyActor {
    type Reply = crate::error::Result;

    async fn handle(&mut self, cmd: Command, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        match cmd {
            Command::Member(sid, event) => self.handle_member(sid, event, ctx).await,
            Command::MemberDead(sid) => {
                self.members.shift_remove(&sid);
                Ok(())
            }
            Command::Start => self.start_party(ctx).await,
            Command::AcceptMatchNow => self.send_accept_match(ctx).await,
        }
    }
}

impl Message<MemberCreate> for PartyActor {
    type Reply = crate::error::Result;

    async fn handle(
        &mut self,
        MemberCreate { sid, friend_code }: MemberCreate,
        _: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let member = Member {
            phase: MemberPhase::Waiting,
            menu_task: None,
            friend_code,
            team: None,
            name: None,
        };
        self.members.insert(sid, member);
        Ok(())
    }
}

impl Message<engine::Event> for PartyActor {
    type Reply = ();

    async fn handle(
        &mut self,
        event: engine::Event,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if let Err(e) = self.handle_engine(event, ctx).await {
            eprintln!("[party] {e}");
        }
    }
}

// ---------------------------------------------------------------------------
// Party lifecycle helpers
// ---------------------------------------------------------------------------

impl PartyActor {
    async fn start_party(
        &mut self,
        _ctx: &mut Context<Self, crate::error::Result>,
    ) -> crate::error::Result {
        let ids: Vec<SessionKey> = self.members.keys().cloned().collect();

        // Build a single group from all members (master = first inserted).
        let _gid = self.groups.insert_with_key(|_key| Group {
            master: ids[0],
            slaves: ids[1..].to_vec(),
            match_id: None,
        });

        self.party_phase = PartyPhase::Inviting;

        for (_gid, group) in &self.groups {
            let master_sid = group.master;

            let mut tasks = vec![MenuTask::Clear];
            for &slave_sid in &group.slaves {
                let code = self.members[&slave_sid].friend_code.clone();
                tasks.push(MenuTask::SendInvite { code });
                tasks.push(MenuTask::Clear);
            }

            let total = tasks.len();
            if let Some(master) = self.members.get_mut(&master_sid) {
                master.menu_task =
                    Some(MenuTaskInfo { total, on_done: MemberPhase::Lobby });
            }

            let command = game::Command::BotMenuSetTasks { tasks, offset: 0 };
            self.send_session(master_sid, session::Command::Game(command)).await?;

            let command = game::Command::BotMenuStart;
            self.send_session(master_sid, session::Command::Game(command)).await?;
        }
        Ok(())
    }

    async fn send_accept_match(
        &mut self,
        _ctx: &mut Context<Self, crate::error::Result>,
    ) -> crate::error::Result {
        self.party_phase = PartyPhase::Accepting;

        let accept_task = MenuTaskInfo { total: 1, on_done: MemberPhase::InGame };
        for member in self.members.values_mut() {
            member.menu_task = Some(accept_task.clone());
            member.phase = MemberPhase::Waiting;
        }

        let sids: Vec<SessionKey> = self.members.keys().cloned().collect();
        for sid in sids {
            let command = session::Command::Game(game::Command::BotMenuSetTasks {
                tasks: vec![MenuTask::AcceptMatch],
                offset: 0,
            });
            self.send_session(sid, command).await?;

            let command = session::Command::Game(game::Command::BotMenuStart);
            self.send_session(sid, command).await?;
        }
        Ok(())
    }

    async fn handle_engine(
        &mut self,
        event: engine::Event,
        ctx: &mut Context<Self, ()>,
    ) -> crate::error::Result {
        match event {
            engine::Event::Bot(id, BotEvent::Progress(prog)) => {
                println!("{:?} {:?}", id, prog);
            }
            engine::Event::CBot(id, ev) => self.handle_cbot(id, ev, ctx).await?,
            engine::Event::Stop => {
                self.bot_to_session.clear();
                self.engine = None;
                self.party_phase = PartyPhase::Forming;
                self.send_event(Event::Done).await?;
            }
        }
        Ok(())
    }

    async fn send_event(&self, event: Event) -> crate::error::Result {
        self.parent
            .tell((self.pid, event))
            .await
            .map_err(|e| Error::Send(e.into()))
    }

    async fn send_session(&self, sid: SessionKey, cmd: session::Command) -> crate::error::Result {
        self.send_event(Event::Session(sid, cmd)).await
    }

    async fn handle_cbot(
        &mut self,
        id: BotId,
        event: CBotEvent,
        _: &mut Context<Self, ()>,
    ) -> crate::error::Result {
        let sid = self
            .bot_to_session
            .get_by_left(&id)
            .cloned()
            .ok_or(Error::BotNotFound(id))?;

        match event {
            CBotEvent::SetTasks { tasks, offset } => {
                let cmd = game::Command::BotGameplaySetTasks { tasks, offset };
                self.send_session(sid, session::Command::Game(cmd)).await?;
            }
            CBotEvent::Start => {
                let cmd = game::Command::BotGameplayStart;
                self.send_session(sid, session::Command::Game(cmd)).await?;
            }
            CBotEvent::Stop => {
                let cmd = game::Command::BotGameplayStop;
                self.send_session(sid, session::Command::Game(cmd)).await?;
            }
            CBotEvent::Signal => {
                let cmd = game::Command::BotGameplaySignal;
                self.send_session(sid, session::Command::Game(cmd)).await?;
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Member event handler + phase transitions
    // -----------------------------------------------------------------------

    async fn handle_member(
        &mut self,
        sid: SessionKey,
        event: session::Event,
        ctx: &mut Context<Self, crate::error::Result>,
    ) -> crate::error::Result {
        let member = self.members.get_mut(&sid).ok_or(Error::SessionNotFound(sid))?;

        match event {
            session::Event::Game(game::Event::InviteReceived) => {
                member.phase = MemberPhase::InviteReceived;
                self.check_phase_transitions(ctx).await?;
            }

            session::Event::Game(game::Event::MatchReceived { match_id }) => {
                for (_gid, group) in &mut self.groups {
                    if sid == group.master {
                        group.match_id = Some(match_id);
                    }
                }

                let member = self.members.get_mut(&sid).ok_or(Error::SessionNotFound(sid))?;
                member.phase = MemberPhase::MatchReceived;
                self.check_phase_transitions(ctx).await?;
            }

            session::Event::Game(game::Event::StateUpdate {
                sign_on_state: SignOnState::Full,
                player: Some(PlayerState { team, name }),
                map: Map::Base { name: map_name },
                match_state: Some(MatchState { phase: Phase::Round { number }, freeze: false, .. }),
            }) => {
                // Dynamic map name from PartyLayout — no hardcoded constant.
                if map_name == self.layout.map
                    && matches!(team, Team::CounterTerrorists | Team::Terrorists)
                    && number as usize == self.round
                {
                    let member =
                        self.members.get_mut(&sid).ok_or(Error::SessionNotFound(sid))?;
                    member.name = Some(name);
                    member.team = Some(team);
                    member.phase = MemberPhase::InGame;
                    self.check_phase_transitions(ctx).await?;
                }
            }

            session::Event::Game(game::Event::BotTaskCompleted { id }) => {
                let bot_id = self
                    .bot_to_session
                    .get_by_right(&sid)
                    .cloned()
                    .ok_or(Error::SessionNotFound(sid))?;
                let engine = self.engine.as_ref().ok_or(Error::NoEngine)?;
                engine.task_completed(bot_id, id).await.map_err(Error::from)?;
            }

            session::Event::Game(game::Event::MenuTaskCompleted { id }) => {
                let member = self.members.get_mut(&sid).ok_or(Error::SessionNotFound(sid))?;
                let Some(task) = &member.menu_task else {
                    return Ok(());
                };

                if id + 1 != task.total {
                    return Ok(());
                }

                let task = member.menu_task.take().unwrap();
                member.phase = task.on_done;

                let command = session::Command::Game(game::Command::BotMenuStop);
                self.send_session(sid, command).await?;

                self.check_phase_transitions(ctx).await?;
            }

            _ => {}
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Phase transition logic (replaces sync_epoch + all_at)
    // -----------------------------------------------------------------------

    fn all_members_at(&self, phase: &MemberPhase) -> bool {
        !self.members.is_empty() && self.members.values().all(|m| &m.phase == phase)
    }

    async fn check_phase_transitions(
        &mut self,
        ctx: &mut Context<Self, crate::error::Result>,
    ) -> crate::error::Result {
        match &self.party_phase.clone() {
            PartyPhase::Inviting => {
                // All slaves received their invite → master advances to Lobby,
                // slaves run accept-invite task.
                let all_invited = {
                    let groups: Vec<_> = self.groups.values().collect();
                    groups.iter().all(|g| {
                        g.slaves.iter().all(|s| {
                            self.members
                                .get(s)
                                .map(|m| m.phase == MemberPhase::InviteReceived)
                                .unwrap_or(false)
                        })
                    })
                };

                if all_invited {
                    self.on_all_invited(ctx).await?;
                }
            }

            PartyPhase::InLobby => {
                if self.all_members_at(&MemberPhase::MatchReceived) {
                    self.on_all_recv_match(ctx).await?;
                }
            }

            PartyPhase::Accepting => {
                // Members transition to InGame phase when their menu task
                // completes with on_done = InGame.
                // The actual engine start happens here.
                if self.all_members_at(&MemberPhase::InGame) {
                    self.on_all_game_ready(ctx).await?;
                }
            }

            _ => {}
        }
        Ok(())
    }

    async fn on_all_invited(
        &mut self,
        _ctx: &mut Context<Self, crate::error::Result>,
    ) -> crate::error::Result {
        self.party_phase = PartyPhase::InLobby;

        for (_gid, group) in &self.groups {
            // Master is already in the lobby; just reset its phase.
            if let Some(master) = self.members.get_mut(&group.master) {
                master.phase = MemberPhase::Lobby;
            }

            for &slave_sid in &group.slaves {
                if let Some(slave) = self.members.get_mut(&slave_sid) {
                    slave.phase = MemberPhase::Waiting;
                    slave.menu_task =
                        Some(MenuTaskInfo { total: 2, on_done: MemberPhase::Lobby });
                }

                let command = session::Command::Game(game::Command::BotMenuSetTasks {
                    tasks: vec![MenuTask::Clear, MenuTask::AcceptInvite],
                    offset: 0,
                });
                self.send_session(slave_sid, command).await?;

                let command = session::Command::Game(game::Command::BotMenuStart);
                self.send_session(slave_sid, command).await?;
            }
        }
        Ok(())
    }

    async fn on_all_recv_match(
        &mut self,
        ctx: &mut Context<Self, crate::error::Result>,
    ) -> crate::error::Result {
        let match_ids: Vec<_> = self.groups.values().filter_map(|g| g.match_id).collect();
        if !match_ids.windows(2).all(|w| w[0] == w[1]) {
            println!("match_id mismatch across groups: {:?}", match_ids);
            // Reset members to lobby phase and try again.
            for m in self.members.values_mut() {
                m.phase = MemberPhase::Lobby;
            }
            return Ok(());
        }

        self.party_phase = PartyPhase::MatchPending { match_id: match_ids[0] };

        let self_recp = ctx.actor_ref().clone().recipient();
        tokio::spawn(async move {
            sleep(Duration::from_secs(3)).await;
            let _ = self_recp.tell(Command::AcceptMatchNow).await;
        });

        Ok(())
    }

    async fn on_all_game_ready(
        &mut self,
        ctx: &mut Context<Self, crate::error::Result>,
    ) -> crate::error::Result {
        self.round += 1;
        self.party_phase = PartyPhase::InGame;

        let recp = ctx.actor_ref().clone().recipient();
        let engine: Arc<dyn PathEngine> =
            Arc::new(Engine::new(recp).await.map_err(Error::from)?);

        for (sid, member) in self.members.iter_mut() {
            let (Some(team), Some(name)) = (&member.team, &member.name) else {
                continue;
            };

            if matches!(team, Team::CounterTerrorists | Team::Terrorists) {
                let bid = engine.create(name, team.clone()).await.map_err(Error::from)?;
                self.bot_to_session.insert(bid, *sid);
            }
        }

        engine.start().await.map_err(Error::from)?;
        self.engine = Some(engine);

        // Reset member phases for future rounds.
        for m in self.members.values_mut() {
            m.phase = MemberPhase::Waiting;
        }

        self.send_event(Event::RoundComplete).await?;
        Ok(())
    }
}
