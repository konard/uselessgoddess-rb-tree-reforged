use bimap::BiMap;
use indexmap::IndexMap;
use kameo::Actor;
use kameo::actor::{ActorRef, Recipient};
use kameo::prelude::{Context, Message};
use path_engine::engine::{self, BotEvent, BotId, CBotEvent, Engine};
use protocol::migo::worker::session::game::menu_task::MenuTask;
use protocol::migo::worker::session::game::{
    self, Map, MatchState, Phase, PlayerState, SignOnState, Team,
};
use protocol::migo::worker::session::{self};
use tokio::time::sleep;

use crate::config::FarmConfig;
use crate::error::Error;
use crate::prelude::*;

pub struct MemberCreate {
    pub sid: SessionId,
    pub friend_code: String,
}

#[derive(Debug)]
pub enum Command {
    Member(SessionId, session::Event),
    MemberDead(SessionId),
    Start,
    AcceptMatchNow,
}

#[derive(Debug)]
pub enum Event {
    Session(SessionId, session::Command),
    RoundComplete,
    Done,
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum SyncPoint {
    Waiting,
    Ready,
    Lobby,
    RecvMatch,
    GameLoading,
    GameReady,
}

#[derive(Debug, Clone)]
pub struct InMenuTask {
    total: usize,
    next_sync: SyncPoint,
}

#[derive(Debug, Clone)]
pub struct Member {
    sync_epoch: u32,
    sync: SyncPoint,
    menu_task: Option<InMenuTask>,
    friend_code: String,
    team: Option<Team>,
    name: Option<String>,
}

/// All members of a party play as a single lobby group.
///
/// The first member added becomes the `master` (lobby host); the rest are
/// `slaves` (invited players).  `match_id` is set when the master receives
/// the match notification and is used to detect cross-group mismatches in
/// multi-group scenarios (kept for forward compatibility).
struct LobbyGroup {
    master: SessionId,
    slaves: Vec<SessionId>,
    match_id: Option<u64>,
}

pub struct PartyActor {
    pid: PartyId,
    config: FarmConfig,
    parent: Recipient<(PartyId, Event)>,
    engine: Option<Engine>,
    round: usize,

    // IndexMap preserves insertion order, which guarantees a deterministic
    // master selection (first inserted session = master of the group).
    members: IndexMap<SessionId, Member>,
    group: Option<LobbyGroup>,

    bot_to_session: BiMap<BotId, SessionId>,
}

impl Actor for PartyActor {
    type Args = (PartyId, FarmConfig, Recipient<(PartyId, Event)>);
    type Error = anyhow::Error;

    async fn on_start(args: Self::Args, _: ActorRef<Self>) -> anyhow::Result<Self, anyhow::Error> {
        let (pid, config, parent) = args;
        Ok(Self {
            pid,
            config,
            parent,
            engine: None,
            round: 0,
            members: IndexMap::new(),
            group: None,
            bot_to_session: BiMap::new(),
        })
    }
}

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
            sync_epoch: 0,
            sync: SyncPoint::Waiting,
            menu_task: None,
            friend_code,
            team: None,
            name: None,
        };
        self.members.insert(sid, member);
        Ok(())
    }
}

impl PartyActor {
    async fn start_party(&mut self, _ctx: &mut Context<Self, crate::error::Result>) -> crate::error::Result {
        let ids: Vec<SessionId> = self.members.keys().cloned().collect();
        let master = ids[0];
        let slaves = ids[1..].to_vec();

        let mut tasks = vec![MenuTask::Clear];
        for &slave_sid in &slaves {
            let code = self.members[&slave_sid].friend_code.clone();
            tasks.push(MenuTask::SendInvite { code });
            tasks.push(MenuTask::Clear);
        }

        let total = tasks.len();
        if let Some(master_member) = self.members.get_mut(&master) {
            master_member.menu_task = Some(InMenuTask { total, next_sync: SyncPoint::Ready });
        }

        self.group = Some(LobbyGroup { master, slaves, match_id: None });

        let command = game::Command::BotMenuSetTasks { tasks, offset: 0 };
        self.send_session(master, session::Command::Game(command)).await?;

        let command = game::Command::BotMenuStart;
        self.send_session(master, session::Command::Game(command)).await?;

        Ok(())
    }

    async fn send_accept_match(
        &mut self,
        _ctx: &mut Context<Self, crate::error::Result>,
    ) -> crate::error::Result {
        let accept_task = InMenuTask { total: 1, next_sync: SyncPoint::GameLoading };
        for member in self.members.values_mut() {
            member.menu_task = Some(accept_task.clone());
        }

        let sids: Vec<SessionId> = self.members.keys().cloned().collect();
        for sid in sids {
            let command = session::Command::Game(game::Command::BotMenuSetTasks {
                tasks: vec![MenuTask::AcceptMatch],
                offset: 0,
            });
            self.send_session(sid, command).await?;

            let command = session::Command::Game(game::Command::BotMenuStart);
            self.send_session(sid, command).await?;
        }

        self.clear_sync();
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

impl PartyActor {
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

    async fn send_session(&self, sid: SessionId, cmd: session::Command) -> crate::error::Result {
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

    async fn handle_member(
        &mut self,
        sid: SessionId,
        event: session::Event,
        ctx: &mut Context<Self, crate::error::Result>,
    ) -> crate::error::Result {
        let member = self.members.get_mut(&sid).ok_or(Error::SessionNotFound(sid))?;

        match event {
            session::Event::Game(game::Event::InviteReceived) => {
                member.sync = SyncPoint::Ready;
                self.sync_update(ctx).await?;
            }

            session::Event::Game(game::Event::MatchReceived { match_id }) => {
                if let Some(group) = &mut self.group
                    && sid == group.master
                {
                    group.match_id = Some(match_id);
                }

                member.sync = SyncPoint::RecvMatch;
                self.sync_update(ctx).await?;
            }

            session::Event::Game(game::Event::StateUpdate {
                sign_on_state: SignOnState::Full,
                player: Some(PlayerState { team, name }),
                map: Map::Base { name: map_name },
                match_state: Some(MatchState { phase: Phase::Round { number }, freeze: false, .. }),
            }) => {
                if map_name == self.config.map
                    && matches!(team, Team::CounterTerrorists | Team::Terrorists)
                    && number as usize == self.round
                {
                    member.name = Some(name);
                    member.team = Some(team);
                    member.sync = SyncPoint::GameReady;
                    self.sync_update(ctx).await?;
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
                let Some(task) = &member.menu_task else {
                    return Ok(());
                };

                if id + 1 != task.total {
                    return Ok(());
                }

                let Some(task) = member.menu_task.take() else {
                    return Ok(());
                };

                member.sync = task.next_sync;

                let command = session::Command::Game(game::Command::BotMenuStop);
                self.send_session(sid, command).await?;

                self.sync_update(ctx).await?;
            }

            _ => {}
        }
        Ok(())
    }

    fn clear_sync(&mut self) {
        for member in self.members.values_mut() {
            member.sync_epoch += 1;
            member.sync = SyncPoint::Waiting;
        }
    }

    fn current_epoch(&self) -> u32 {
        self.members.values().map(|m| m.sync_epoch).max().unwrap_or(0)
    }

    fn all_at(&self, epoch: u32, point: &SyncPoint) -> bool {
        self.members.values().all(|m| m.sync_epoch == epoch && &m.sync == point)
    }

    async fn sync_update(
        &mut self,
        ctx: &mut Context<Self, crate::error::Result>,
    ) -> crate::error::Result {
        let epoch = self.current_epoch();

        if self.all_at(epoch, &SyncPoint::Ready) {
            return self.on_all_ready(ctx).await;
        }
        if self.all_at(epoch, &SyncPoint::RecvMatch) {
            return self.on_all_recv_match(ctx).await;
        }
        if self.all_at(epoch, &SyncPoint::GameReady) {
            return self.on_all_game_ready(ctx).await;
        }
        Ok(())
    }

    async fn on_all_ready(
        &mut self,
        _ctx: &mut Context<Self, crate::error::Result>,
    ) -> crate::error::Result {
        let slaves = self.group.as_ref().map(|g| g.slaves.clone()).unwrap_or_default();

        // Master's lobby is open — slaves accept the invite.
        if let Some(group) = &self.group
            && let Some(master) = self.members.get_mut(&group.master)
        {
            master.sync = SyncPoint::Lobby;
        }

        for slave_sid in slaves {
            if let Some(slave) = self.members.get_mut(&slave_sid) {
                slave.menu_task = Some(InMenuTask { total: 2, next_sync: SyncPoint::Lobby });
            }

            let command = session::Command::Game(game::Command::BotMenuSetTasks {
                tasks: vec![MenuTask::Clear, MenuTask::AcceptInvite],
                offset: 0,
            });
            self.send_session(slave_sid, command).await?;

            let command = session::Command::Game(game::Command::BotMenuStart);
            self.send_session(slave_sid, command).await?;
        }

        self.clear_sync();
        Ok(())
    }

    async fn on_all_recv_match(
        &mut self,
        ctx: &mut Context<Self, crate::error::Result>,
    ) -> crate::error::Result {
        // Verify all groups agreed on the same match (forward-compat check).
        let match_id = self.group.as_ref().and_then(|g| g.match_id);
        if match_id.is_none() {
            println!("on_all_recv_match: no match_id recorded");
            self.clear_sync();
            return Ok(());
        }

        let delay = self.config.accept_match_delay;
        let self_recp = ctx.actor_ref().clone().recipient();
        tokio::spawn(async move {
            sleep(delay).await;
            let _ = self_recp.tell(Command::AcceptMatchNow).await;
        });

        Ok(())
    }

    async fn on_all_game_ready(
        &mut self,
        ctx: &mut Context<Self, crate::error::Result>,
    ) -> crate::error::Result {
        self.round += 1;

        let recp = ctx.actor_ref().clone().recipient();
        let engine = Engine::new(recp).await.map_err(Error::from)?;

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

        self.clear_sync();
        self.send_event(Event::RoundComplete).await?;
        Ok(())
    }
}
