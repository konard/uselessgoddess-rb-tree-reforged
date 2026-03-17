use anyhow::{Result, bail};
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
use std::time::Duration;
use tokio::time::sleep;

use crate::ids::{GroupId, IdGen, PartyId, SessionId};

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

struct Group {
    master: SessionId,
    slaves: Vec<SessionId>,
    match_id: Option<u64>,
}

pub struct PartyActor {
    pid: PartyId,
    parent: Recipient<(PartyId, Event)>,
    engine: Option<Engine>,
    round: usize,

    group_gen: IdGen<crate::ids::GroupTag>,

    members: IndexMap<SessionId, Member>,
    groups: IndexMap<GroupId, Group>,

    bot_to_session: BiMap<BotId, SessionId>,
}

impl Actor for PartyActor {
    type Args = (PartyId, Recipient<(PartyId, Event)>);
    type Error = anyhow::Error;

    async fn on_start(args: Self::Args, _: ActorRef<Self>) -> Result<Self, Self::Error> {
        let (pid, parent) = args;
        Ok(Self {
            pid,
            parent,
            engine: None,
            round: 0,
            group_gen: IdGen::default(),
            members: IndexMap::new(),
            groups: IndexMap::new(),
            bot_to_session: BiMap::new(),
        })
    }
}

impl Message<Command> for PartyActor {
    type Reply = Result<()>;

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
    type Reply = Result<()>;

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
    async fn start_party(&mut self, _ctx: &mut Context<Self, Result<()>>) -> Result<()> {
        let ids: Vec<SessionId> = self.members.keys().cloned().collect();

        let gid = self.group_gen.next();
        self.groups.insert(
            gid,
            Group { master: ids[0], slaves: ids[1..].to_vec(), match_id: None },
        );

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
                master.menu_task = Some(InMenuTask { total, next_sync: SyncPoint::Ready });
            }

            let command = game::Command::BotMenuSetTasks { tasks, offset: 0 };
            self.send_session(master_sid, session::Command::Game(command)).await?;

            let command = game::Command::BotMenuStart;
            self.send_session(master_sid, session::Command::Game(command)).await?;
        }
        Ok(())
    }

    async fn send_accept_match(&mut self, ctx: &mut Context<Self, Result<()>>) -> Result<()> {
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
        let _ = ctx;
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
            eprintln!("Err: {}", e);
        }
    }
}

impl PartyActor {
    async fn handle_engine(
        &mut self,
        event: engine::Event,
        ctx: &mut Context<Self, ()>,
    ) -> Result<()> {
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

    async fn send_event(&self, event: Event) -> Result<()> {
        Ok(self.parent.tell((self.pid, event)).await?)
    }

    async fn send_session(&self, sid: SessionId, cmd: session::Command) -> Result<()> {
        Ok(self.send_event(Event::Session(sid, cmd)).await?)
    }

    async fn handle_cbot(
        &mut self,
        id: BotId,
        event: CBotEvent,
        _: &mut Context<Self, ()>,
    ) -> Result<()> {
        let Some(sid) = self.bot_to_session.get_by_left(&id).cloned() else {
            println!("{:?}, not found", id);
            return Ok(());
        };

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
        ctx: &mut Context<Self, Result<()>>,
    ) -> Result<()> {
        let Some(member) = self.members.get_mut(&sid) else {
            bail!("Session {:?} not found", sid);
        };

        match event {
            session::Event::Game(game::Event::InviteReceived) => {
                member.sync = SyncPoint::Ready;
                self.sync_update(ctx).await?;
            }

            session::Event::Game(game::Event::MatchReceived { match_id }) => {
                for (_gid, group) in &mut self.groups {
                    if sid == group.master {
                        group.match_id = Some(match_id);
                    }
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
                const MAP_NAME: &str = "de_vertigo";
                if map_name == MAP_NAME
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
                let Some(bot_id) = self.bot_to_session.get_by_right(&sid).cloned() else {
                    bail!("{:?}, not found", sid);
                };
                let Some(engine) = &self.engine else {
                    bail!("No engine");
                };
                engine.task_completed(bot_id, id).await?;
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

    async fn sync_update(&mut self, ctx: &mut Context<Self, Result<()>>) -> Result<()> {
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

    async fn on_all_ready(&mut self, _ctx: &mut Context<Self, Result<()>>) -> Result<()> {
        for (_gid, group) in &self.groups {
            if let Some(master) = self.members.get_mut(&group.master) {
                master.sync = SyncPoint::Lobby;
            }

            for &slave_sid in &group.slaves {
                if let Some(slave) = self.members.get_mut(&slave_sid) {
                    slave.menu_task =
                        Some(InMenuTask { total: 2, next_sync: SyncPoint::Lobby });
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

        self.clear_sync();
        Ok(())
    }

    async fn on_all_recv_match(&mut self, ctx: &mut Context<Self, Result<()>>) -> Result<()> {
        let match_ids: Vec<_> = self.groups.values().filter_map(|g| g.match_id).collect();
        if !match_ids.windows(2).all(|w| w[0] == w[1]) {
            println!("match_id mismatch across groups: {:?}", match_ids);
            self.clear_sync();
            return Ok(());
        }

        let self_recp = ctx.actor_ref().clone().recipient();
        tokio::spawn(async move {
            sleep(Duration::from_secs(3)).await;
            let _ = self_recp.tell(Command::AcceptMatchNow).await;
        });

        Ok(())
    }

    async fn on_all_game_ready(&mut self, ctx: &mut Context<Self, Result<()>>) -> Result<()> {
        self.round += 1;

        let recp = ctx.actor_ref().clone().recipient();
        let engine = Engine::new(recp).await?;

        for (sid, member) in self.members.iter_mut() {
            let (Some(team), Some(name)) = (&member.team, &member.name) else {
                continue;
            };

            if matches!(team, Team::CounterTerrorists | Team::Terrorists) {
                let bid = engine.create(name, team.clone()).await?;
                self.bot_to_session.insert(bid, *sid);
            }
        }

        engine.start().await?;
        self.engine = Some(engine);

        self.clear_sync();
        self.send_event(Event::RoundComplete).await?;
        Ok(())
    }
}
