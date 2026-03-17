use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Result, bail};
use bimap::BiMap;
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

#[derive(Clone, Copy, Debug, Default, Hash, PartialEq, Eq)]
pub struct PartyId(usize);

impl PartyId {
    pub fn increment(&mut self) -> Self {
        let current = *self;
        self.0 = self.0.saturating_add(1);
        current
    }
}

#[derive(Clone, Copy, Debug, Default, Hash, PartialEq, Eq)]
pub struct GroupId(usize);

impl GroupId {
    pub fn increment(&mut self) -> Self {
        let current = *self;
        self.0 = self.0.saturating_add(1);
        current
    }
}

#[derive(Clone, Copy, Debug, Default, Hash, PartialEq, Eq)]
pub struct MemberId(usize);

impl MemberId {
    fn increment(&mut self) -> Self {
        let current = *self;
        self.0 = self.0.saturating_add(1);
        current
    }
}

pub struct MemberCreate {
    pub friend_code: String,
}

#[derive(Debug)]
pub enum Command {
    Member(MemberId, session::Event),
    Start,
}

#[derive(Debug)]
pub enum Event {
    Member(MemberId, session::Command),
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
    end: usize,
    state: SyncPoint,
}

#[derive(Debug, Clone)]
pub struct Member {
    menu_task: Option<InMenuTask>,

    friend_code: String,
    team: Option<Team>,
    name: Option<String>,
    sync: SyncPoint,
}

struct Group {
    master: MemberId,
    slaves: Vec<MemberId>,
    match_id: Option<u64>,
}

pub struct PartyActor {
    pid: PartyId,
    parent: Recipient<(PartyId, Event)>,
    engine: Option<Engine>,
    round: usize,

    member_id: MemberId,
    group_id: GroupId,

    members: HashMap<MemberId, Member>,
    groups: HashMap<GroupId, Group>,

    bot_to_members: BiMap<BotId, MemberId>,
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
            member_id: MemberId::default(),
            group_id: GroupId::default(),
            members: HashMap::new(),
            groups: HashMap::new(),
            bot_to_members: BiMap::new(),
        })
    }
}

impl Message<Command> for PartyActor {
    type Reply = Result<()>;

    async fn handle(&mut self, cmd: Command, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        match cmd {
            Command::Member(mid, event) => {
                self.handle_member(mid, event, ctx).await?;
            }

            Command::Start => {
                let ids: Vec<MemberId> = self.members.keys().cloned().collect();

                let gid0 = self.group_id.increment();
                self.groups.insert(
                    gid0,
                    Group { master: ids[0], slaves: ids[1..].to_vec(), match_id: None },
                );

                for (_gid, group) in &self.groups {
                    let master_mid = group.master;

                    let mut tasks = vec![MenuTask::Clear];

                    for &slave_mid in &group.slaves {
                        let code = self.members[&slave_mid].friend_code.clone();
                        tasks.push(MenuTask::SendInvite { code });
                        tasks.push(MenuTask::Clear);
                    }

                    if let Some(master) = self.members.get_mut(&master_mid) {
                        master.menu_task =
                            Some(InMenuTask { end: tasks.len(), state: SyncPoint::Ready });
                    }

                    let command = game::Command::BotMenuSetTasks { tasks, offset: 0 };
                    self.send_member(master_mid, session::Command::Game(command)).await?;

                    let command = game::Command::BotMenuStart;
                    self.send_member(master_mid, session::Command::Game(command)).await?;
                }
            }
        }
        Ok(())
    }
}

impl Message<MemberCreate> for PartyActor {
    type Reply = Result<MemberId>;

    async fn handle(
        &mut self,
        MemberCreate { friend_code }: MemberCreate,
        _: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let member = Member {
            menu_task: None,
            friend_code,
            team: None,
            name: None,
            sync: SyncPoint::Waiting,
        };

        let mid = self.member_id.increment();
        self.members.insert(mid, member);

        Ok(mid)
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
                self.bot_to_members.clear();
                self.engine = None;
            }
        }
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
    async fn send_event(&self, event: Event) -> Result<()> {
        Ok(self.parent.tell((self.pid, event)).await?)
    }

    async fn send_member(&self, id: MemberId, cmd: session::Command) -> Result<()> {
        Ok(self.send_event(Event::Member(id, cmd)).await?)
    }

    async fn handle_cbot(
        &mut self,
        id: BotId,
        event: CBotEvent,
        _: &mut Context<Self, ()>,
    ) -> Result<()> {
        let Some(sid) = self.bot_to_members.get_by_left(&id).cloned() else {
            println!("{:?}, not found", id);
            return Ok(());
        };

        match event {
            CBotEvent::SetTasks { tasks, offset } => {
                let cmd = game::Command::BotGameplaySetTasks { tasks, offset };
                self.send_member(sid, session::Command::Game(cmd)).await?;
            }
            CBotEvent::Start => {
                let cmd = game::Command::BotGameplayStart;
                self.send_member(sid, session::Command::Game(cmd)).await?;
            }
            CBotEvent::Stop => {
                let cmd = game::Command::BotGameplayStop;
                self.send_member(sid, session::Command::Game(cmd)).await?;
            }
            CBotEvent::Signal => {
                let cmd = game::Command::BotGameplaySignal;
                self.send_member(sid, session::Command::Game(cmd)).await?;
            }
        }

        Ok(())
    }

    async fn handle_member(
        &mut self,
        mid: MemberId,
        event: session::Event,
        ctx: &mut Context<Self, Result<()>>,
    ) -> Result<()> {
        let Some(member) = self.members.get_mut(&mid) else {
            bail!("Session {:?} not found", mid);
        };

        match event {
            session::Event::Game(game::Event::InviteReceived) => {
                member.sync = SyncPoint::Ready;
                self.sync_update(ctx).await?;
            }

            session::Event::Game(game::Event::MatchReceived { match_id }) => {
                for (_, group) in &mut self.groups {
                    if mid == group.master {
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
                if map_name == "de_vertigo"
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
                let Some(bot_id) = self.bot_to_members.get_by_right(&mid).cloned() else {
                    bail!("{:?}, not found", mid);
                };

                let Some(engine) = &self.engine else {
                    bail!("No engine");
                };

                engine.task_completed(bot_id, id).await?;
            }

            session::Event::Game(game::Event::MenuTaskCompleted { id }) => {
                let Some(task) = &member.menu_task else {
                    println!("menu task {:?} ok {} not found task", mid, id);
                    return Ok(());
                };

                if id + 1 != task.end {
                    println!("menu task {:?} not end {}", mid, id);
                    return Ok(());
                }

                let Some(task) = member.menu_task.take() else {
                    println!("menu task {:?} can't end {}", mid, id);
                    return Ok(());
                };

                println!("menu task {:?} ok {} => {:?}", mid, id, task.state);
                member.sync = task.state;

                let command = session::Command::Game(game::Command::BotMenuStop);
                self.send_member(mid.clone(), command).await?;

                println!("menu task {:?} ok {} sync_update", mid, id);
                self.sync_update(ctx).await?;
            }

            _ => {}
        }
        Ok(())
    }

    fn clear_sync(&mut self) -> Result<()> {
        for (_sid, sess) in &mut self.members {
            sess.sync = SyncPoint::Waiting;
        }
        Ok(())
    }

    async fn sync_update(&mut self, ctx: &mut Context<Self, Result<()>>) -> Result<()> {
        let all_same = |f: fn(&SyncPoint) -> bool| self.members.values().all(|s| f(&s.sync));

        println!("smnvv {:?}", self.members.values().map(|s| &s.sync));

        match () {
            _ if all_same(|s| matches!(s, SyncPoint::Ready)) => {
                for (_gid, group) in &self.groups {
                    if let Some(master) = self.members.get_mut(&group.master) {
                        master.sync = SyncPoint::Lobby;
                    }

                    for &slave_mid in &group.slaves {
                        if let Some(slave) = self.members.get_mut(&slave_mid) {
                            slave.menu_task = Some(InMenuTask { end: 2, state: SyncPoint::Lobby });
                        }

                        let command = session::Command::Game(game::Command::BotMenuSetTasks {
                            tasks: vec![MenuTask::Clear, MenuTask::AcceptInvite],
                            offset: 0,
                        });
                        self.send_member(slave_mid, command).await?;

                        let command = session::Command::Game(game::Command::BotMenuStart);
                        self.send_member(slave_mid, command).await?;
                    }
                }

                self.clear_sync()?;
                Ok(())
            }

            _ if all_same(|s| matches!(s, SyncPoint::RecvMatch)) => {
                let match_ids: Vec<_> = self.groups.values().filter_map(|g| g.match_id).collect();

                if !match_ids.windows(2).all(|w| w[0] == w[1]) {
                    println!("match_id mismatch across groups: {:?}", match_ids);
                    self.clear_sync()?;
                    return Ok(());
                }

                sleep(Duration::from_secs(3)).await;

                let accept_match = InMenuTask { end: 1, state: SyncPoint::GameLoading };

                for member in self.members.values_mut() {
                    member.menu_task = Some(accept_match.clone());
                }

                for mid in self.members.keys() {
                    let command = session::Command::Game(game::Command::BotMenuSetTasks {
                        tasks: vec![MenuTask::AcceptMatch],
                        offset: 0,
                    });
                    self.send_member(*mid, command).await?;

                    let command = session::Command::Game(game::Command::BotMenuStart);
                    self.send_member(*mid, command).await?;
                }

                self.clear_sync()?;
                Ok(())
            }

            _ if all_same(|s| matches!(s, SyncPoint::GameReady)) => {
                self.round += 1;

                let recp = ctx.actor_ref().clone().recipient();
                let engine = Engine::new(recp).await?;

                for (sid, sess) in self.members.iter_mut() {
                    let (Some(team), Some(name)) = (&sess.team, &sess.name) else {
                        continue;
                    };

                    if matches!(team, Team::CounterTerrorists | Team::Terrorists) {
                        let bid = engine.create(name, team.clone()).await?;
                        self.bot_to_members.insert(bid, *sid);
                    }
                }

                engine.start().await?;

                self.engine = Some(engine);

                self.clear_sync()?;
                Ok(())
            }

            _ => Ok(()),
        }
    }
}
