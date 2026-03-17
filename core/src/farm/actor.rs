use std::collections::HashMap;

use anyhow::{Result, bail};
use bimap::BiMap;
use kameo::Actor;
use kameo::actor::{ActorRef, Recipient};
use kameo::prelude::{Context, Message};
use protocol::migo::worker;
use protocol::migo::worker::session::{self, User, game};
use server::WorkerId;

use super::party::{self, MemberId, Party, PartyId};

struct Worker {
    sid: usize,
}

pub struct SessionCreate {
    pub wid: WorkerId,
    pub username: String,
    pub password: String,
    pub secret: String,
}

pub struct PartyCreate {}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
struct PartyMemberId(PartyId, MemberId);

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct SessionId(pub WorkerId, pub usize);

#[derive(Debug, Clone)]
pub enum SessionState {
    Starting,
    Running,
}

#[derive(Debug)]
pub enum SessionEvent {
    StateUpdate(SessionState),
}

#[derive(Debug)]
pub enum WorkerEvent {
    New,
}

#[derive(Debug)]
pub enum Event {
    Worker(WorkerId, WorkerEvent),
    Session(SessionId, SessionEvent),
}

#[derive(Debug)]
pub enum PartyCommand {
    Add(SessionId),
    Start,
}

#[derive(Debug)]
pub enum Command {
    Connected(WorkerId),
    Worker(WorkerId, worker::Event),
    Party(PartyId, PartyCommand),
}

struct Session {
    friend_code: Option<String>,
}

pub struct FarmModuleActor {
    parent: Recipient<Event>,
    server: Recipient<(WorkerId, worker::Command)>,

    party_id: PartyId,

    parties: HashMap<PartyId, Party>,
    member_to_session: BiMap<PartyMemberId, SessionId>,

    workers: HashMap<WorkerId, Worker>,
    sessions: HashMap<SessionId, Session>,
}

impl Actor for FarmModuleActor {
    type Args = (Recipient<(WorkerId, worker::Command)>, Recipient<Event>);
    type Error = anyhow::Error;

    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let (server, parent) = args;
        Ok(Self {
            parent,
            server,
            party_id: PartyId::default(),
            parties: HashMap::new(),
            member_to_session: BiMap::new(),
            workers: HashMap::new(),
            sessions: HashMap::new(),
        })
    }
}

impl Message<(PartyId, party::Event)> for FarmModuleActor {
    type Reply = ();

    async fn handle(
        &mut self,
        event: (PartyId, party::Event),
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if let Err(e) = self.handle_party(event, ctx).await {
            eprintln!("Err: {}", e);
        }
    }
}

impl FarmModuleActor {
    async fn handle_party(
        &mut self,
        (pid, event): (PartyId, party::Event),
        _ctx: &mut Context<Self, ()>,
    ) -> Result<()> {
        match event {
            party::Event::Member(mid, cmd) => {
                let pmid = PartyMemberId(pid, mid);
                let Some(id) = self.member_to_session.get_by_left(&pmid) else {
                    bail!("{:?} no found", pmid)
                };

                self.send_session(*id, cmd).await?;
            }
        }
        Ok(())
    }
}

impl Message<Command> for FarmModuleActor {
    type Reply = Result<()>;

    async fn handle(&mut self, cmd: Command, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        match cmd {
            Command::Connected(wid) => {
                self.workers.insert(wid, Worker { sid: 0 });
                self.send_worker_event(wid, WorkerEvent::New).await?;
            }

            Command::Worker(wid, worker::Event::Session(sid, ev)) => {
                self.handle_session(SessionId(wid, sid), ev, ctx).await?;
            }

            Command::Worker(..) => {}

            Command::Party(pid, PartyCommand::Add(sid)) => {
                let Some(party) = &self.parties.get(&pid) else {
                    bail!("Party {:?} not found", pid);
                };

                let Some(sess) = &self.sessions.get(&sid) else {
                    bail!("Session {:?} not found", sid);
                };

                let Some(code) = &sess.friend_code else {
                    bail!("Friend Code err");
                };

                let mid = party.create(code).await?;

                let pmid = PartyMemberId(pid, mid);
                self.member_to_session.insert(pmid, sid);
            }

            Command::Party(pid, PartyCommand::Start) => {
                let Some(party) = &self.parties.get(&pid) else {
                    bail!("Party {:?} not found", pid);
                };

                party.start().await?;
            }
        }

        Ok(())
    }
}

impl Message<SessionCreate> for FarmModuleActor {
    type Reply = Result<SessionId>;

    async fn handle(
        &mut self,
        SessionCreate { wid, username, password, secret }: SessionCreate,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let Some(worker) = self.workers.get_mut(&wid) else {
            bail!("Worker {:?} not found", wid);
        };

        let user = User { username, password, secret };

        let sid = SessionId(wid, worker.sid);
        worker.sid += 1;

        let sess = Session { friend_code: None };

        self.sessions.insert(sid, sess);

        let command = session::Command::Create { user };
        self.send_session(sid, command).await?;

        self.send_session_event(sid, SessionEvent::StateUpdate(SessionState::Starting)).await?;

        Ok(sid)
    }
}

impl Message<PartyCreate> for FarmModuleActor {
    type Reply = Result<PartyId>;

    async fn handle(
        &mut self,
        PartyCreate {}: PartyCreate,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let pid = self.party_id.increment();
        let recp = ctx.actor_ref().clone().recipient();

        let party = Party::new(pid, recp).await?;
        self.parties.insert(pid, party);

        Ok(pid)
    }
}

impl FarmModuleActor {
    async fn send_member(&self, sid: &PartyMemberId, cmd: session::Event) -> Result<()> {
        let Some(party) = self.parties.get(&sid.0) else {
            bail!("Not found {:?} in paties", sid.0);
        };
        party.send_member(sid.1, cmd).await?;
        Ok(())
    }

    async fn send_session(&self, sid: SessionId, cmd: session::Command) -> Result<()> {
        let command = worker::Command::Session(sid.1, cmd);
        self.server.tell((sid.0, command)).await?;
        Ok(())
    }

    async fn send_worker_event(&self, sid: WorkerId, event: WorkerEvent) -> Result<()> {
        self.parent.tell(Event::Worker(sid, event)).await?;
        Ok(())
    }

    async fn send_session_event(&self, sid: SessionId, event: SessionEvent) -> Result<()> {
        self.parent.tell(Event::Session(sid, event)).await?;
        Ok(())
    }

    async fn handle_session(
        &mut self,
        sid: SessionId,
        event: session::Event,
        _: &mut Context<Self, Result<()>>,
    ) -> Result<()> {
        let Some(sess) = self.sessions.get_mut(&sid) else {
            bail!("Session {:?} not found", sid);
        };

        println!("{:?}, {:?}", sid, event);
        match &event {
            session::Event::StateUpdate(session::State::Running) => {
                self.send_session_event(sid, SessionEvent::StateUpdate(SessionState::Running))
                    .await?;
            }

            session::Event::Game(game::Event::InviteCodeUpdate { code }) => {
                sess.friend_code = Some(code.to_string());
            }

            _ => {}
        }

        let Some(pmid) = self.member_to_session.get_by_right(&sid) else {
            return Ok(());
        };

        self.send_member(pmid, event).await?;

        Ok(())
    }
}
