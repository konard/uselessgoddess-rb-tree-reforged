use std::collections::HashMap;

use anyhow::{Result, bail};
use kameo::Actor;
use kameo::actor::{ActorRef, Recipient};
use kameo::prelude::{Context, Message};
use protocol::migo::worker;
use protocol::migo::worker::session::{self, User, game};
use server::WorkerId;

use super::party::{self, Party};
use crate::ids::{IdGen, PartyId, SessionId};

struct Worker {
    next_slot: usize,
    slot_to_session: HashMap<usize, SessionId>,
}

pub struct SessionCreate {
    pub wid: WorkerId,
    pub username: String,
    pub password: String,
    pub secret: String,
}

pub struct PartyCreate {}

struct SessionRecord {
    worker_id: WorkerId,
    slot: usize,
    party_id: Option<PartyId>,
    friend_code: Option<String>,
}

#[derive(Debug)]
pub enum WorkerEvent {
    New,
}

#[derive(Debug)]
pub enum Event {
    Worker(WorkerId, WorkerEvent),
    SessionReady(SessionId),
    SessionDead(SessionId),
    PartyRound(PartyId),
    PartyDone(PartyId),
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

pub struct FarmSupervisor {
    parent: Recipient<Event>,
    server: Recipient<(WorkerId, worker::Command)>,

    session_gen: IdGen<crate::ids::SessionTag>,
    party_gen: IdGen<crate::ids::PartyTag>,

    parties: HashMap<PartyId, Party>,
    workers: HashMap<WorkerId, Worker>,
    sessions: HashMap<SessionId, SessionRecord>,
}

impl Actor for FarmSupervisor {
    type Args = (Recipient<(WorkerId, worker::Command)>, Recipient<Event>);
    type Error = anyhow::Error;

    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let (server, parent) = args;
        Ok(Self {
            parent,
            server,
            session_gen: IdGen::default(),
            party_gen: IdGen::default(),
            parties: HashMap::new(),
            workers: HashMap::new(),
            sessions: HashMap::new(),
        })
    }
}

impl Message<(PartyId, party::Event)> for FarmSupervisor {
    type Reply = ();

    async fn handle(
        &mut self,
        event: (PartyId, party::Event),
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if let Err(e) = self.handle_party(event).await {
            eprintln!("Err: {}", e);
        }
    }
}

impl FarmSupervisor {
    async fn handle_party(&mut self, (pid, event): (PartyId, party::Event)) -> Result<()> {
        match event {
            party::Event::Session(sid, cmd) => self.route_cmd(sid, cmd)?,
            party::Event::RoundComplete => {
                self.parent.tell(Event::PartyRound(pid)).await?;
            }
            party::Event::Done => {
                self.parties.remove(&pid);
                self.parent.tell(Event::PartyDone(pid)).await?;
            }
        }
        Ok(())
    }

    fn route_cmd(&self, sid: SessionId, cmd: session::Command) -> Result<()> {
        let rec =
            self.sessions.get(&sid).ok_or_else(|| anyhow::anyhow!("session {sid:?} not found"))?;
        let command = worker::Command::Session(rec.slot, cmd);
        self.server.tell((rec.worker_id, command)).try_send()?;
        Ok(())
    }
}

impl Message<Command> for FarmSupervisor {
    type Reply = Result<()>;

    async fn handle(&mut self, cmd: Command, ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        match cmd {
            Command::Connected(wid) => {
                self.workers
                    .insert(wid, Worker { next_slot: 0, slot_to_session: HashMap::new() });
                self.parent.tell(Event::Worker(wid, WorkerEvent::New)).await?;
            }

            Command::Worker(wid, worker::Event::Session(slot, ev)) => {
                let Some(worker) = self.workers.get(&wid) else {
                    bail!("Worker {:?} not found", wid);
                };
                let Some(&sid) = worker.slot_to_session.get(&slot) else {
                    bail!("Slot {} not found for worker {:?}", slot, wid);
                };
                self.handle_session(sid, ev, ctx).await?;
            }

            Command::Party(pid, PartyCommand::Add(sid)) => {
                let Some(party) = self.parties.get(&pid) else {
                    bail!("Party {:?} not found", pid);
                };

                let Some(sess) = self.sessions.get_mut(&sid) else {
                    bail!("Session {:?} not found", sid);
                };

                let Some(code) = sess.friend_code.clone() else {
                    bail!("Session {:?} has no friend code", sid);
                };

                sess.party_id = Some(pid);
                party.create(sid, &code).await?;
            }

            Command::Party(pid, PartyCommand::Start) => {
                let Some(party) = self.parties.get(&pid) else {
                    bail!("Party {:?} not found", pid);
                };
                party.start().await?;
            }
        }
        Ok(())
    }
}

impl Message<SessionCreate> for FarmSupervisor {
    type Reply = Result<SessionId>;

    async fn handle(
        &mut self,
        SessionCreate { wid, username, password, secret }: SessionCreate,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let Some(worker) = self.workers.get_mut(&wid) else {
            bail!("Worker {:?} not found", wid);
        };

        let slot = worker.next_slot;
        worker.next_slot += 1;

        let sid = self.session_gen.next();
        worker.slot_to_session.insert(slot, sid);

        let user = User { username, password, secret };
        self.sessions.insert(sid, SessionRecord { worker_id: wid, slot, party_id: None, friend_code: None });

        let command = worker::Command::Session(slot, session::Command::Create { user });
        self.server.tell((wid, command)).try_send()?;

        Ok(sid)
    }
}

impl Message<PartyCreate> for FarmSupervisor {
    type Reply = Result<PartyId>;

    async fn handle(
        &mut self,
        PartyCreate {}: PartyCreate,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let pid = self.party_gen.next();
        let recp = ctx.actor_ref().clone().recipient();

        let party = Party::new(pid, recp).await?;
        self.parties.insert(pid, party);

        Ok(pid)
    }
}

impl FarmSupervisor {
    async fn handle_session(
        &mut self,
        sid: SessionId,
        event: session::Event,
        _: &mut Context<Self, Result<()>>,
    ) -> Result<()> {
        let Some(sess) = self.sessions.get_mut(&sid) else {
            bail!("Session {:?} not found", sid);
        };

        match &event {
            session::Event::StateUpdate(session::State::Running) => {
                self.parent.tell(Event::SessionReady(sid)).await?;
            }

            session::Event::Game(game::Event::InviteCodeUpdate { code }) => {
                sess.friend_code = Some(code.to_string());
            }

            _ => {}
        }

        let Some(pid) = sess.party_id else {
            return Ok(());
        };

        let Some(party) = self.parties.get(&pid) else {
            return Ok(());
        };

        party.send_member(sid, event).await?;
        Ok(())
    }

    pub async fn on_worker_dead(&mut self, wid: WorkerId) -> Result<()> {
        let dead: Vec<SessionId> = self
            .sessions
            .iter()
            .filter(|(_, s)| s.worker_id == wid)
            .map(|(id, _)| *id)
            .collect();
        for sid in dead {
            self.on_session_dead(sid).await?;
        }
        self.workers.remove(&wid);
        Ok(())
    }

    pub async fn on_session_dead(&mut self, sid: SessionId) -> Result<()> {
        if let Some(rec) = self.sessions.get(&sid) {
            if let Some(pid) = rec.party_id {
                if let Some(party) = self.parties.get(&pid) {
                    let _ = party.member_dead(sid).await;
                }
            }
        }
        self.sessions.remove(&sid);
        self.parent.tell(Event::SessionDead(sid)).await?;
        Ok(())
    }
}
