use std::collections::HashMap;

use anyhow::Result;
use kameo::Actor;
use kameo::actor::{ActorRef, Recipient};
use kameo::prelude::{Context, Message};
use protocol::migo::worker;
use protocol::migo::worker::session::{self, User, game};
use server::WorkerId;

use super::party::{self, Party};
use crate::error::Error;
use crate::prelude::*;

struct Worker {
    sessions: Vec<SessionId>,
}

impl Worker {
    fn new() -> Self {
        Self { sessions: Vec::new() }
    }

    fn alloc(&mut self, sid: SessionId) -> usize {
        let slot = self.sessions.len();
        self.sessions.push(sid);
        slot
    }

    fn lookup(&self, slot: usize) -> Option<SessionId> {
        self.sessions.get(slot).copied()
    }
}

pub struct SessionCreate {
    pub wid: WorkerId,
    pub username: String,
    pub password: String,
    pub secret: String,
}

pub struct PartyCreate {}

struct SessionRecord {
    wid: WorkerId,
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

/// Core routing actor for the CS2 farm panel.
///
/// `Ratchet` sits between the server (which talks to workers) and the
/// party logic.  It maintains the authoritative maps of
/// workers → sessions and sessions → parties, and routes every
/// incoming worker event or outgoing session command to the right place.
pub struct Ratchet {
    parent: Recipient<Event>,
    server: Recipient<(WorkerId, worker::Command)>,

    session_gen: IdGen<crate::ids::SessionTag>,
    party_gen: IdGen<crate::ids::PartyTag>,

    parties: HashMap<PartyId, Party>,
    workers: HashMap<WorkerId, Worker>,
    sessions: HashMap<SessionId, SessionRecord>,
}

impl Actor for Ratchet {
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

impl Message<(PartyId, party::Event)> for Ratchet {
    type Reply = ();

    async fn handle(
        &mut self,
        (pid, event): (PartyId, party::Event),
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let result: crate::error::Result = match event {
            party::Event::Session(sid, cmd) => self.route_session_cmd(sid, cmd),
            party::Event::RoundComplete => {
                self.parent.tell(Event::PartyRound(pid)).await.map_err(|e| Error::Send(e.into()))
            }
            party::Event::Done => {
                self.parties.remove(&pid);
                self.parent.tell(Event::PartyDone(pid)).await.map_err(|e| Error::Send(e.into()))
            }
        };
        if let Err(e) = result {
            eprintln!("[ratchet] {e}");
        }
    }
}

impl Message<Command> for Ratchet {
    type Reply = crate::error::Result;

    async fn handle(
        &mut self,
        cmd: Command,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        match cmd {
            Command::Connected(wid) => {
                self.workers.insert(wid, Worker::new());
                self.parent
                    .tell(Event::Worker(wid, WorkerEvent::New))
                    .await
                    .map_err(|e| Error::Send(e.into()))?;
            }

            Command::Worker(wid, worker::Event::Session(slot, ev)) => {
                let worker = self.workers.get(&wid).ok_or(Error::WorkerNotFound(wid))?;
                let sid = worker.lookup(slot).ok_or(Error::SlotNotFound { wid, slot })?;
                self.handle_session(sid, ev, ctx).await?;
            }

            Command::Party(pid, PartyCommand::Add(sid)) => {
                let party = self.parties.get(&pid).ok_or(Error::PartyNotFound(pid))?;
                let code = {
                    let sess =
                        self.sessions.get_mut(&sid).ok_or(Error::SessionNotFound(sid))?;
                    let code = sess.friend_code.clone().ok_or(Error::NoFriendCode(sid))?;
                    sess.party_id = Some(pid);
                    code
                };
                party.create(sid, &code).await.map_err(Error::from)?;
            }

            Command::Party(pid, PartyCommand::Start) => {
                let party = self.parties.get(&pid).ok_or(Error::PartyNotFound(pid))?;
                party.start().await.map_err(Error::from)?;
            }
        }
        Ok(())
    }
}

impl Message<SessionCreate> for Ratchet {
    type Reply = crate::error::Result<SessionId>;

    async fn handle(
        &mut self,
        SessionCreate { wid, username, password, secret }: SessionCreate,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let worker = self.workers.get_mut(&wid).ok_or(Error::WorkerNotFound(wid))?;

        let sid = self.session_gen.next();
        let slot = worker.alloc(sid);

        let user = User { username, password, secret };
        self.sessions.insert(sid, SessionRecord { wid, slot, party_id: None, friend_code: None });

        let command = worker::Command::Session(slot, session::Command::Create { user });
        self.server
            .tell((wid, command))
            .try_send()
            .map_err(|e| Error::Send(e.into()))?;

        Ok(sid)
    }
}

impl Message<PartyCreate> for Ratchet {
    type Reply = crate::error::Result<PartyId>;

    async fn handle(
        &mut self,
        PartyCreate {}: PartyCreate,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let pid = self.party_gen.next();
        let recp = ctx.actor_ref().clone().recipient();

        let party = Party::new(pid, recp).await.map_err(Error::from)?;
        self.parties.insert(pid, party);

        Ok(pid)
    }
}

impl Ratchet {
    /// Route a session command to the correct worker slot.
    fn route_session_cmd(
        &self,
        sid: SessionId,
        cmd: session::Command,
    ) -> crate::error::Result {
        let rec = self.sessions.get(&sid).ok_or(Error::SessionNotFound(sid))?;
        let command = worker::Command::Session(rec.slot, cmd);
        self.server.tell((rec.wid, command)).try_send().map_err(|e| Error::Send(e.into()))
    }

    async fn handle_session(
        &mut self,
        sid: SessionId,
        event: session::Event,
        _: &mut Context<Self, crate::error::Result>,
    ) -> crate::error::Result {
        let sess = self.sessions.get_mut(&sid).ok_or(Error::SessionNotFound(sid))?;

        match &event {
            session::Event::StateUpdate(session::State::Running) => {
                self.parent
                    .tell(Event::SessionReady(sid))
                    .await
                    .map_err(|e| Error::Send(e.into()))?;
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

        party.send_member(sid, event).await.map_err(Error::from)
    }

    pub async fn on_worker_dead(&mut self, wid: WorkerId) -> crate::error::Result {
        let dead: Vec<SessionId> = self
            .sessions
            .iter()
            .filter(|(_, s)| s.wid == wid)
            .map(|(id, _)| *id)
            .collect();
        for sid in dead {
            self.on_session_dead(sid).await?;
        }
        self.workers.remove(&wid);
        Ok(())
    }

    pub async fn on_session_dead(&mut self, sid: SessionId) -> crate::error::Result {
        if let Some(rec) = self.sessions.get(&sid)
            && let Some(pid) = rec.party_id
            && let Some(party) = self.parties.get(&pid)
        {
            let _ = party.member_dead(sid).await;
        }
        self.sessions.remove(&sid);
        self.parent
            .tell(Event::SessionDead(sid))
            .await
            .map_err(|e| Error::Send(e.into()))?;
        Ok(())
    }
}
