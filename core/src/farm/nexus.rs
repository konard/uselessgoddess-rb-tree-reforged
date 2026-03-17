use std::collections::HashMap;

use anyhow::Result;
use kameo::Actor;
use kameo::actor::{ActorRef, Recipient};
use kameo::prelude::{Context, Message};
use protocol::migo::worker;
use protocol::migo::worker::session::{self, User, game};
use server::WorkerId;
use slotmap::SlotMap;

use super::party::{self, Party};
use super::layout::PartyLayout;
use crate::error::Error;
use crate::ids::{PartyKey, SessionKey, WorkerSlot};

// ---------------------------------------------------------------------------
// Registry — tracks worker connections and routes wire-slot ↔ SessionKey
// ---------------------------------------------------------------------------

struct Worker {
    /// Dense list of session keys in wire-slot order.
    /// The index into this vec equals the wire-level `WorkerSlot`.
    slots: Vec<SessionKey>,
}

impl Worker {
    fn new() -> Self {
        Self { slots: Vec::new() }
    }

    /// Append `key` as the next slot and return its `WorkerSlot`.
    fn alloc(&mut self, key: SessionKey) -> WorkerSlot {
        let slot = WorkerSlot(self.slots.len());
        self.slots.push(key);
        slot
    }

    fn lookup(&self, slot: WorkerSlot) -> Option<SessionKey> {
        self.slots.get(slot.0).copied()
    }
}

// ---------------------------------------------------------------------------
// Public commands & events
// ---------------------------------------------------------------------------

pub struct SessionCreate {
    pub wid: WorkerId,
    pub username: String,
    pub password: String,
    pub secret: String,
}

pub struct PartyCreate {
    pub layout: PartyLayout,
}

#[derive(Debug)]
pub enum WorkerEvent {
    New,
}

#[derive(Debug)]
pub enum Event {
    Worker(WorkerId, WorkerEvent),
    SessionReady(SessionKey),
    SessionDead(SessionKey),
    PartyRound(PartyKey),
    PartyDone(PartyKey),
}

#[derive(Debug)]
pub enum PartyCommand {
    Add(SessionKey),
    Start,
}

#[derive(Debug)]
pub enum Command {
    Connected(WorkerId),
    Worker(WorkerId, worker::Event),
    Party(PartyKey, PartyCommand),
}

// ---------------------------------------------------------------------------
// Internal session record — bridges Registry ↔ Orchestrator
// ---------------------------------------------------------------------------

struct SessionRecord {
    wid: WorkerId,
    slot: WorkerSlot,
    party_key: Option<PartyKey>,
    friend_code: Option<String>,
}

// ---------------------------------------------------------------------------
// Nexus actor
// ---------------------------------------------------------------------------

/// Core routing actor for the CS2 farm panel.
///
/// `Nexus` sits between the server (which talks to workers) and the
/// party logic.  Responsibilities are cleanly separated internally:
///
/// - **Registry** side: maintains authoritative maps of workers → slots and
///   slots → session keys; routes every incoming worker event or outgoing
///   session command to the right wire address.
///
/// - **Orchestrator** side: decides which sessions belong to which party,
///   creates/destroys parties, and propagates lifecycle events upward.
pub struct Nexus {
    parent: Recipient<Event>,
    server: Recipient<(WorkerId, worker::Command)>,

    // Registry state
    workers: HashMap<WorkerId, Worker>,
    sessions: SlotMap<SessionKey, SessionRecord>,

    // Orchestrator state
    parties: SlotMap<PartyKey, Party>,
}

impl Actor for Nexus {
    type Args = (Recipient<(WorkerId, worker::Command)>, Recipient<Event>);
    type Error = anyhow::Error;

    async fn on_start(args: Self::Args, _actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let (server, parent) = args;
        Ok(Self {
            parent,
            server,
            workers: HashMap::new(),
            sessions: SlotMap::with_key(),
            parties: SlotMap::with_key(),
        })
    }
}

// --- Party event callback ---------------------------------------------------

impl Message<(PartyKey, party::Event)> for Nexus {
    type Reply = ();

    async fn handle(
        &mut self,
        (pid, event): (PartyKey, party::Event),
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let result: crate::error::Result = match event {
            party::Event::Session(sid, cmd) => self.route_session_cmd(sid, cmd),
            party::Event::RoundComplete => {
                self.parent.tell(Event::PartyRound(pid)).await.map_err(|e| Error::Send(e.into()))
            }
            party::Event::Done => {
                self.parties.remove(pid);
                self.parent.tell(Event::PartyDone(pid)).await.map_err(|e| Error::Send(e.into()))
            }
        };
        if let Err(e) = result {
            eprintln!("[nexus] {e}");
        }
    }
}

// --- Main command handler ---------------------------------------------------

impl Message<Command> for Nexus {
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

            Command::Worker(wid, worker::Event::Session(wire_slot, ev)) => {
                let slot = WorkerSlot(wire_slot);
                let worker = self.workers.get(&wid).ok_or(Error::WorkerNotFound(wid))?;
                let sid = worker.lookup(slot).ok_or(Error::SlotNotFound { wid, slot })?;
                self.handle_session(sid, ev, ctx).await?;
            }

            Command::Party(pid, PartyCommand::Add(sid)) => {
                let party = self.parties.get(pid).ok_or(Error::PartyNotFound(pid))?;
                let code = {
                    let sess =
                        self.sessions.get_mut(sid).ok_or(Error::SessionNotFound(sid))?;
                    let code = sess.friend_code.clone().ok_or(Error::NoFriendCode(sid))?;
                    sess.party_key = Some(pid);
                    code
                };
                party.create(sid, &code).await.map_err(Error::from)?;
            }

            Command::Party(pid, PartyCommand::Start) => {
                let party = self.parties.get(pid).ok_or(Error::PartyNotFound(pid))?;
                party.start().await.map_err(Error::from)?;
            }
        }
        Ok(())
    }
}

// --- Session create ---------------------------------------------------------

impl Message<SessionCreate> for Nexus {
    type Reply = crate::error::Result<SessionKey>;

    async fn handle(
        &mut self,
        SessionCreate { wid, username, password, secret }: SessionCreate,
        _ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        if !self.workers.contains_key(&wid) {
            return Err(Error::WorkerNotFound(wid));
        }

        // Insert record with placeholder slot; we fix it up right after.
        let sid = self.sessions.insert(SessionRecord {
            wid,
            slot: WorkerSlot(usize::MAX), // overwritten immediately below
            party_key: None,
            friend_code: None,
        });

        let slot = self.workers.get_mut(&wid).unwrap().alloc(sid);
        self.sessions[sid].slot = slot;

        let user = User { username, password, secret };
        let command = worker::Command::Session(slot.0, session::Command::Create { user });
        self.server
            .tell((wid, command))
            .try_send()
            .map_err(|e| Error::Send(e.into()))?;

        Ok(sid)
    }
}

// --- Party create -----------------------------------------------------------

impl Message<PartyCreate> for Nexus {
    type Reply = crate::error::Result<PartyKey>;

    async fn handle(
        &mut self,
        PartyCreate { layout }: PartyCreate,
        ctx: &mut Context<Self, Self::Reply>,
    ) -> Self::Reply {
        let recp = ctx.actor_ref().clone().recipient();

        // Use insert_with_key so we know the PartyKey before constructing the
        // Party (the actor needs its own pid to tag outgoing events).
        // Party::new is sync (kameo spawn is non-async), so this works.
        let pid = self.parties.insert_with_key(|pid| Party::new(pid, layout, recp));

        Ok(pid)
    }
}

// ---------------------------------------------------------------------------
// Registry helpers (private)
// ---------------------------------------------------------------------------

impl Nexus {
    /// Route a session command down to the correct worker slot.
    fn route_session_cmd(
        &self,
        sid: SessionKey,
        cmd: session::Command,
    ) -> crate::error::Result {
        let rec = self.sessions.get(sid).ok_or(Error::SessionNotFound(sid))?;
        let command = worker::Command::Session(rec.slot.0, cmd);
        self.server.tell((rec.wid, command)).try_send().map_err(|e| Error::Send(e.into()))
    }

    async fn handle_session(
        &mut self,
        sid: SessionKey,
        event: session::Event,
        _: &mut Context<Self, crate::error::Result>,
    ) -> crate::error::Result {
        let sess = self.sessions.get_mut(sid).ok_or(Error::SessionNotFound(sid))?;

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

        let Some(pid) = sess.party_key else {
            return Ok(());
        };

        let Some(party) = self.parties.get(pid) else {
            return Ok(());
        };

        party.send_member(sid, event).await.map_err(Error::from)
    }

    pub async fn on_worker_dead(&mut self, wid: WorkerId) -> crate::error::Result {
        let dead: Vec<SessionKey> = self
            .sessions
            .iter()
            .filter(|(_, s)| s.wid == wid)
            .map(|(key, _)| key)
            .collect();
        for sid in dead {
            self.on_session_dead(sid).await?;
        }
        self.workers.remove(&wid);
        Ok(())
    }

    pub async fn on_session_dead(&mut self, sid: SessionKey) -> crate::error::Result {
        if let Some(rec) = self.sessions.get(sid)
            && let Some(pid) = rec.party_key
            && let Some(party) = self.parties.get(pid)
        {
            let _ = party.member_dead(sid).await;
        }
        self.sessions.remove(sid);
        self.parent
            .tell(Event::SessionDead(sid))
            .await
            .map_err(|e| Error::Send(e.into()))?;
        Ok(())
    }
}
