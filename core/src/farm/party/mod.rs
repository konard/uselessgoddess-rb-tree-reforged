mod actor;

use actor::{Command, MemberCreate, PartyActor};
pub use actor::Event;
use kameo::actor::{ActorRef, Recipient, Spawn};
use protocol::migo::worker::session;

use crate::ids::{PartyKey, SessionKey};
use super::layout::PartyLayout;

#[derive(Debug, Clone)]
pub struct Party(ActorRef<PartyActor>);

impl Party {
    /// Spawn a new `PartyActor` and return the handle.
    ///
    /// Synchronous: kameo's `spawn` starts the actor immediately and
    /// returns an `ActorRef` without needing `await`.
    pub fn new(pid: PartyKey, layout: PartyLayout, parent: Recipient<(PartyKey, Event)>) -> Self {
        let actor = PartyActor::spawn((pid, layout, parent));
        Self(actor)
    }

    pub fn actor(&self) -> &ActorRef<PartyActor> {
        &self.0
    }

    pub async fn create(&self, sid: SessionKey, friend_code: &str) -> anyhow::Result<()> {
        self.actor()
            .ask(MemberCreate { sid, friend_code: friend_code.to_string() })
            .await?;
        Ok(())
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        self.actor().ask(Command::Start).await?;
        Ok(())
    }

    pub async fn send_member(&self, sid: SessionKey, event: session::Event) -> anyhow::Result<()> {
        self.actor().ask(Command::Member(sid, event)).await?;
        Ok(())
    }

    pub async fn member_dead(&self, sid: SessionKey) -> anyhow::Result<()> {
        self.actor().ask(Command::MemberDead(sid)).await?;
        Ok(())
    }
}
