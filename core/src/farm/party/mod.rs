mod actor;

use actor::{Command, MemberCreate, PartyActor};
pub use actor::Event;
use anyhow::Result;
use kameo::actor::{ActorRef, Recipient, Spawn};
use protocol::migo::worker::session;

use crate::config::FarmConfig;
use crate::ids::{PartyId, SessionId};

#[derive(Debug, Clone)]
pub struct Party(ActorRef<PartyActor>);

impl Party {
    pub async fn new(
        pid: PartyId,
        config: FarmConfig,
        parent: Recipient<(PartyId, Event)>,
    ) -> Result<Self> {
        let actor = PartyActor::spawn((pid, config, parent));
        Ok(Self(actor))
    }

    pub fn actor(&self) -> &ActorRef<PartyActor> {
        &self.0
    }

    pub async fn create(&self, sid: SessionId, friend_code: &str) -> Result<()> {
        self.actor()
            .ask(MemberCreate { sid, friend_code: friend_code.to_string() })
            .await?;
        Ok(())
    }

    pub async fn start(&self) -> Result<()> {
        self.actor().ask(Command::Start).await?;
        Ok(())
    }

    pub async fn send_member(&self, sid: SessionId, event: session::Event) -> Result<()> {
        self.actor().ask(Command::Member(sid, event)).await?;
        Ok(())
    }

    pub async fn member_dead(&self, sid: SessionId) -> Result<()> {
        self.actor().ask(Command::MemberDead(sid)).await?;
        Ok(())
    }
}
