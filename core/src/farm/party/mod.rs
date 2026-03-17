mod actor;

use actor::{Command, MemberCreate, PartyActor};
pub use actor::{Event, MemberId, PartyId};
use anyhow::Result;
use kameo::actor::{ActorRef, Recipient, Spawn};
use protocol::migo::worker::session;

#[derive(Debug, Clone)]
pub struct Party(ActorRef<PartyActor>);

impl Party {
    pub async fn new(pid: PartyId, parent: Recipient<(PartyId, Event)>) -> Result<Self> {
        let actor = PartyActor::spawn((pid, parent));
        Ok(Self(actor))
    }

    pub fn actor(&self) -> &ActorRef<PartyActor> {
        &self.0
    }

    pub async fn create(&self, friend_code: &str) -> Result<MemberId> {
        let bid = self.actor().ask(MemberCreate { friend_code: friend_code.to_string() }).await?;
        Ok(bid)
    }

    pub async fn start(&self) -> Result<()> {
        self.actor().ask(Command::Start).await?;
        Ok(())
    }

    pub async fn send_member(&self, id: MemberId, event: session::Event) -> Result<()> {
        self.actor().ask(Command::Member(id, event)).await?;
        Ok(())
    }
}
