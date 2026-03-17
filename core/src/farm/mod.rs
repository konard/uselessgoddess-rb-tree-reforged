mod party;
mod supervisor;

use supervisor::{Command, FarmSupervisor, PartyCommand, PartyCreate, SessionCreate};
pub use supervisor::{Event, WorkerEvent};
use anyhow::Result;
use kameo::actor::{ActorRef, Recipient, Spawn};
pub use crate::ids::{PartyId, SessionId};
use protocol::migo::worker;
use server::WorkerId;

#[derive(Debug, Clone)]
pub struct FarmModule(ActorRef<FarmSupervisor>);

impl FarmModule {
    pub async fn new(
        server: Recipient<(WorkerId, worker::Command)>,
        parent: Recipient<Event>,
    ) -> Result<Self> {
        let actor = FarmSupervisor::spawn((server, parent));
        Ok(Self(actor))
    }

    pub fn actor(&self) -> &ActorRef<FarmSupervisor> {
        &self.0
    }

    pub async fn worker_connected(&self, wid: WorkerId) -> Result<()> {
        Ok(self.actor().ask(Command::Connected(wid)).await?)
    }

    pub async fn worker_event(&self, wid: WorkerId, event: worker::Event) -> Result<()> {
        Ok(self.actor().ask(Command::Worker(wid, event)).await?)
    }

    pub async fn create_session(
        &self,
        &wid: &WorkerId,
        username: &str,
        password: &str,
        secret: &str,
    ) -> Result<SessionId> {
        Ok(self
            .actor()
            .ask(SessionCreate {
                wid,
                username: username.to_string(),
                password: password.to_string(),
                secret: secret.to_string(),
            })
            .await?)
    }

    pub async fn create_party(&self) -> Result<PartyId> {
        Ok(self.actor().ask(PartyCreate {}).await?)
    }

    pub async fn party_add(&self, pid: &PartyId, sid: &SessionId) -> Result<()> {
        let cmd = Command::Party(*pid, PartyCommand::Add(*sid));
        Ok(self.actor().ask(cmd).await?)
    }

    pub async fn party_start(&self, pid: &PartyId) -> Result<()> {
        let cmd = Command::Party(*pid, PartyCommand::Start);
        Ok(self.actor().ask(cmd).await?)
    }
}
