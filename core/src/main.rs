use std::net::SocketAddr;
use std::str::FromStr;

use anyhow::Result;
use farm_core::farm;
use farm_core::farm::{
    Command, Nexus, PartyCommand, PartyCreate, PartyKey, PartyLayout,
    SessionKey, WorkerEvent,
};
use kameo::Actor;
use kameo::actor::{ActorRef, Spawn};
use kameo::prelude::{Context, Message};
use protocol::migo::{ClientType, worker};
use server::{Server, WorkerId};
use time_check::mini_check;

/// Runtime configuration for the farm core.
pub struct Config {
    /// Bind address for the worker server.
    pub addr: SocketAddr,
    /// Dynamic party layout received from the server.
    pub layout: PartyLayout,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            addr: SocketAddr::from_str("0.0.0.0:4000").unwrap(),
            layout: PartyLayout { map: "de_vertigo".to_string(), party_size: 4 },
        }
    }
}

pub struct Core {
    config: Config,
    nexus: ActorRef<Nexus>,
    server: Server,
    pending_sessions: Vec<SessionKey>,
}

impl Actor for Core {
    type Args = Config;
    type Error = anyhow::Error;

    async fn on_start(config: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let addr = config.addr;

        let recp = actor_ref.clone().recipient();
        let server = Server::new(addr, recp).await?;

        let nexus = Nexus::spawn((
            actor_ref.clone().recipient::<(WorkerId, worker::Command)>(),
            actor_ref.clone().recipient::<farm::Event>(),
        ));

        Ok(Self { config, nexus, server, pending_sessions: Vec::new() })
    }
}

impl Message<server::Event> for Core {
    type Reply = ();
    async fn handle(&mut self, event: server::Event, ctx: &mut Context<Self, ()>) -> () {
        if let Err(e) = self.handle_server(event, ctx).await {
            eprintln!("server err: {e}");
        }
    }
}

impl Message<farm::Event> for Core {
    type Reply = ();
    async fn handle(&mut self, event: farm::Event, ctx: &mut Context<Self, ()>) -> () {
        if let Err(e) = self.handle_event(event, ctx).await {
            eprintln!("farm err: {e}");
        }
    }
}

impl Message<(WorkerId, worker::Command)> for Core {
    type Reply = ();
    async fn handle(
        &mut self,
        cmd: (WorkerId, worker::Command),
        ctx: &mut Context<Self, ()>,
    ) -> () {
        if let Err(e) = self.handle_worker(cmd, ctx).await {
            eprintln!("worker err: {e}");
        }
    }
}

impl Core {
    async fn handle_worker(
        &mut self,
        (wid, cmd): (WorkerId, worker::Command),
        _ctx: &mut Context<Self, ()>,
    ) -> Result<()> {
        self.server.send_worker(wid, cmd).await?;
        Ok(())
    }

    async fn handle_server(
        &mut self,
        event: server::Event,
        _ctx: &mut Context<Self, ()>,
    ) -> Result<()> {
        match event {
            server::Event::New(wid, ClientType::Worker) => {
                self.nexus.ask(Command::Connected(wid)).await?;
            }
            server::Event::Worker(wid, ev) => {
                self.nexus.ask(Command::Worker(wid, ev)).await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn handle_event(
        &mut self,
        event: farm::Event,
        _ctx: &mut Context<Self, ()>,
    ) -> Result<()> {
        match event {
            farm::Event::Worker(_wid, WorkerEvent::New) => {}

            farm::Event::SessionReady(sid) => {
                self.pending_sessions.push(sid);
                let party_size = self.config.layout.party_size;
                if self.pending_sessions.len() >= party_size {
                    let sids: Vec<SessionKey> =
                        self.pending_sessions.drain(..party_size).collect();
                    let pid: PartyKey = self
                        .nexus
                        .ask(PartyCreate { layout: self.config.layout.clone() })
                        .await?;
                    for s in sids {
                        self.nexus.ask(Command::Party(pid, PartyCommand::Add(s))).await?;
                    }
                    self.nexus.ask(Command::Party(pid, PartyCommand::Start)).await?;
                }
            }

            farm::Event::SessionDead(_sid) => {}
            farm::Event::PartyRound(_pid) => {}
            farm::Event::PartyDone(_pid) => {}
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    mini_check().await?;

    let core_ref = Core::spawn(Config::default());
    core_ref.wait_for_startup().await;

    tokio::signal::ctrl_c().await?;
    println!("Closing...");

    core_ref.stop_gracefully().await?;
    core_ref.wait_for_shutdown().await;

    Ok(())
}
