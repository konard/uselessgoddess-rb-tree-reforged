use std::net::SocketAddr;
use std::str::FromStr;

use anyhow::Result;
use farm_core::farm::{self, FarmModule, SessionId, WorkerEvent};
use kameo::Actor;
use kameo::actor::{ActorRef, Spawn};
use kameo::prelude::{Context, Message};
use protocol::migo::{ClientType, worker};
use server::{Server, WorkerId};
use time_check::mini_check;

const PARTY_SIZE: usize = 4;

pub struct Core {
    farm: FarmModule,
    server: Server,
    pending_sessions: Vec<SessionId>,
}

impl Actor for Core {
    type Args = ();
    type Error = anyhow::Error;

    async fn on_start(_: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let addr = SocketAddr::from_str("0.0.0.0:4000")?;

        let recp = actor_ref.clone().recipient();
        let server = Server::new(addr, recp).await?;

        let farm = FarmModule::new(
            actor_ref.clone().recipient(),
            actor_ref.clone().recipient(),
        ).await?;

        Ok(Self { farm, server, pending_sessions: Vec::new() })
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
            server::Event::New(wid, ClientType::Worker) => self.farm.worker_connected(wid).await?,
            server::Event::Worker(wid, ev) => self.farm.worker_event(wid, ev).await?,
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
                if self.pending_sessions.len() >= PARTY_SIZE {
                    let sids: Vec<SessionId> =
                        self.pending_sessions.drain(..PARTY_SIZE).collect();
                    let pid = self.farm.create_party().await?;
                    for s in sids {
                        self.farm.party_add(pid, s).await?;
                    }
                    self.farm.party_start(pid).await?;
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

    let core_ref = Core::spawn(());
    core_ref.wait_for_startup().await;

    tokio::signal::ctrl_c().await?;
    println!("Closing...");

    core_ref.stop_gracefully().await?;
    core_ref.wait_for_shutdown().await;

    Ok(())
}
