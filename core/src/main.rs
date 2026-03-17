pub mod farm;

use std::net::SocketAddr;
use std::str::FromStr;

use anyhow::Result;
use farm::{FarmModule, SessionEvent, SessionId, SessionState, WorkerEvent};
use kameo::Actor;
use kameo::actor::{ActorRef, Spawn};
use kameo::prelude::{Context, Message};
use protocol::migo::{ClientType, worker};
use server::{Server, WorkerId};
use time_check::mini_check;

pub struct Core {
    farm: FarmModule,
    server: Server,
}

impl Actor for Core {
    type Args = ();
    type Error = anyhow::Error;

    async fn on_start(_: Self::Args, actor_ref: ActorRef<Self>) -> Result<Self, Self::Error> {
        let addr = SocketAddr::from_str("0.0.0.0:4000")?;

        let recp = actor_ref.clone().recipient();
        let server = Server::new(addr, recp).await?;

        let recp_s = actor_ref.clone().recipient();
        let recp_p = actor_ref.clone().recipient();
        let farm = FarmModule::new(recp_s, recp_p).await?;

        Ok(Self { farm, server })
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
            farm::Event::Session(sid, SessionEvent::StateUpdate(state)) => {
                println!("[core] session {sid:?} -> {state:?}");
            }
        }
        Ok(())
    }
}

async fn simulate(farm: FarmModule, core_ref: ActorRef<Core>) {
    use protocol::migo::worker::session::{self, State, game};
    use tokio::time::{Duration, sleep};

    sleep(Duration::from_millis(100)).await;

    let wid = WorkerId(1);
    core_ref.ask(server::Event::New(wid, ClientType::Worker)).await.unwrap();
    println!("[sim] worker {wid:?} connected");

    let mut sids = Vec::new();
    for i in 0..4usize {
        let sid = farm.create_session(&wid, &format!("player{i}"), "pass", "secret").await.unwrap();
        sids.push(sid);
        println!("[sim] session created {sid:?}");

        let ev = worker::Event::Session(sid.1, session::Event::StateUpdate(State::Running));
        farm.worker_event(wid, ev).await.unwrap();

        let ev = worker::Event::Session(
            sid.1,
            session::Event::Game(game::Event::InviteCodeUpdate {
                code: format!("FRIEND-CODE-{i}"),
            }),
        );
        farm.worker_event(wid, ev).await.unwrap();

        sleep(Duration::from_millis(20)).await;
    }

    let pid = farm.create_party().await.unwrap();
    println!("[sim] party {pid:?} created");

    for sid in &sids {
        farm.party_add(&pid, sid).await.unwrap();
        println!("[sim] session {sid:?} added to party");
    }

    farm.party_start(&pid).await.unwrap();
    println!("[sim] party started — master sending invites");

    let master_sid = sids[0];
    sleep(Duration::from_millis(50)).await;
    {
        let ev = worker::Event::Session(
            master_sid.1,
            session::Event::Game(game::Event::MenuTaskCompleted { id: 4 }),
        );
        farm.worker_event(wid, ev).await.unwrap();
        println!("[sim] master menu task done");
    }

    for sid in &sids[1..] {
        let ev = worker::Event::Session(sid.1, session::Event::Game(game::Event::InviteReceived));
        farm.worker_event(wid, ev).await.unwrap();
    }

    sleep(Duration::from_millis(50)).await;
    for sid in &sids[1..] {
        let ev = worker::Event::Session(
            sid.1,
            session::Event::Game(game::Event::MenuTaskCompleted { id: 1 }),
        );
        farm.worker_event(wid, ev).await.unwrap();
    }
    println!("[sim] all slaves in lobby");

    for sid in &sids {
        let ev = worker::Event::Session(
            sid.1,
            session::Event::Game(game::Event::MatchReceived { match_id: 999 }),
        );
        farm.worker_event(wid, ev).await.unwrap();
    }

    sleep(Duration::from_millis(3500)).await;
    for sid in &sids {
        let ev = worker::Event::Session(
            sid.1,
            session::Event::Game(game::Event::MenuTaskCompleted { id: 0 }),
        );
        farm.worker_event(wid, ev).await.unwrap();
    }
    println!("[sim] all accepted match, loading game...");

    sleep(Duration::from_millis(50)).await;
    let teams = [
        game::Team::CounterTerrorists,
        game::Team::CounterTerrorists,
        game::Team::Terrorists,
        game::Team::Terrorists,
    ];
    for (i, sid) in sids.iter().enumerate() {
        let ev = worker::Event::Session(
            sid.1,
            session::Event::Game(game::Event::StateUpdate {
                sign_on_state: game::SignOnState::Full,
                player: Some(game::PlayerState {
                    team: teams[i].clone(),
                    name: format!("player{i}"),
                }),
                map: game::Map::Base { name: "de_vertigo".to_string() },
                match_state: Some(game::MatchState {
                    phase: game::Phase::Round { number: 0 },
                    freeze: false,
                }),
            }),
        );
        farm.worker_event(wid, ev).await.unwrap();
    }
    println!("[sim] all players GameReady → engine should start");

    println!("[sim] done — press Ctrl+C to exit");
}

#[tokio::main]
async fn main() -> Result<()> {
    mini_check().await?;

    let core_ref = Core::spawn(());
    core_ref.wait_for_startup().await;

    let _addr = SocketAddr::from_str("0.0.0.0:0")?;

    let dummy_server_recp = core_ref.clone().recipient::<(WorkerId, worker::Command)>();
    let farm_recp = core_ref.clone().recipient::<farm::Event>();
    let farm_for_sim = FarmModule::new(dummy_server_recp, farm_recp).await?;

    tokio::spawn(simulate(farm_for_sim, core_ref.clone()));

    tokio::signal::ctrl_c().await?;
    println!("Closing...");

    core_ref.stop_gracefully().await?;
    core_ref.wait_for_shutdown().await;

    Ok(())
}
