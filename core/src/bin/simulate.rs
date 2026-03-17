use anyhow::Result;
use farm_core::farm::{self, FarmModule, SessionId};
use kameo::actor::{ActorRef, Spawn};
use kameo::prelude::{Context, Message};
use protocol::migo::worker::session::{self, game};
use server::WorkerId;
use tokio::time::{Duration, sleep};

struct SimSession {
    sid: SessionId,
    wid: WorkerId,
    farm: FarmModule,
}

impl SimSession {
    async fn fire(&self, event: session::Event) -> Result<()> {
        self.farm
            .worker_event(
                self.wid,
                protocol::migo::worker::Event::Session(self.sid.inner() as usize, event),
            )
            .await
    }

    async fn fire_running(&self) -> Result<()> {
        self.fire(session::Event::StateUpdate(session::State::Running)).await
    }

    async fn fire_invite_code(&self, code: &str) -> Result<()> {
        self.fire(session::Event::Game(game::Event::InviteCodeUpdate { code: code.to_string() }))
            .await
    }

    async fn fire_invite_received(&self) -> Result<()> {
        self.fire(session::Event::Game(game::Event::InviteReceived)).await
    }

    async fn fire_menu_done(&self, id: usize) -> Result<()> {
        self.fire(session::Event::Game(game::Event::MenuTaskCompleted { id })).await
    }

    async fn fire_match_received(&self, match_id: u64) -> Result<()> {
        self.fire(session::Event::Game(game::Event::MatchReceived { match_id })).await
    }

    async fn fire_game_ready(&self, team: game::Team, round: u32) -> Result<()> {
        self.fire(session::Event::Game(game::Event::StateUpdate {
            sign_on_state: game::SignOnState::Full,
            player: Some(game::PlayerState { team, name: format!("{:?}", self.sid) }),
            map: game::Map::Base { name: "de_vertigo".to_string() },
            match_state: Some(game::MatchState {
                phase: game::Phase::Round { number: round },
                freeze: false,
            }),
        }))
        .await
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let server_recp = {
        struct Noop;
        impl kameo::Actor for Noop {
            type Args = ();
            type Error = anyhow::Error;
            async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self> {
                Ok(Noop)
            }
        }
        impl Message<(WorkerId, protocol::migo::worker::Command)> for Noop {
            type Reply = ();
            async fn handle(
                &mut self,
                (wid, cmd): (WorkerId, protocol::migo::worker::Command),
                _: &mut Context<Self, ()>,
            ) -> Self::Reply {
                println!("[server] {wid:?} <- {cmd:?}");
            }
        }
        Noop::spawn(()).recipient()
    };

    let farm_recp = {
        struct FarmLog;
        impl kameo::Actor for FarmLog {
            type Args = ();
            type Error = anyhow::Error;
            async fn on_start(_: (), _: ActorRef<Self>) -> Result<Self> {
                Ok(FarmLog)
            }
        }
        impl Message<farm::Event> for FarmLog {
            type Reply = ();
            async fn handle(
                &mut self,
                ev: farm::Event,
                _: &mut Context<Self, ()>,
            ) -> Self::Reply {
                println!("[farm] {ev:?}");
            }
        }
        FarmLog::spawn(()).recipient()
    };

    let farm = FarmModule::new(server_recp, farm_recp).await?;

    let wid = WorkerId(1);
    farm.worker_connected(wid).await?;
    println!("[sim] worker {wid:?} connected");
    sleep(Duration::from_millis(100)).await;

    let mut sessions: Vec<SimSession> = Vec::new();
    for i in 0..4usize {
        let sid = farm.create_session(wid, &format!("player{i}"), "pass", "secret").await?;
        println!("[sim] session {sid:?} created");
        let s = SimSession { sid, wid, farm: farm.clone() };
        s.fire_running().await?;
        s.fire_invite_code(&format!("FC-{i:04X}")).await?;
        sessions.push(s);
        sleep(Duration::from_millis(20)).await;
    }

    sleep(Duration::from_millis(200)).await;

    let pid = farm.create_party().await?;
    println!("[sim] party {pid:?} created");

    for s in &sessions {
        farm.party_add(pid, s.sid).await?;
    }
    farm.party_start(pid).await?;
    println!("[sim] party started");

    sleep(Duration::from_millis(50)).await;

    let slave_count = sessions.len() - 1;
    let master_last_task = slave_count * 2;
    sessions[0].fire_menu_done(master_last_task).await?;
    println!("[sim] master menu done");

    for s in &sessions[1..] {
        s.fire_invite_received().await?;
    }

    sleep(Duration::from_millis(50)).await;
    for s in &sessions[1..] {
        s.fire_menu_done(1).await?;
    }
    println!("[sim] all slaves in lobby");

    for s in &sessions {
        s.fire_match_received(999).await?;
    }
    println!("[sim] match received, waiting 3s...");

    sleep(Duration::from_millis(3500)).await;
    for s in &sessions {
        s.fire_menu_done(0).await?;
    }
    println!("[sim] all accepted match");

    sleep(Duration::from_millis(50)).await;
    let teams = [
        game::Team::CounterTerrorists,
        game::Team::CounterTerrorists,
        game::Team::Terrorists,
        game::Team::Terrorists,
    ];
    for (i, s) in sessions.iter().enumerate() {
        s.fire_game_ready(teams[i].clone(), 0).await?;
    }
    println!("[sim] all GameReady -> engine should start");
    println!("[sim] done");

    Ok(())
}
