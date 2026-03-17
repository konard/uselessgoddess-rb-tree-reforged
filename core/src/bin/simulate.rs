use anyhow::Result;
use farm_core::farm::{
    Command, Event, Nexus, PartyCommand, PartyCreate, PartyLayout, SessionCreate, SessionKey,
};
use farm_core::ids::WorkerSlot;
use kameo::actor::{ActorRef, Spawn};
use kameo::prelude::{Context, Message};
use protocol::migo::worker::session::{self, game};
use server::WorkerId;
use tokio::time::{Duration, sleep};

struct SimSession {
    sid: SessionKey,
    wid: WorkerId,
    /// Wire slot assigned by the worker registry.
    slot: WorkerSlot,
    nexus: ActorRef<Nexus>,
}

impl SimSession {
    async fn fire(&self, event: session::Event) -> Result<()> {
        self.nexus
            .ask(Command::Worker(
                self.wid,
                protocol::migo::worker::Event::Session(self.slot.0, event),
            ))
            .await?;
        Ok(())
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
            player: Some(game::PlayerState { team, name: format!("bot-{:?}", self.sid) }),
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
        impl Message<Event> for FarmLog {
            type Reply = ();
            async fn handle(
                &mut self,
                ev: Event,
                _: &mut Context<Self, ()>,
            ) -> Self::Reply {
                println!("[farm] {ev:?}");
            }
        }
        FarmLog::spawn(()).recipient()
    };

    let nexus = Nexus::spawn((server_recp, farm_recp));

    let wid = WorkerId(1);
    nexus.ask(Command::Connected(wid)).await?;
    println!("[sim] worker {wid:?} connected");
    sleep(Duration::from_millis(100)).await;

    let layout = PartyLayout { map: "de_vertigo".to_string(), party_size: 4 };

    // Sessions are allocated in order; slot 0 = first session, etc.
    let mut sessions: Vec<SimSession> = Vec::new();
    for i in 0..4usize {
        let sid: SessionKey = nexus
            .ask(SessionCreate {
                wid,
                username: format!("player{i}"),
                password: "pass".to_string(),
                secret: "secret".to_string(),
            })
            .await?;
        println!("[sim] session {sid:?} created (slot {i})");
        let s = SimSession { sid, wid, slot: WorkerSlot(i), nexus: nexus.clone() };
        s.fire_running().await?;
        s.fire_invite_code(&format!("FC-{i:04X}")).await?;
        sessions.push(s);
        sleep(Duration::from_millis(20)).await;
    }

    sleep(Duration::from_millis(200)).await;

    let pid = nexus.ask(PartyCreate { layout }).await?;
    println!("[sim] party {pid:?} created");

    for s in &sessions {
        nexus.ask(Command::Party(pid, PartyCommand::Add(s.sid))).await?;
    }
    nexus.ask(Command::Party(pid, PartyCommand::Start)).await?;
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
