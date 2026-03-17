pub mod engine {
    use std::sync::atomic::{AtomicU32, Ordering};

    use anyhow::Result;
    use kameo::actor::Recipient;
    use protocol::migo::worker::session::game::{GameplayTask, Team};

    #[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
    pub struct BotId(pub u32);

    #[derive(Debug, Clone)]
    pub enum BotEvent {
        Progress(f32),
    }

    #[derive(Debug, Clone)]
    pub enum CBotEvent {
        SetTasks { tasks: Vec<GameplayTask>, offset: usize },
        Start,
        Stop,
        Signal,
    }

    #[derive(Debug, Clone)]
    pub enum Event {
        Bot(BotId, BotEvent),
        CBot(BotId, CBotEvent),
        Stop,
    }

    pub struct Engine {
        _recp: Recipient<Event>,
        next_id: AtomicU32,
    }

    impl Engine {
        pub async fn new(recp: Recipient<Event>) -> Result<Self> {
            println!("[engine] created");
            Ok(Self { _recp: recp, next_id: AtomicU32::new(0) })
        }

        pub async fn create(&self, name: &str, _team: Team) -> Result<BotId> {
            let id = self.next_id.fetch_add(1, Ordering::SeqCst);
            println!("[engine] create bot '{name}' => BotId({id})");
            Ok(BotId(id))
        }

        pub async fn start(&self) -> Result<()> {
            println!("[engine] start");
            Ok(())
        }

        pub async fn task_completed(&self, bot_id: BotId, id: usize) -> Result<()> {
            println!("[engine] task_completed {bot_id:?} step={id}");
            Ok(())
        }
    }
}
