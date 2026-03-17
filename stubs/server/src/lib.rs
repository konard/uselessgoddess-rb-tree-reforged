use std::net::SocketAddr;

use anyhow::Result;
use kameo::actor::Recipient;
use protocol::migo::{ClientType, worker};

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct WorkerId(pub u32);

#[derive(Debug)]
pub enum Event {
    New(WorkerId, ClientType),
    Worker(WorkerId, worker::Event),
}

pub struct Server {
    _recp: Recipient<Event>,
}

impl Server {
    pub async fn new(addr: SocketAddr, recp: Recipient<Event>) -> Result<Self> {
        println!("[server] stub listening on {addr} (no-op)");
        Ok(Self { _recp: recp })
    }

    pub async fn send_worker(&self, wid: WorkerId, cmd: worker::Command) -> Result<()> {
        println!("[server] send_worker {wid:?} -> {cmd:?}");
        Ok(())
    }
}
