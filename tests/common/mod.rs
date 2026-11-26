#![allow(dead_code)]

use std::net::SocketAddr;

use minibroker::broker::{Config, Server};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

pub struct TestBroker {
    pub addr: SocketAddr,
    pub shutdown: CancellationToken,
    pub handle: JoinHandle<std::io::Result<()>>,
}

pub async fn start(config: Config) -> TestBroker {
    let server = Server::bind("127.0.0.1:0", config).await.unwrap();
    let addr = server.local_addr().unwrap();
    let shutdown = CancellationToken::new();
    let handle = tokio::spawn(server.run(shutdown.clone()));
    TestBroker {
        addr,
        shutdown,
        handle,
    }
}

pub async fn start_default() -> TestBroker {
    start(Config::default()).await
}
