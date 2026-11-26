//! TCP accept loop.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::{TcpListener, TcpSocket, ToSocketAddrs};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use super::{ClientId, Config, connection};

/// State shared by every connection task.
#[derive(Debug)]
pub(crate) struct Shared {
    pub config: Config,
}

/// A bound broker, ready to [`run`](Server::run).
pub struct Server {
    listener: TcpListener,
    shared: Arc<Shared>,
}

impl Server {
    pub async fn bind(addr: impl ToSocketAddrs, config: Config) -> io::Result<Self> {
        let addr = tokio::net::lookup_host(addr)
            .await?
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no address"))?;
        let socket = if addr.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        // Allow an immediate restart while old connections sit in TIME_WAIT.
        socket.set_reuseaddr(true)?;
        socket.bind(addr)?;
        // The kernel silently caps this at somaxconn (see DEBUGGING.md #1).
        let listener = socket.listen(config.listen_backlog)?;
        warn_if_backlog_capped(config.listen_backlog);
        Ok(Self {
            listener,
            shared: Arc::new(Shared { config }),
        })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Accept connections until `shutdown` is cancelled.
    pub async fn run(self, shutdown: CancellationToken) -> io::Result<()> {
        info!(addr = %self.local_addr()?, "minibroker listening");
        let mut next_id: ClientId = 0;
        let mut conns = JoinSet::new();

        loop {
            tokio::select! {
                _ = shutdown.cancelled() => break,
                res = self.listener.accept() => match res {
                    Ok((stream, peer)) => {
                        next_id += 1;
                        let id = next_id;
                        let shared = Arc::clone(&self.shared);
                        let cancel = shutdown.child_token();
                        conns.spawn(async move {
                            connection::run(id, stream, peer, shared, cancel).await
                        });
                    }
                    Err(e) => {
                        // EMFILE/ENFILE (out of file descriptors) or
                        // ECONNABORTED. Retrying immediately would spin at
                        // 100% CPU, so back off briefly.
                        warn!(error = %e, "accept failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                },
                // Reap finished connection tasks so the JoinSet doesn't grow
                // without bound on a long-running broker.
                Some(res) = conns.join_next(), if !conns.is_empty() => {
                    if let Err(e) = res {
                        error!(error = %e, "connection task panicked");
                    }
                }
            }
        }

        info!(open = conns.len(), "shutting down");
        drop(self.listener);
        let grace = self.shared.config.shutdown_grace;
        let drained = tokio::time::timeout(grace, async {
            while let Some(res) = conns.join_next().await {
                if let Err(e) = res {
                    error!(error = %e, "connection task panicked");
                }
            }
        })
        .await;
        if drained.is_err() {
            warn!(remaining = conns.len(), "grace period elapsed, aborting");
            conns.shutdown().await;
        }
        debug!("all connections closed");
        Ok(())
    }
}

/// The kernel clamps `listen(backlog)` to a system-wide maximum without
/// telling us. Connections beyond it are reset during a connection storm.
fn warn_if_backlog_capped(requested: u32) {
    if let Some(max) = somaxconn()
        && max < requested
    {
        warn!(
            requested,
            somaxconn = max,
            "listen backlog is capped by the kernel; connection storms beyond \
                 this many pending connections will be reset (raise net.core.somaxconn \
                 on Linux or kern.ipc.somaxconn on macOS)"
        );
    }
}

#[cfg(target_os = "linux")]
fn somaxconn() -> Option<u32> {
    std::fs::read_to_string("/proc/sys/net/core/somaxconn")
        .ok()?
        .trim()
        .parse()
        .ok()
}

#[cfg(target_os = "macos")]
fn somaxconn() -> Option<u32> {
    let out = std::process::Command::new("sysctl")
        .args(["-n", "kern.ipc.somaxconn"])
        .output()
        .ok()?;
    String::from_utf8(out.stdout).ok()?.trim().parse().ok()
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn somaxconn() -> Option<u32> {
    None
}
