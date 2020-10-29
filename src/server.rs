use tokio::net::{TcpListener, TcpStream};
use std::future::Future;
use tokio::sync::{Semaphore, broadcast, mpsc};
use std::sync::Arc;
use tracing::{debug, error, info, instrument};
use crate::db::Db;
use tokio::time::{sleep, Duration};
use crate::shutdown::Shutdown;
use tokio::io::BufWriter;
use bytes::BytesMut;
use crate::connection::Connection;
use crate::cmd::Command;

#[derive(Debug)]
struct Listener {
    db: Db,
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
}

#[derive(Debug)]
struct Handler {
    db: Db,
    connection: Connection,
    limit_connections: Arc<Semaphore>,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
}

pub const MAX_CONNECTIONS: usize = 250;

pub async fn run(listener: TcpListener, shutdown: impl Future) -> crate::Result<()> {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
    let mut server = Listener {
        db: Db::new(),
        listener,
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_rx,
        shutdown_complete_tx,
    };

    tokio::select! {
        res = server.run() => {
            if let Err(e) = res {
                error!(cause = %e, "failed to accept");
            }
        }
        _ = shutdown => {
            info!("shutting down")
        }
    }

    // 开始回收资源了(还要取一遍值，尼玛的麻烦)
    let Listener {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = server;

    drop(notify_shutdown);
    drop(shutdown_complete_tx);
    let _ = shutdown_complete_rx.recv().await;
    Ok(())
}

impl Listener {
    async fn run(&mut self) -> crate::Result<()> {
        info!("accepting inbound connections");
        loop {
            self.limit_connections.acquire().await.forget();
            let socket = self.accept().await?;
            let mut handler = Handler {
                db: self.db.clone(),
                connection: Connection::new(socket),
                limit_connections: self.limit_connections.clone(),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
            };

            tokio::task::spawn(async move {
                if let Err(e) = handler.run().await {
                    error!(cause = ?e , "connection error")
                }
            });
        }
    }

    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;
        loop {
            match self.listener.accept().await {
                Ok((stream, _)) => {
                    return Ok(stream);
                }
                Err(e) => {
                    if backoff > 64 {
                        return Err(e.into());
                    }
                }
            }

            sleep(Duration::from_secs(backoff)).await;
            backoff * 2;
        }
    }
}

impl Handler {
    async fn run(&mut self) -> crate::Result<()> {
        while !self.shutdown.is_shutdown() {
            let maybe_frame = tokio::select! {
                res = self.connection.read_frame() => res?,
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };

            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(())
            };

            let cmd = Command::from_frame(frame)?;
            debug!(?cmd);
            cmd.apply(&self.db, &mut self.connection, &mut self.shutdown).await?;
        }

        Ok(())
    }
}