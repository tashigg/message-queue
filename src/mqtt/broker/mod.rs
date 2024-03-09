use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;

use color_eyre::eyre::{self, Context};
use slotmap::SlotMap;
use tashi_consensus_engine::{MessageStream, Platform};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use connection::Connection;

use crate::config::users::UsersConfig;
use crate::mqtt::router::{system_publish, MqttRouter, RouterHandle};
use crate::mqtt::session::{InactiveSessions, Session};
use crate::mqtt::{ClientId, ConnectionId};
use crate::password::PasswordHashingPool;

mod connection;

// The MQTT spec imposes a maximum topic length of 64 KiB but implementations can impose a smaller limit
const TOPIC_MAX_LENGTH: usize = 1024;

/// The Topic Alias Maximum to report to the client and enforce.
///
/// A topic alias is an integer that may be sent in lieu of a topic string to save bandwidth.
/// The client specifies an alias for a topic string by sending at least one PUBLISH with both set,
/// and then it may send an empty topic string for future PUBLISHes and provide the alias instead.
///
/// A client is allowed to overwrite any of their topic aliases by sending another PUBLISH with
/// the alias set and a different, non-empty topic string. Topic aliases are tracked
/// per-connection and only exist for the lifetime of a connection.
///
/// This is the maximum value the client may use for a topic alias.
/// Since 0 is not a valid topic alias, this also happens to be the maximum _number_
/// of topic aliases the client may use.
///
/// The broker must store topic aliases and their associated topic strings for the lifetime of
/// a connection, so having a good limit is prescient.
///
/// The broker can also use topic aliases when sending PUBLISHes to a client for topics matching
/// their subscription filters, in which case that is a completely separate value space.
///
/// https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901051
// `hashbrown` uses a max load factor of 7/8 and rounds allocations up to the next power of two,
// so a maximum larger than 112 will actually lead to an allocation of 256 buckets,
// meaning up to ~56% wasted space in the table.
//
// Might as well just make it a nice round number to avoid advertising that we're using a hashmap.
//
// TODO: make this configurable
const TOPIC_ALIAS_MAX: u16 = 100;

pub struct MqttBroker {
    listen_addr: SocketAddr,

    listener: TcpListener,

    token: CancellationToken,

    /// Generator for `ConnectionId`s
    connections: SlotMap<ConnectionId, ()>,
    tasks: JoinSet<ConnectionData>,

    shared: Arc<Shared>,

    broker_rx: mpsc::Receiver<BrokerEvent>,

    inactive_sessions: InactiveSessions,

    router: MqttRouter,
}

enum BrokerEvent {
    /// The session sender must only be set if the client wants to resume their
    /// session, i.e. the clean session flag isn't set on connect.
    ConnectionAccepted(ClientId, Option<oneshot::Sender<Option<Session>>>),
}

struct Shared {
    password_hasher: PasswordHashingPool,
    users: UsersConfig,
    broker_tx: mpsc::Sender<BrokerEvent>,
    tce_platform: Arc<Platform>,
    router: RouterHandle,
}

struct ConnectionData {
    id: ConnectionId,
    client_id: ClientId,
    session: Session,
}

impl MqttBroker {
    pub async fn bind(
        listen_addr: SocketAddr,
        users: UsersConfig,
        tce_platform: Arc<Platform>,
        tce_messages: MessageStream,
    ) -> crate::Result<Self> {
        let listener = TcpListener::bind(listen_addr)
            .await
            .wrap_err_with(|| format!("failed to bind listen_addr: {}", listen_addr))?;

        let (broker_tx, broker_rx) = mpsc::channel(100);

        let token = CancellationToken::new();

        let router = MqttRouter::start(tce_platform.clone(), tce_messages, token.clone());

        Ok(MqttBroker {
            listen_addr,
            listener,
            token,
            connections: SlotMap::with_capacity_and_key(256),
            tasks: JoinSet::new(),
            shared: Arc::new(Shared {
                password_hasher: PasswordHashingPool::new(
                    std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN),
                ),
                users,
                broker_tx: broker_tx.clone(),
                tce_platform,
                router: router.handle(),
            }),
            broker_rx,
            inactive_sessions: Default::default(),
            router,
        })
    }

    pub async fn run(&mut self) -> crate::Result<()> {
        tracing::info!(listen_addr = %self.listen_addr, "listening for connections");

        let mut shutdown = false;

        while !shutdown {
            tokio::select! {
                _ = self.token.cancelled() => {
                    shutdown = true;
                }
                Some(Ok(data)) = self.tasks.join_next() => {
                    self.router.disconnected(data.id);
                    self.connections.remove(data.id);
                    handle_connection_lost(&mut self.inactive_sessions, data);
                }
                res = self.listener.accept() => {
                    self.handle_accept(res);
                }
                Some(event) = self.broker_rx.recv() => {
                    self.handle_event(event).await;
                }
                res = self.router.task_result() => {
                    res.wrap_err("error from router task")?;
                    eyre::bail!("router task exited unexpectedly");
                }
                _ = self.inactive_sessions.process_expirations() => { }
            }
        }

        Ok(())
    }

    fn handle_accept(&mut self, result: std::io::Result<(TcpStream, SocketAddr)>) {
        match result {
            Ok((stream, remote_addr)) => {
                tracing::info!(%remote_addr, "connection received");

                let connection_id = self.connections.insert(());

                let conn = Connection::new(
                    connection_id,
                    stream,
                    remote_addr,
                    self.token.clone(),
                    self.shared.clone(),
                );

                self.tasks.spawn(conn.run());
            }
            // TODO: Some kinds of accept failures are probably fatal
            Err(e) => tracing::error!(?e, "accept failed"),
        }
    }

    async fn handle_event(&mut self, event: BrokerEvent) {
        match event {
            BrokerEvent::ConnectionAccepted(client_id, session_tx) => {
                match (session_tx, self.inactive_sessions.claim(&client_id)) {
                    (Some(session_tx), Some(mut session)) => {
                        if let Some(_last_will) = session.last_will.take() {
                            // TODO: last_will_properties
                            // TODO: submit it
                        }

                        // The client might have disconnected, closing the channel.
                        match session_tx.send(Some(session)) {
                            Ok(()) => tracing::trace!(client_id, "existing session was resumed"),
                            Err(maybe_session) => {
                                if let Some(session) = maybe_session {
                                    self.inactive_sessions.insert(client_id, session);
                                }
                            }
                        }
                    }
                    (Some(session_tx), None) => {
                        let _ = session_tx.send(None);
                    }
                    (None, Some(_)) => tracing::trace!(client_id, "existing session was dropped"),
                    _ => {}
                }
            }
        }
    }

    pub fn connections(&self) -> usize {
        self.tasks.len()
    }

    pub async fn shutdown(mut self) -> crate::Result<()> {
        // Closes any pending connections and stops listening for new ones.
        drop(self.listener);

        system_publish("$SYS/notices", "shutting down");

        self.token.cancel();

        while let Some(Ok(data)) = self.tasks.join_next().await {
            handle_connection_lost(&mut self.inactive_sessions, data);
            tracing::info!("{} connections remaining", self.tasks.len());
        }

        Ok(())
    }
}

// This is a free function because it would require a borrow of Broker while
// listener is partially moved in shutdown.
fn handle_connection_lost(inactive_sessions: &mut InactiveSessions, data: ConnectionData) {
    let ConnectionData {
        client_id, session, ..
    } = data;

    tracing::trace!(client_id, "connection lost");

    if session.should_save() {
        tracing::trace!(client_id, "session saved");
        inactive_sessions.insert(client_id, session);
    } else {
        tracing::trace!(client_id, "session expired");
    }
}
