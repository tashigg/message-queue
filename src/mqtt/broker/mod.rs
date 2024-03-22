use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;

use color_eyre::eyre::{self, Context};
use slotmap::SlotMap;
use tashi_collections::{hash_map, HashMap};
use tashi_consensus_engine::{MessageStream, Platform};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use connection::Connection;

use crate::config::users::UsersConfig;
use crate::mqtt::router::{system_publish, MqttRouter, RouterHandle};
use crate::mqtt::session::{InactiveSessions, SessionStore};
use crate::mqtt::{ClientId, ClientIndex, ConnectionId};
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

    clients: Clients,

    /// Generator for `ConnectionId`s
    connections: SlotMap<ConnectionId, ()>,
    tasks: JoinSet<ConnectionData>,

    shared: Arc<Shared>,

    broker_rx: mpsc::Receiver<BrokerEvent>,

    inactive_sessions: InactiveSessions,

    router: MqttRouter,
}

#[derive(Default)]
struct Clients {
    by_id: HashMap<ClientId, ClientIndex>,
    by_index: SlotMap<ClientIndex, ClientState>,
}

struct ClientState {
    id: ClientId,
    connection_id: Option<ConnectionId>,
}

enum BrokerEvent {
    /// The session sender must only be set if the client wants to resume their
    /// session, i.e. the clean session flag isn't set on connect.
    ConnectionAccepted {
        connection_id: ConnectionId,
        client_id: ClientId,
        clean_session: bool,
        response_tx: oneshot::Sender<ConnectionAcceptedResponse>,
    },
}

struct ConnectionAcceptedResponse {
    client_index: ClientIndex,
    session: Option<SessionStore>,
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
    client_index: Option<ClientIndex>,
    store: SessionStore,
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
            clients: Default::default(),
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
                    if let Some(client_index) = data.client_index {
                        self.router.disconnected(client_index, data.id);
                    }

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
                Some(client_id) = self.inactive_sessions.next_expiration() => {
                    if let Some(client_idx) = self.clients.index_from_id(&client_id) {
                        self.router.evict_client(client_idx);
                        self.clients.remove(client_idx);
                    }
                }
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
            BrokerEvent::ConnectionAccepted {
                connection_id,
                client_id,
                clean_session,
                response_tx,
            } => {
                let session = match (clean_session, self.inactive_sessions.claim(&client_id)) {
                    (false, Some(mut store)) => {
                        if let Some(_last_will) = store.session.last_will.take() {
                            // TODO: last_will_properties
                            // TODO: submit it
                        }

                        tracing::trace!(client_id, "existing session was resumed");
                        Some(store)
                    }
                    (false, None) => {
                        tracing::trace!(
                            client_id,
                            "client requested to resume session, none was found"
                        );
                        None
                    }
                    (true, Some(_)) => {
                        tracing::trace!(client_id, "existing session was dropped");
                        None
                    }
                    (true, None) => {
                        tracing::trace!(client_id, "starting clean session");
                        None
                    }
                };

                let client_index = self.clients.get_or_insert(client_id.clone());

                if let Some(replaced_connection) = self.clients.by_index[client_index]
                    .connection_id
                    .replace(connection_id)
                {
                    // TODO: close replaced connection and wait for session to be released [TG-440]
                    tracing::warn!(client_id, "leaking replaced connection for client ID");
                    self.connections.remove(replaced_connection);
                }

                // The client might have disconnected, closing the channel.
                if let Err(response) = response_tx.send(ConnectionAcceptedResponse {
                    client_index,
                    session,
                }) {
                    if let Some(store) = response.session {
                        self.inactive_sessions.insert(client_id, store);
                    } else {
                        // Note: a malicious client could theoretically force the version for a slot
                        // to wrap around by connecting and disconnecting a bunch of times.
                        //
                        // While `slotmap` handles this just fine, it could cause us
                        // to leak information about another client that ended up in the same slot
                        // if the version happens to wrap around to one referenced by an old key
                        // somewhere.
                        //
                        // This is unlikely to be feasibly exploitable, however, as long as we
                        // are sure to keep other maps using the same key in-sync. We can also
                        // make this more difficult to manipulate by throttling connection attempts.
                        self.clients.remove(client_index);
                    }
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

impl Clients {
    fn index_from_id(&self, id: &str) -> Option<ClientIndex> {
        self.by_id.get(id).cloned()
    }

    #[allow(dead_code)]
    fn by_index(&self, index: ClientIndex) -> Option<&ClientState> {
        self.by_index.get(index)
    }

    #[allow(dead_code)]
    fn by_index_mut(&mut self, index: ClientIndex) -> Option<&mut ClientState> {
        self.by_index.get_mut(index)
    }

    fn get_or_insert(&mut self, id: ClientId) -> ClientIndex {
        match self.by_id.entry(id) {
            hash_map::Entry::Occupied(occupied) => *occupied.get(),
            hash_map::Entry::Vacant(vacant) => self.by_index.insert_with_key(move |index| {
                // No real getting around this, `.entry_ref()` clones anyway.
                let id = vacant.key().clone();
                vacant.insert(index);

                ClientState {
                    id,
                    connection_id: None,
                }
            }),
        }
    }

    fn remove(&mut self, index: ClientIndex) -> Option<ClientState> {
        let state = self.by_index.remove(index)?;
        self.by_id.remove(&state.id);

        Some(state)
    }
}

// This is a free function because it would require a borrow of Broker while
// listener is partially moved in shutdown.
fn handle_connection_lost(inactive_sessions: &mut InactiveSessions, data: ConnectionData) {
    let ConnectionData {
        client_id,
        store: session,
        ..
    } = data;

    tracing::trace!(client_id, "connection lost");

    if session.expiry.should_save() {
        tracing::trace!(client_id, "session saved");
        inactive_sessions.insert(client_id, session);
    } else {
        tracing::trace!(client_id, "session expired");
    }
}
