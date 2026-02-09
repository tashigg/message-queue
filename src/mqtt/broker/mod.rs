use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;

use color_eyre::eyre::{self, Context};
use der::Encode;
use futures::future::OptionFuture;
use rand::RngCore;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use slotmap::SlotMap;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_rustls::rustls;
use tokio_util::sync::CancellationToken;

use crate::collections::{hash_map, HashMap};
use tashi_vertex::{Engine, Transaction as TvTransaction};

use connection::Connection;

use crate::cli::run::WsConfig;
use crate::config::permissions::PermissionsConfig;
use crate::config::users::UsersConfig;
use crate::mqtt::broker::socket::{DirectSocket, MqttSocket};
use crate::mqtt::broker::tls::TlsAcceptor;
use crate::mqtt::broker::websocket::WebsocketAcceptor;
use crate::mqtt::router::{system_publish, MqttRouter, RouterHandle, TceState};
use crate::mqtt::session::{InactiveSessions, SessionStore};
use crate::mqtt::KeepAlive;
use crate::mqtt::{client_id, ClientId, ClientIndex, ConnectionId};
use crate::password::PasswordHashingPool;
use crate::transaction::{TimestampSeconds, Transaction, TransactionData};

use super::session::Will;

mod connection;
mod socket;
mod tls;
mod websocket;

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
    tls: Option<TlsAcceptor>,
    websocket: Option<WebsocketAcceptor>,

    token: CancellationToken,

    clients: Clients,


    /// Store information about a connection awaiting another connection to exit to take it over.
    ///
    /// When `key` closes out, `value` takes over the session immediately,
    /// if the value is a `clean_session` or the session expires on disconnect, a new session will be created instead.
    pending_session_takeovers: HashMap<ConnectionId, SessionTakeover>,

    /// Generator for `ConnectionId`s
    connections: SlotMap<ConnectionId, CancellationToken>,
    tasks: JoinSet<ConnectionData>,

    shared: Arc<Shared>,

    broker_rx: mpsc::Receiver<BrokerEvent>,

    inactive_sessions: InactiveSessions,

    router: MqttRouter,
}


struct SessionTakeover {
    client_id: ClientId,
    assigned: bool,
    client_index: ClientIndex,
    clean_session: bool,
    response_tx: oneshot::Sender<ConnectionAcceptedResponse>,
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
        client_id: Option<ClientId>,
        clean_session: bool,
        response_tx: oneshot::Sender<ConnectionAcceptedResponse>,
    },
}

struct ConnectionAcceptedResponse {
    assigned_client_id: Option<ClientId>,
    client_index: ClientIndex,
    session: Option<SessionStore>,
}

struct Shared {
    password_hasher: PasswordHashingPool,
    users: UsersConfig,
    broker_tx: mpsc::Sender<BrokerEvent>,
    engine: Option<Arc<Engine>>,
    router: RouterHandle,

    max_keep_alive: KeepAlive,
}

struct ConnectionData {
    id: ConnectionId,
    client_id: Option<ClientId>,
    client_index: Option<ClientIndex>,
    store: SessionStore,
}

pub struct TlsConfig {
    pub socket_addr: SocketAddr,
    pub cert_chain: Vec<CertificateDer<'static>>,
    pub key: PrivateKeyDer<'static>,
}

impl MqttBroker {
    #[allow(clippy::too_many_arguments)]
    pub async fn bind(
        listen_addr: SocketAddr,
        tls_config: Option<TlsConfig>,
        ws_config: Option<WsConfig>,
        users: UsersConfig,
        permissions_config: PermissionsConfig,
        tce: Option<TceState>,
        max_keep_alive: KeepAlive,
    ) -> crate::Result<Self> {
        let token = CancellationToken::new();

        let listener = TcpListener::bind(listen_addr)
            .await
            .wrap_err_with(|| format!("failed to bind listen_addr: {}", listen_addr))?;

        let tls = match tls_config {
            Some(tls_config) => Some(
                TlsAcceptor::bind(tls_config)
                    .await
                    .wrap_err("failed to create TlsAcceptor")?,
            ),
            None => None,
        };

        let websocket = match ws_config {
            Some(ws_config) => Some(
                WebsocketAcceptor::bind(ws_config.websockets_addr, token.child_token())
                    .await
                    .wrap_err("failed to create WebsocketAcceptor")?,
            ),
            None => None,
        };

        let (broker_tx, broker_rx) = mpsc::channel(100);

        let engine = tce.as_ref().map(|tce| tce.engine.clone());

        let router = MqttRouter::start(tce, token.clone(), permissions_config);

        Ok(MqttBroker {
            listen_addr,
            listener,
            tls,
            websocket,
            token,
            clients: Default::default(),
            pending_session_takeovers: Default::default(),
            connections: SlotMap::with_capacity_and_key(256),
            tasks: JoinSet::new(),
            shared: Arc::new(Shared {
                password_hasher: PasswordHashingPool::new(
                    std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN),
                ),
                users,
                broker_tx: broker_tx.clone(),
                engine,
                router: router.handle(),
                max_keep_alive,
            }),
            broker_rx,
            inactive_sessions: Default::default(),
            router,
        })
    }

    pub async fn run(&mut self) -> crate::Result<()> {
        tracing::info!(listen_addr = %self.listen_addr, "listening for connections");

        let mut shutdown = false;

        // hack: borrowck limitations means we'd have an overlapping borrow.
        let _engine = self.shared.engine.clone();

        while !shutdown {
            let tls_accept_fut = OptionFuture::from(self.tls.as_mut().map(|it| it.accept()));

            let ws_accept_fut = OptionFuture::from(self.websocket.as_mut().map(|it| it.accept()));

            tokio::select! {
                _ = self.token.cancelled() => {
                    shutdown = true;
                }
                Some(Ok(data)) = self.tasks.join_next() => {
                    let conn_id = data.id;
                    if let Some(client_index) = data.client_index {
                        self.clients.by_index[client_index].connection_id = None;
                        self.router.disconnected(client_index, conn_id);
                    }

                    self.connections.remove(conn_id);
                    handle_connection_lost(
                        &mut self.inactive_sessions,
                        &mut self.router,
                        self.shared.engine.as_deref(),
                        data
                    );

                    if let Some(takeover) = self.pending_session_takeovers.remove(&conn_id) {
                        self.handle_session_takeover(takeover);
                    }
                }
                res = self.listener.accept() => {
                    self.handle_accept(res);
                }
                Some(res) = tls_accept_fut => {
                    let socket = res.wrap_err("error from TlsAcceptor")?;
                    self.add_connection(socket);
                }
                Some(res) = ws_accept_fut => {
                    let socket = res.wrap_err("error from WebsocketAcceptor")?;
                    self.add_connection(socket);
                }
                Some(event) = self.broker_rx.recv() => {
                    self.handle_event(event).await;
                }
                res = self.router.task_result() => {
                    res.wrap_err("error from router task")?;
                    eyre::bail!("router task exited unexpectedly");
                }
                Some(event) = self.inactive_sessions.next_event() => {
                    self.handle_inactive_session_event(event)?;
                }
            }
        }

        Ok(())
    }

    fn handle_inactive_session_event(&mut self, event: super::session::Event) -> crate::Result<()> {
        let client_id = match &event {
            super::session::Event::Expiration(id, _)
            | super::session::Event::WillElapsed(id, _) => *id,
        };

        let Some(client_idx) = self.clients.index_from_id(&client_id) else {
            return Ok(());
        };

        match event {
            // if we have other on-expiration actions, now would be the time to do them.
            super::session::Event::Expiration(_, session) => {
                // currently the only on-expiration event is publishing the `last_will`.
                if let Some(will) = session.last_will {
                    execute_will(
                        &mut self.router,
                        self.shared.engine.as_deref(),
                        client_id,
                        client_idx,
                        will,
                    );
                }

                self.router.evict_client(client_idx);
                self.clients.remove(client_idx);
            }

            super::session::Event::WillElapsed(_, will) => execute_will(
                &mut self.router,
                self.shared.engine.as_deref(),
                client_id,
                client_idx,
                will,
            ),
        }

        Ok(())
    }

    fn handle_accept(&mut self, result: std::io::Result<(TcpStream, SocketAddr)>) {
        match result {
            Ok((stream, remote_addr)) => {
                tracing::info!(%remote_addr, "accepted new connection");

                // Disable Nagle's algorithm since we always send complete packets.
                // https://en.wikipedia.org/wiki/Nagle's_algorithm
                if let Err(e) = stream.set_nodelay(true) {
                    // It's unclear how this could actually fail and what it means when it does.
                    tracing::debug!(?e, "error setting TCP_NODELAY on socket");
                }

                self.add_connection(DirectSocket::new(remote_addr, stream));
            }
            // TODO: Some kinds of accept failures are probably fatal
            Err(e) => tracing::error!(?e, "accept failed"),
        }
    }

    fn add_connection<S: MqttSocket>(&mut self, socket: S) {
        let child_token = self.token.child_token();
        let connection_id = self.connections.insert(child_token.clone());

        let conn = Connection::new(connection_id, socket, child_token, self.shared.clone());

        self.tasks.spawn(conn.run());
    }

    fn handle_session_takeover(&mut self, takeover: SessionTakeover) {
        let SessionTakeover {
            client_id,
            assigned,
            client_index,
            clean_session,
            response_tx,
        } = takeover;

        let session = match (clean_session, self.inactive_sessions.claim(&client_id)) {
            (false, Some(store)) => {
                tracing::trace!(%client_id, "existing session was resumed");
                Some(store)
            }
            (false, None) => {
                tracing::trace!(
                    %client_id,
                    "client requested to resume session, none was found"
                );
                None
            }
            (true, Some(mut store)) => {
                if let Some(last_will) = store.session.last_will.take() {
                    execute_will(
                        &mut self.router,
                        self.shared.engine.as_deref(),
                        client_id,
                        client_index,
                        last_will,
                    )
                }

                tracing::trace!(%client_id, "existing session was dropped");
                None
            }
            (true, None) => {
                tracing::trace!(%client_id, "starting clean session");
                None
            }
        };

        // The client might have disconnected, closing the channel.
        if let Err(response) = response_tx.send(ConnectionAcceptedResponse {
            assigned_client_id: assigned.then_some(client_id),
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

    async fn handle_event(&mut self, event: BrokerEvent) {
        match event {
            BrokerEvent::ConnectionAccepted {
                connection_id,
                client_id,
                clean_session,
                response_tx,
            } => {
                let (client_id, assigned_client_id, client_index) = match client_id {
                    Some(client_id) => (client_id, None, self.clients.get_or_insert(client_id)),
                    None => {
                        let (client_id, client_index) = self.clients.insert_new_unique_id();
                        (client_id, Some(client_id), client_index)
                    }
                };

                let takeover_data = SessionTakeover {
                    client_id,
                    assigned: assigned_client_id.is_some(),
                    client_index,
                    clean_session,
                    response_tx,
                };

                let Some(replaced_connection) = self.clients.by_index[client_index]
                    .connection_id
                    .replace(connection_id)
                else {
                    // no active connection to replace, so just take over the session.
                    self.handle_session_takeover(takeover_data);
                    return;
                };

                tracing::trace!(%client_id, "beginning session replacement for client ID");

                // cancel the old connection.
                // todo: we should convince the old connection to send the appropriate Disconnect packet.
                self.connections[replaced_connection].cancel();

                self.pending_session_takeovers
                    .insert(replaced_connection, takeover_data);
            }
        }
    }

    pub fn connections(&self) -> usize {
        self.tasks.len()
    }

    pub async fn shutdown(self) -> crate::Result<()> {
        // drop all the parts we don't care about to avoid deadlocks.
        // Notably:
        // 1. `self.listener`.
        //    Close pending connections and stop listening for new ones.
        // 2. `self.broker_rx`
        //    Ignore messages from Connections to avoid deadlocks (IE, "replace sessions with this client ID, please").
        // 3. `self.deferred_session_takeovers`
        //    drop any outstating connection takeovers as we will not be completing them.
        let Self {
            mut tasks,
            mut inactive_sessions,
            mut router,
            shared,
            ..
        } = self;
        // Closes any pending connections and stops listening for new ones.
        drop(self.listener);

        system_publish("$SYS/notices", "shutting down");

        self.token.cancel();

        while let Some(Ok(data)) = tasks.join_next().await {
            handle_connection_lost(
                &mut inactive_sessions,
                &mut router,
                shared.engine.as_deref(),
                data,
            );
            tracing::info!("{} connections remaining", tasks.len());
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
                vacant.insert(index);

                ClientState {
                    id,
                    connection_id: None,
                }
            }),
        }
    }

    /// Generate, insert and return a random `ClientId` that is known not to be in-use by this broker.
    fn insert_new_unique_id(&mut self) -> (ClientId, ClientIndex) {
        let mut rng = rand::thread_rng();

        // A collision is unlikely if `ThreadRng` has good entropy,
        // but may be possible if this is a VM that just booted.
        //
        // It could also happen to be a random collision.
        // Let's do some math on the odds of that happening:
        // 64 allowed characters (0-9, A-Z, a-z, -, _) times a length of 23
        // gives us 64^23 = 2^(6*23) = 2^138 possible client IDs, or 138 bits of entropy.
        //
        // If we plug that into the formula given here:
        // https://en.wikipedia.org/wiki/Birthday_attack#Mathematics
        // For a 1 in a million probability of collision,
        // we need to generate approximately 834 quadrillion client IDs.
        //
        // However, if we only generated 16 character client IDs, equaling 96 bits of entropy,
        // that drops to "just" 398 billion.
        //
        // Either way, we should keep trying (at least for a reasonable amount of time)
        // if the first ID we generate collides with an existing one.
        for i in 0..=1000 {
            if [10, 100, 200, 500, 1000].contains(&i) {
                // This is unlikely enough that we should yell about it.
                tracing::warn!(
                    known_client_ids = self.by_id.len(),
                    "failed to generate an unused client ID after {i} attempts"
                );

                // `ThreadRng` reseeds from the OS every 64 KiB,
                // so read at least that much to force a reseed.
                //
                // We could duplicate the reseeding behavior with more control if we used
                // `ReseedingRng<StdRng, OsRng>` but that's missing fork detection.
                //
                // Using `ThreadRng` also helps to obscure the output
                // because it's used by a bunch of other things,
                // so even if some statistical correlation is possible to find in its output,
                // just looking at generated client IDs is probably not enough information.
                //
                // *And*, if the broker task gets moved to another thread,
                // that gives us a new RNG with, most likely, a completely different seed.
                let mut buf = vec![0u8; 1024 * 64];
                rng.fill_bytes(&mut buf);
            }

            // There's no real reason not to generate a full-length ID since it wouldn't save memory
            // on our end either way, and it's only sent once or twice over the wire, so it wouldn't
            // save bandwidth either.
            //
            // We can increase the number of possible client IDs by randomizing the _length_ of IDs,
            // but that doesn't even double the size of the search space.
            let client_id = ClientId::generate(&mut rng, client_id::MAX_LEN);

            if let hash_map::Entry::Vacant(vacant) = self.by_id.entry(client_id) {
                // 99.9999% of the time, this is going to be the happy path.
                let client_index = self.by_index.insert_with_key(move |index| {
                    vacant.insert(index);

                    ClientState {
                        id: client_id,
                        connection_id: None,
                    }
                });

                return (client_id, client_index);
            }

            tracing::trace!(%client_id, "generated a client ID that is already known");
        }

        // We failed to generate a unique client ID after 1000 attempts and *at least* 5 reseedings.
        // It's probably better to die than loop forever at this point.
        panic!("thread_rng() is broken")
    }

    fn remove(&mut self, index: ClientIndex) -> Option<ClientState> {
        let state = self.by_index.remove(index)?;
        self.by_id.remove(&state.id);

        Some(state)
    }
}

// This is a free function because it would require a borrow of Broker while
// listener is partially moved in shutdown.
fn handle_connection_lost(
    inactive_sessions: &mut InactiveSessions,
    router: &mut MqttRouter,
    engine: Option<&Engine>,
    data: ConnectionData,
) {
    let ConnectionData {
        client_id,
        client_index,
        store: session,
        ..
    } = data;

    let Some(client_id) = client_id else {
        return;
    };

    tracing::trace!(%client_id, "connection lost");

    if session.expiry.should_save() {
        tracing::trace!(%client_id, "session saved");
        inactive_sessions.insert(client_id, session);
    } else {
        tracing::trace!(%client_id, "session expired");
        // will is published on session close (if present).

        if let Some(will) = session.session.last_will {
            let Some(client_index) = client_index else {
                return;
            };

            execute_will(
                router,
                engine,
                client_id,
                client_index,
                will,
            )
        }
    }
}

fn dispatch_will(
    router: &mut MqttRouter,
    engine: Option<&Engine>,
    client_id: ClientId,
    client_idx: ClientIndex,
    mut will: Will,
) {
    // we have to reset the timestamp_received because we're only publishing the will *now*.
    will.transaction.timestamp_received = TimestampSeconds::now();
    tracing::trace!("submitting transaction: {:?}", will.transaction);

    let transaction = Transaction {
        data: TransactionData::Publish(will.transaction.clone()),
    };


    if let Some(engine) = engine {
        match create_tv_transaction(&transaction) {
            Ok(tv_txn) => {
                if let Err(e) = engine.send_transaction(tv_txn) {
                     tracing::error!(%client_id, "Failed to submit will transaction: {e}");
                }
            }
            Err(e) => {
                 tracing::error!(%client_id, "Failed to encode will transaction: {e}");
            }
        }
    }

    let TransactionData::Publish(transaction) = transaction.data;

    router.publish_will(client_idx, transaction);
}

fn create_tv_transaction(txn: &Transaction) -> crate::Result<TvTransaction> {
    let der = txn.to_der().map_err(Into::<color_eyre::Report>::into)?;
    let mut tv_txn = TvTransaction::allocate(der.len());
    tv_txn.copy_from_slice(&der);
    Ok(tv_txn)
}

fn execute_will(
    router: &mut MqttRouter,
    engine: Option<&Engine>,
    client_id: ClientId,
    client_idx: ClientIndex,
    will: Will,
) {
    dispatch_will(router, engine, client_id, client_idx, will)
}
