use bytes::Bytes;
use color_eyre::eyre;
use color_eyre::eyre::WrapErr;
use der::Decode;
use std::num::NonZeroU32;
use std::ops::{Index, IndexMut};
use std::sync::{Arc, OnceLock};

use slotmap::SecondaryMap;
use tashi_collections::{HashMap, HashSet};
use tashi_consensus_engine::{CreatorId, Message, MessageStream, Platform, PlatformEvent};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::task;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Span;

use crate::map_join_error;
use rumqttd_shim::protocol::{QoS, RetainForwardRule, SubscribeReasonCode};

use crate::mqtt::packets::PacketId;
use crate::mqtt::trie::{self, Filter, FilterTrie, TopicName};
use crate::mqtt::{ClientId, ConnectionId};
use crate::tce_message::{
    BytesAsOctetString, PublishMeta, PublishTrasaction, TimestampSeconds, Transaction,
    TransactionData,
};

// Tokio's channels allocate in slabs of 32.
const COMMAND_CAPACITY: usize = 128;
static SYSTEM_TX: OnceLock<mpsc::UnboundedSender<SystemCommand>> = OnceLock::new();

pub struct MqttRouter {
    system_tx: mpsc::UnboundedSender<SystemCommand>,
    command_tx: mpsc::Sender<(ConnectionId, RouterCommand)>,
    task: JoinHandle<crate::Result<()>>,
}

pub struct RouterHandle {
    command_tx: mpsc::Sender<(ConnectionId, RouterCommand)>,
}

pub struct RouterConnection {
    connection_id: ConnectionId,
    command_tx: mpsc::Sender<(ConnectionId, RouterCommand)>,
    message_rx: mpsc::UnboundedReceiver<RouterMessage>,
}

#[derive(Debug, Clone)]
pub struct FilterProperties {
    pub qos: QoS,
    pub nolocal: bool,
    pub preserve_retain: bool,
    pub retain_forward_rule: RetainForwardRule,
}

impl MqttRouter {
    pub fn start(
        tce_platform: Arc<Platform>,
        tce_messages: MessageStream,
        token: CancellationToken,
    ) -> Self {
        let (command_tx, command_rx) = mpsc::channel(COMMAND_CAPACITY);

        let (system_tx, system_rx) = mpsc::unbounded_channel();

        SYSTEM_TX
            .set(system_tx.clone())
            .expect("BUG: MqttRouter initialized more than once!");

        let state = RouterState {
            token,
            connections: Default::default(),
            // This should round up to 256 buckets.
            dead_connections: HashSet::with_capacity_and_hasher(200, Default::default()),
            subscriptions: Subscriptions::default(),
            command_rx,
            system_rx,
            tce_platform,
            tce_messages,
        };

        // `rumqttd` runs their router in its own thread, we could do that here
        // but we want `run()` to be an `async fn` so it's easy to use `select!{}`.
        // I expect it to be largely CPU-bound, but due to Tokio's work-stealing nature
        // I don't expect that to cause issues.
        //
        // We could run it in its own thread with `Handle::block_on()`, but that won't cancel itself
        // when the runtime is shut down.
        //
        // `rumqttd` uses a handful of single-threaded Tokio runtimes instead of a multithreaded one
        // for some reason, possibly just copying Actix.
        let task = task::spawn(run(state));

        MqttRouter {
            system_tx,
            command_tx,
            task,
        }
    }

    pub fn disconnected(&mut self, conn_id: ConnectionId) {
        let _ = self.system_tx.send(SystemCommand::Disconnected { conn_id });
    }

    pub fn handle(&self) -> RouterHandle {
        RouterHandle {
            command_tx: self.command_tx.clone(),
        }
    }

    pub async fn task_result(&mut self) -> crate::Result<()> {
        (&mut self.task).await.map_err(map_join_error)?
    }
}

impl RouterHandle {
    pub async fn connected(
        &self,
        connection_id: ConnectionId,
        client_id: ClientId,
    ) -> crate::Result<RouterConnection> {
        let (message_tx, message_rx) = mpsc::unbounded_channel();

        self.command_tx
            .send((
                connection_id,
                RouterCommand::NewConnection {
                    client_id,
                    message_tx,
                },
            ))
            .await
            .map_err(|_| eyre::eyre!("Router shut down"))?;

        Ok(RouterConnection {
            connection_id,
            command_tx: self.command_tx.clone(),
            message_rx,
        })
    }
}

impl RouterConnection {
    pub async fn next_message(&mut self) -> Option<RouterMessage> {
        self.message_rx.recv().await
    }

    pub async fn subscribe(
        &mut self,
        packet_id: PacketId,
        sub_id: Option<SubscriptionId>,
        filters: Vec<Option<(Filter, FilterProperties)>>,
    ) {
        // If the channel is closed,
        // the connection will notice the next time it calls `.next_message()`.
        let _ = self
            .command_tx
            .send((
                self.connection_id,
                RouterCommand::Subscribe {
                    packet_id,
                    sub_id,
                    filters,
                },
            ))
            .await;
    }

    pub async fn transaction(&mut self, transaction: Transaction) {
        let _ = self
            .command_tx
            .send((self.connection_id, RouterCommand::Transaction(transaction)))
            .await;
    }
}

// `rumqttd` types subscription IDs as `usize` but the spec states a max value that fits in `u32`.
// It also cannot be zero, so we can wrap it in `Option` for free using `NonZeroU32`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionId(NonZeroU32);

impl TryFrom<u32> for SubscriptionId {
    type Error = &'static str;

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        if value <= Self::MAX_VALUE {
            NonZeroU32::new(value)
                .map(SubscriptionId)
                .ok_or("Subscription ID cannot be zero")
        } else {
            Err(Self::MAX_VALUE_ERR)
        }
    }
}

// For conversion from `rumqttd`'s typing.
impl TryFrom<usize> for SubscriptionId {
    type Error = &'static str;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        u32::try_from(value)
            // If it doesn't fit in a `u32` it definitely exceeds the max value.
            .map_err(|_| Self::MAX_VALUE_ERR)
            .and_then(Self::try_from)
    }
}

impl From<SubscriptionId> for u32 {
    fn from(value: SubscriptionId) -> Self {
        value.0.get()
    }
}

impl From<SubscriptionId> for usize {
    fn from(value: SubscriptionId) -> Self {
        value.0.get() as usize
    }
}

impl SubscriptionId {
    // The spec states the maximum value in decimal, so doing the same here for consistency.
    const MAX_VALUE: u32 = 268_435_455; // Turns out to be 2^28 - 1

    const MAX_VALUE_ERR: &'static str = "subscription ID exceeds the max value of 268435455";
}

enum RouterCommand {
    NewConnection {
        client_id: ClientId,
        message_tx: mpsc::UnboundedSender<RouterMessage>,
    },
    Subscribe {
        packet_id: PacketId,
        sub_id: Option<SubscriptionId>,
        /// The filters to subscribe with.
        ///
        /// If any index is `None`, that filter failed validation in the frontend.
        filters: Vec<Option<(Filter, FilterProperties)>>,
    },
    Transaction(Transaction),
}

enum SystemCommand {
    Disconnected {
        conn_id: ConnectionId,
    },
    Publish {
        source: Span,
        txn: PublishTrasaction,
    },
}

pub enum RouterMessage {
    SubAck {
        packet_id: PacketId,
        return_codes: Vec<SubscribeReasonCode>,
    },
    // PubAck {
    //     packet_id: PacketId,
    // },
    Publish {
        sub_id: Option<SubscriptionId>,
        // This type should be small since it's just a couple `bool`s and `enum`s.
        props: FilterProperties,
        txn: Arc<PublishTrasaction>,
    },
}

struct RouterState {
    token: CancellationToken,

    connections: SecondaryMap<ConnectionId, ConnectionState>,
    /// The set of connections that are currently known to be dead.
    ///
    /// Populated during message dispatch and slowly drained because we don't want to block normal
    /// operations if we get a large chunk of connections dropping all at once.
    ///
    /// On disconnect, the connection should send a message but that could end up deep in the queue,
    /// and in the meantime we'll continue attempting to send to it unless we remember this.
    dead_connections: HashSet<ConnectionId>,

    subscriptions: Subscriptions,
    command_rx: mpsc::Receiver<(ConnectionId, RouterCommand)>,
    system_rx: mpsc::UnboundedReceiver<SystemCommand>,

    tce_platform: Arc<Platform>,
    tce_messages: MessageStream,
}

struct ConnectionState {
    client_id: ClientId,
    message_tx: mpsc::UnboundedSender<RouterMessage>,
    subscriptions: ConnectionSubscriptions,
}

#[derive(Default)]
struct Subscriptions {
    /// Application-level subscriptions: anything not prefixed with `$`
    app: SubscriptionMap,

    /// System-level subscriptions: everything prefixed with `$SYS`
    ///
    /// We have control over topics prefixed with `$`; clients cannot publish to them.
    /// https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901246
    ///
    /// Root-level wildcards are not allowed to match topics prefixed with `$`;
    /// the user is required to subscribe to a specific `$` prefix.
    ///
    /// Currently, we have no plans to implement other `$` prefixes because the `$SYS` namespace
    /// should be sufficient.
    // `FilterTrieMultiMap` has no knowledge of `$` prefixes; it made more sense to handle that
    // at this level.
    sys: SubscriptionMap,
}

#[derive(Default)]
struct ConnectionSubscriptions {
    /// Indexes into `Subscriptions::app`
    app: HashSet<trie::EntryId>,
    /// Indexes into `Subscriptions::sys`
    sys: HashSet<trie::EntryId>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum SubscriptionKind {
    /// Application-level subscriptions: anything not prefixed with `$`
    App,
    /// System-level subscriptions: everything prefixed with `$SYS`
    Sys,
}

type SubscriptionMap = FilterTrie<HashMap<ConnectionId, Subscription>>;

struct Subscription {
    id: Option<SubscriptionId>,
    props: FilterProperties,
}

enum PublishOrigin<'a> {
    System { source: Span },
    Local(ConnectionId),
    Consensus(&'a CreatorId),
}

impl Index<SubscriptionKind> for Subscriptions {
    type Output = SubscriptionMap;

    fn index(&self, kind: SubscriptionKind) -> &Self::Output {
        match kind {
            SubscriptionKind::App => &self.app,
            SubscriptionKind::Sys => &self.sys,
        }
    }
}

impl IndexMut<SubscriptionKind> for Subscriptions {
    fn index_mut(&mut self, kind: SubscriptionKind) -> &mut Self::Output {
        match kind {
            SubscriptionKind::App => &mut self.app,
            SubscriptionKind::Sys => &mut self.sys,
        }
    }
}

impl Index<SubscriptionKind> for ConnectionSubscriptions {
    type Output = HashSet<trie::EntryId>;

    fn index(&self, kind: SubscriptionKind) -> &Self::Output {
        match kind {
            SubscriptionKind::App => &self.app,
            SubscriptionKind::Sys => &self.sys,
        }
    }
}

impl IndexMut<SubscriptionKind> for ConnectionSubscriptions {
    fn index_mut(&mut self, kind: SubscriptionKind) -> &mut Self::Output {
        match kind {
            SubscriptionKind::App => &mut self.app,
            SubscriptionKind::Sys => &mut self.sys,
        }
    }
}

impl SubscriptionKind {
    const ALL: &'static [Self] = &[Self::App, Self::Sys];

    fn from_filter(filter: &Filter) -> Result<Self, SubscribeReasonCode> {
        if let Some(root) = filter.root_literal() {
            if root.starts_with('$') {
                if root.starts_with("$SYS") {
                    return Ok(Self::Sys);
                }

                return Err(SubscribeReasonCode::NotAuthorized);
            }
        }

        Ok(Self::App)
    }
}

impl RouterState {
    fn evict_connection(&mut self, conn_id: ConnectionId) {
        let Some(mut conn_state) = self.connections.remove(conn_id) else {
            return;
        };

        self.dead_connections.remove(&conn_id);

        for &kind in SubscriptionKind::ALL {
            for place_id in conn_state.subscriptions[kind].drain() {
                if let Some(mut entry) = self.subscriptions[kind].entry_by_id(place_id) {
                    entry.get_mut().remove(&conn_id);
                    entry.remove_if(|map| map.is_empty());
                }
            }
        }
    }

    fn pop_dead_connection(&mut self) {
        // `HashSet` annoyingly doesn't have a `.pop()` method despite it being somewhat trivial
        // to implement, I guess because `.pop()` implies stack- or heap-like operation.
        let Some(&conn_id) = self.dead_connections.iter().next() else {
            return;
        };

        self.evict_connection(conn_id);
    }
}

// Experimenting with using free functions instead of methods for less right-drift in core logic.
// Methods should be added to `RouterState` or its constituent types
// when they make sense for directly updating some state.
#[tracing::instrument(name = "router", skip(state))]
async fn run(mut state: RouterState) -> crate::Result<()> {
    loop {
        tokio::select! {
            msg = state.command_rx.recv() => {
                let Some((conn_id, cmd)) = msg else {
                    tracing::debug!("Command channel closed; exiting.");
                    break;
                };

                handle_command(&mut state, conn_id, cmd);
            },
            msg = state.system_rx.recv() => {
                handle_system_command(
                    &mut state,
                    msg.expect("BUG: system_rx cannot return None as its Sender is held in a `static`")
                );
            }
            res = state.tce_messages.next_message() => {
                let Some(msg) = res.wrap_err("error from MessageStream")? else {
                    tracing::debug!("Message stream closed; exiting.");
                    break;
                };

                handle_message(&mut state, msg);
            }
            // If we have dead connections, clean them up asynchronously.
            // `select!{}` will ensure that we continue to alternate handling actual messages.
            () = tokio::task::yield_now(), if !state.dead_connections.is_empty() => {
                state.pop_dead_connection();
            }
            _ = state.token.cancelled() => {
                break;
            }
        }
    }

    Ok(())
}

// NOTE: these functions are infallible because most errors should be handled where they occur
// or bubbled up as panics if they represent a fatal bug.
//
// This is to ideally prevent any transient error from killing the router.
fn handle_command(state: &mut RouterState, conn_id: ConnectionId, command: RouterCommand) {
    match command {
        RouterCommand::NewConnection {
            client_id,
            message_tx,
        } => {
            // NOTE: this will silently replace the state for an old connection
            // if it wasn't already evicted. Ideally, we would have already noticed the connection
            // was dead before we try to re-use the slot.
            //
            // `SecondaryMap` doesn't currently have a way to tell the difference:
            // https://github.com/orlp/slotmap/issues/55#issuecomment-1972072068
            state.connections.insert(
                conn_id,
                ConnectionState {
                    client_id,
                    message_tx,
                    subscriptions: Default::default(),
                },
            );
        }
        RouterCommand::Transaction(txn) => match txn.data {
            TransactionData::Publish(publish) => {
                dispatch(state, publish, PublishOrigin::Local(conn_id))
            }
        },
        RouterCommand::Subscribe {
            packet_id,
            sub_id,
            filters,
        } => {
            if !state.connections.contains_key(conn_id) {
                return;
            }

            if state.connections[conn_id].message_tx.is_closed() {
                state.evict_connection(conn_id);
                return;
            }

            let reasons: Vec<_> = filters
                .into_iter()
                .map(|maybe_filter| {
                    maybe_filter
                        // Codes for `None` filters should be overwritten by the connection
                        // as they would have failed validation on the frontend.
                        .ok_or(SubscribeReasonCode::Unspecified)
                        .and_then(|(filter, props)| {
                            let sub_kind = SubscriptionKind::from_filter(&filter)?;

                            let qos = props.qos;

                            let (map, place_id) =
                                state.subscriptions[sub_kind].entry(filter).or_default();

                            map.insert(conn_id, Subscription { id: sub_id, props });

                            state.connections[conn_id].subscriptions[sub_kind].insert(place_id);

                            Ok(SubscribeReasonCode::Success(qos))
                        })
                        // Surprisingly there's no easy way to flatten a `Result<T, T>` to `T`
                        .unwrap_or_else(|code| code)
                })
                .collect();

            let conn_closed = state.connections[conn_id]
                .message_tx
                .send(RouterMessage::SubAck {
                    packet_id,
                    return_codes: reasons,
                })
                .is_err();

            if conn_closed {
                state.evict_connection(conn_id);
            }
        }
    }
}

fn handle_message(state: &mut RouterState, message: Message) {
    match message {
        Message::SyncPoint(sync_point) => {
            // TODO: handle sync points
            tracing::debug!(?sync_point, "received sync point");
        }
        Message::Event(event) => {
            handle_tce_event(state, event);
        }
    }
}

#[tracing::instrument(skip_all, fields(creator=%event.creator, timestamp=event.timestamp_created()))]
fn handle_tce_event(state: &mut RouterState, event: PlatformEvent) {
    // Way too noisy if we don't filter this.
    if !event.application_transactions().is_empty() {
        tracing::trace!(
            "received event with {} application transactions",
            event.application_transactions().len()
        );
    }

    if &event.creator == state.tce_platform.creator_id() {
        // TODO: we'll want to check if our own events are coming to consensus for QoS 1 and 2
        // and re-send them if not.
        //
        // For dispatching local messages, expect them to come down the `command_tx` channel.
        // That will save us having to redundantly decode them.
        return;
    }

    for (idx, transaction) in event.application_transactions().iter().enumerate() {
        match Transaction::from_der(transaction.as_bytes()) {
            Ok(Transaction {
                data: TransactionData::Publish(publish),
            }) => {
                dispatch(state, publish, PublishOrigin::Consensus(&event.creator));
            }
            Err(e) => {
                // This isn't a fatal error.
                tracing::debug!(idx, "error decoding transaction from TCE: {e:?}");
            }
        }
    }
}

fn handle_system_command(state: &mut RouterState, command: SystemCommand) {
    match command {
        SystemCommand::Disconnected {
            conn_id: connnection_id,
        } => {
            state.evict_connection(connnection_id);
        }
        SystemCommand::Publish { source, txn } => {
            dispatch(state, txn, PublishOrigin::System { source });
        }
    }
}

fn dispatch(state: &mut RouterState, publish: PublishTrasaction, origin: PublishOrigin<'_>) {
    // Wrap in `Arc` for cheap clones.
    // This is a potential candidate for `triomphe::Arc` but `PublishTransaction` is so large
    // that it dwarfs the extra machine word for the weak count, and it's relatively short-lived.
    let publish = Arc::new(publish);

    // TODO: statically ensure `topic` is valid in `PublishTransaction`
    let topic = match TopicName::parse(&publish.topic) {
        Ok(topic) => topic,
        Err(e) => match origin {
            PublishOrigin::System { source } => {
                // The topic should have been validated by `system_publish()`.
                panic!(
                    "BUG: attempting dispatch publish to invalid topic {:?} from system component: {source:#?}\n{e:?}",
                    publish.topic,
                );
            }
            PublishOrigin::Local(conn_id) => {
                // The topic should have been validated by the connection.
                panic!(
                    "BUG: attempting dispatch publish to invalid topic {:?} from locally connected client {:?}: {e:?}",
                    publish.topic,
                    state.connections[conn_id].client_id
                );
            }
            PublishOrigin::Consensus(creator) => {
                // However, invalid publishes from other creators shouldn't take us down.
                tracing::debug!(topic=publish.topic, creator=%creator, "discarding publish to invalid topic: {e:?}");
                // TODO: should we start a vote to remove `creator` for sending an invalid publish?
                return;
            }
        },
    };

    // Validate restricted topic against the publish origin
    let kind = if topic.root().starts_with('$') {
        match origin {
            PublishOrigin::System { source } => {
                assert_eq!(
                    topic.root(), "$SYS",
                    // Should have been validated by `system_publish()`
                    "BUG: system component attempted to publish to a restricted topic not under `$SYS`: {topic:?}; {source:#?}"
                );
            }
            PublishOrigin::Local(conn_id) => {
                // The topic should have been validated by the connection.
                panic!(
                    "BUG: attempting dispatch publish to restricted topic {topic:?} from locally connected client {:?}",
                    state.connections[conn_id].client_id
                );
            }
            PublishOrigin::Consensus(creator) => {
                // However, invalid publishes from other creators shouldn't take us down.
                tracing::debug!(topic=publish.topic, creator=%creator, "discarding publish to restricted topic");
                // TODO: should we start a vote to remove `creator` for sending an invalid publish?
                return;
            }
        }

        // This should maybe be a fallible constructor of `SubscriptionKind`,
        // but it would be annoying to lift the control-flow and number of arguments.
        SubscriptionKind::Sys
    } else {
        SubscriptionKind::App
    };

    // fixme: len
    // tracing::trace!(
    //     "dispatching for {} potential filter matches",
    //     state.subscriptions[kind].len()
    // );

    state.subscriptions[kind].visit_matches(&topic, |map| {
        for (&conn_id, sub) in map.iter() {
            if state.dead_connections.contains(&conn_id) {
                continue;
            }

            let _span = tracing::info_span!(
                "match",
                client_id = state.connections[conn_id].client_id,
                topic = publish.topic
            )
            .entered();

            tracing::trace!("dispatching PUBLISH to client");

            let res = state.connections[conn_id]
                .message_tx
                .send(RouterMessage::Publish {
                    sub_id: sub.id,
                    props: sub.props.clone(),
                    txn: publish.clone(),
                });

            if res.is_err() {
                tracing::trace!("client channel closed; marking as dead");
                // Memoize dead connections for asynchronous cleanup so we can continue dispatching.
                state.dead_connections.insert(conn_id);
            }
        }
    });
}

/// Send a message to a `$SYS` topic.
///
/// ### Panics
/// If `topic` does not have `$SYS` as its root segment.
#[track_caller]
pub fn system_publish(topic: impl Into<String>, message: impl Into<Bytes>) {
    let source = Span::current();

    let topic = topic.into();
    let message = message.into();

    let parsed_topic = TopicName::parse(&topic)
        .unwrap_or_else(|e| panic!("BUG: attempting to publish to invalid topic {topic:?}: {e:?}"));

    assert_eq!(
        parsed_topic.root(),
        "$SYS",
        "BUG: attempting to publish to topic outside `$SYS` namespace: {topic:?}"
    );

    let Some(system_tx) = SYSTEM_TX.get() else {
        tracing::debug!(
            topic,
            "Attempting to publish to system topic before MqttRouter is initialized"
        );
        return;
    };

    let res = system_tx.send(SystemCommand::Publish {
        source,
        txn: PublishTrasaction {
            topic,
            meta: PublishMeta::new(QoS::AtMostOnce, false, false),
            payload: BytesAsOctetString(message),
            timestamp_received: TimestampSeconds::now(),
            properties: None,
        },
    });

    if let Err(SendError(SystemCommand::Publish { txn, .. })) = res {
        tracing::debug!(
            topic = txn.topic,
            "Attempted to publish to system topic, but MqttRouter is dead"
        );
    }
}
