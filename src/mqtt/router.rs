use std::num::NonZeroU32;
use std::ops::{Index, IndexMut};
use std::sync::{Arc, OnceLock};

use bytes::Bytes;
use color_eyre::eyre;
use color_eyre::eyre::WrapErr;
use der::Decode;
use slotmap::SecondaryMap;
use tashi_collections::{HashMap, HashSet};
use tashi_consensus_engine::{CreatorId, Message, MessageStream, Platform, PlatformEvent};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tokio::task;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::Span;

use rumqttd_protocol::{QoS, RetainForwardRule, SubscribeReasonCode, UnsubAckReason};

use crate::map_join_error;
use crate::mqtt::mailbox::MailSender;
use crate::mqtt::packets::PacketId;
use crate::mqtt::trie::{self, Filter, FilterTrie, TopicName};
use crate::mqtt::{ClientId, ClientIndex, ConnectionId};
use crate::tce_message::{
    BytesAsOctetString, PublishMeta, PublishTrasaction, TimestampSeconds, Transaction,
    TransactionData,
};

// Tokio's channels allocate in slabs of 32.
const COMMAND_CAPACITY: usize = 128;
static SYSTEM_TX: OnceLock<mpsc::UnboundedSender<SystemCommand>> = OnceLock::new();

pub struct MqttRouter {
    system_tx: mpsc::UnboundedSender<SystemCommand>,
    command_tx: mpsc::Sender<(ClientIndex, RouterCommand)>,
    task: JoinHandle<crate::Result<()>>,
}

pub struct RouterHandle {
    command_tx: mpsc::Sender<(ClientIndex, RouterCommand)>,
}

pub struct RouterConnection {
    client_index: ClientIndex,
    command_tx: mpsc::Sender<(ClientIndex, RouterCommand)>,
    message_rx: mpsc::UnboundedReceiver<RouterMessage>,
}

#[derive(Debug, Clone)]
pub struct FilterProperties {
    pub qos: QoS,
    pub nolocal: bool,
    pub preserve_retain: bool,
    pub retain_forward_rule: RetainForwardRule,
}

// `rumqttd` types subscription IDs as `usize` but the spec states a max value that fits in `u32`.
// It also cannot be zero, so we can wrap it in `Option` for free using `NonZeroU32`.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionId(NonZeroU32);

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
            clients: SecondaryMap::new(),
            dead_clients: HashSet::default(),
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

    pub fn disconnected(&mut self, client_index: ClientIndex, conn_id: ConnectionId) {
        self.system_tx
            .send(SystemCommand::Disconnected {
                client_index,
                conn_id,
            })
            .ok();
    }

    pub fn publish_will(&mut self, client_index: ClientIndex, txn: PublishTrasaction) {
        self.system_tx
            .send(SystemCommand::PublishWill {
                willing_client: client_index,
                txn,
            })
            .ok();
    }

    pub fn evict_client(&mut self, client_index: ClientIndex) {
        self.system_tx
            .send(SystemCommand::EvictClient { client_index })
            .ok();
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
        client_index: ClientIndex,
        client_id: ClientId,
        mail_tx: MailSender,
        clean_session: bool,
    ) -> crate::Result<RouterConnection> {
        let (message_tx, message_rx) = mpsc::unbounded_channel();

        self.command_tx
            .send((
                client_index,
                RouterCommand::NewConnection {
                    connection_id,
                    client_id,
                    message_tx,
                    mail_tx,
                    clean_session,
                },
            ))
            .await
            .map_err(|_| eyre::eyre!("Router shut down"))?;

        Ok(RouterConnection {
            client_index,
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
                self.client_index,
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
            .send((self.client_index, RouterCommand::Transaction(transaction)))
            .await;
    }

    pub async fn unsubscribe(&mut self, packet_id: PacketId, filters: Vec<Filter>) {
        let _ = self
            .command_tx
            .send((
                self.client_index,
                RouterCommand::Unsubscribe { packet_id, filters },
            ))
            .await;
    }
}

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
        connection_id: ConnectionId,
        client_id: ClientId,
        mail_tx: MailSender,
        message_tx: mpsc::UnboundedSender<RouterMessage>,
        clean_session: bool,
    },
    Subscribe {
        packet_id: PacketId,
        sub_id: Option<SubscriptionId>,
        /// The filters to subscribe with.
        ///
        /// If any index is `None`, that filter failed validation in the frontend.
        filters: Vec<Option<(Filter, FilterProperties)>>,
    },
    Unsubscribe {
        packet_id: PacketId,
        filters: Vec<Filter>,
    },
    Transaction(Transaction),
}

enum SystemCommand {
    Disconnected {
        client_index: ClientIndex,
        conn_id: ConnectionId,
    },
    Publish {
        source: Span,
        txn: PublishTrasaction,
    },
    // Avoid blocking the broker task by using a system command instead of a router command.
    PublishWill {
        willing_client: ClientIndex,
        txn: PublishTrasaction,
    },
    EvictClient {
        client_index: ClientIndex,
    },
}

pub enum RouterMessage {
    SubAck {
        packet_id: PacketId,
        return_codes: Vec<SubscribeReasonCode>,
    },
    UnsubAck {
        packet_id: PacketId,
        return_codes: Vec<UnsubAckReason>,
    },
    // PubAck {
    //     packet_id: PacketId,
    // },
}

struct RouterState {
    token: CancellationToken,

    clients: SecondaryMap<ClientIndex, ClientState>,
    dead_clients: HashSet<ClientIndex>,

    subscriptions: Subscriptions,
    command_rx: mpsc::Receiver<(ClientIndex, RouterCommand)>,
    system_rx: mpsc::UnboundedReceiver<SystemCommand>,

    tce_platform: Arc<Platform>,
    tce_messages: MessageStream,
}

/// State associated with a given client ID (i.e. session state).
struct ClientState {
    client_id: ClientId,
    mail_tx: MailSender,
    subscriptions: ClientSubscriptions,
    current_connection: Option<ConnectionState>,
    clean_session: bool,
}

/// State associated with a given connection.
struct ConnectionState {
    connection_id: ConnectionId,
    message_tx: mpsc::UnboundedSender<RouterMessage>,
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
struct ClientSubscriptions {
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

// fixme: this should probably associate by client ID instead of connection ID.
// Changing that probably means changing other things too to match.
type SubscriptionMap = FilterTrie<HashMap<ClientIndex, Subscription>>;

struct Subscription {
    id: Option<SubscriptionId>,
    props: FilterProperties,
}

enum PublishOrigin<'a> {
    System { source: Span },
    Local(ClientIndex),
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

impl Index<SubscriptionKind> for ClientSubscriptions {
    type Output = HashSet<trie::EntryId>;

    fn index(&self, kind: SubscriptionKind) -> &Self::Output {
        match kind {
            SubscriptionKind::App => &self.app,
            SubscriptionKind::Sys => &self.sys,
        }
    }
}

impl IndexMut<SubscriptionKind> for ClientSubscriptions {
    fn index_mut(&mut self, kind: SubscriptionKind) -> &mut Self::Output {
        match kind {
            SubscriptionKind::App => &mut self.app,
            SubscriptionKind::Sys => &mut self.sys,
        }
    }
}

impl SubscriptionKind {
    const ALL: &'static [Self] = &[Self::App, Self::Sys];

    fn from_filter(filter: &Filter) -> Result<Self, InvalidSubscriptionKind> {
        if let Some(root) = filter.root_literal() {
            if root.starts_with('$') {
                if root.starts_with("$SYS") {
                    return Ok(Self::Sys);
                }

                return Err(InvalidSubscriptionKind);
            }
        }

        Ok(Self::App)
    }
}

impl RouterState {
    fn evict_client(&mut self, client_idx: ClientIndex) {
        let Some(mut client_state) = self.clients.remove(client_idx) else {
            return;
        };

        for &kind in SubscriptionKind::ALL {
            for place_id in client_state.subscriptions[kind].drain() {
                if let Some(mut entry) = self.subscriptions[kind].entry_by_id(place_id) {
                    entry.get_mut().remove(&client_idx);
                    entry.remove_if(|map| map.is_empty());
                }
            }
        }
    }
}

impl ClientState {
    fn try_send(&mut self, message: RouterMessage) -> Result<(), RouterMessage> {
        let Some(conn) = &mut self.current_connection else {
            return Err(message);
        };

        if let Err(e) = conn.message_tx.send(message) {
            self.current_connection = None;
            return Err(e.0);
        }

        Ok(())
    }
}

/// A filter has an invalid namespace for the server.
struct InvalidSubscriptionKind;

// Experimenting with using free functions instead of methods for less right-drift in core logic.
// Methods should be added to `RouterState` or its constituent types
// when they make sense for directly updating some state.
#[tracing::instrument(name = "router", skip(state))]
async fn run(mut state: RouterState) -> crate::Result<()> {
    loop {
        tokio::select! {
            msg = state.command_rx.recv() => {
                let Some((client, cmd)) = msg else {
                    tracing::debug!("Command channel closed; exiting.");
                    break;
                };

                handle_command(&mut state, client, cmd);
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
fn handle_command(state: &mut RouterState, client_idx: ClientIndex, command: RouterCommand) {
    match command {
        RouterCommand::NewConnection {
            connection_id,
            client_id,
            message_tx,
            mail_tx,
            clean_session,
        } => {
            if state.clients.contains_key(client_idx) {
                if !clean_session {
                    state.clients[client_idx].current_connection = Some(ConnectionState {
                        connection_id,
                        message_tx,
                    });
                    return;
                }

                state.evict_client(client_idx);
            }

            // NOTE: this will silently replace the state for an old client/connection
            // if it wasn't already evicted. Ideally, we would have already noticed the connection
            // was dead before we try to re-use the slot.
            //
            // `SecondaryMap` doesn't currently have a way to tell the difference:
            // https://github.com/orlp/slotmap/issues/55#issuecomment-1972072068
            state.clients.insert(
                client_idx,
                ClientState {
                    client_id,
                    mail_tx,
                    subscriptions: Default::default(),
                    current_connection: Some(ConnectionState {
                        connection_id,
                        message_tx,
                    }),
                    clean_session,
                },
            );
        }
        RouterCommand::Transaction(txn) => match txn.data {
            TransactionData::Publish(publish) => {
                dispatch(state, publish, PublishOrigin::Local(client_idx))
            }
        },
        RouterCommand::Subscribe {
            packet_id,
            sub_id,
            filters,
        } => {
            if !state.clients.contains_key(client_idx) {
                return;
            }

            // if state.connections[conn_id].message_tx.is_closed() {
            //     return;
            // }

            let reasons: Vec<_> = filters
                .into_iter()
                .map(|maybe_filter| {
                    maybe_filter
                        // Codes for `None` filters should be overwritten by the connection
                        // as they would have failed validation on the frontend.
                        .ok_or(SubscribeReasonCode::Unspecified)
                        .and_then(|(filter, props)| {
                            let sub_kind = SubscriptionKind::from_filter(&filter)
                                .map_err(|_| SubscribeReasonCode::NotAuthorized)?;

                            let qos = props.qos;

                            let (map, place_id) =
                                state.subscriptions[sub_kind].entry(filter).or_default();

                            map.insert(client_idx, Subscription { id: sub_id, props });

                            state.clients[client_idx].subscriptions[sub_kind].insert(place_id);

                            Ok(SubscribeReasonCode::Success(qos))
                        })
                        // Surprisingly there's no easy way to flatten a `Result<T, T>` to `T`
                        .unwrap_or_else(|code| code)
                })
                .collect();

            state.clients[client_idx]
                .try_send(RouterMessage::SubAck {
                    packet_id,
                    return_codes: reasons,
                })
                .ok();
        }
        RouterCommand::Unsubscribe { packet_id, filters } => {
            if !state.clients.contains_key(client_idx) {
                return;
            }

            let reasons: Vec<_> = filters
                .into_iter()
                .map(|filter| {
                    let sub_kind = match SubscriptionKind::from_filter(&filter) {
                        Ok(it) => it,
                        Err(_) => return UnsubAckReason::NoSubscriptionExisted,
                    };

                    let trie::Entry::Occupied(mut entry) =
                        state.subscriptions[sub_kind].entry(filter)
                    else {
                        return UnsubAckReason::NoSubscriptionExisted;
                    };

                    entry.get_mut().remove(&client_idx);
                    let entry_id = entry.entry_id();
                    entry.remove_if(|map| map.is_empty());

                    let removed =
                        state.clients[client_idx].subscriptions[sub_kind].remove(&entry_id);

                    if removed {
                        UnsubAckReason::Success
                    } else {
                        UnsubAckReason::NoSubscriptionExisted
                    }
                })
                .collect();

            state.clients[client_idx]
                .try_send(RouterMessage::UnsubAck {
                    packet_id,
                    return_codes: reasons,
                })
                .ok();
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
            client_index,
            conn_id,
        } => {
            if let Some(client) = state.clients.get_mut(client_index) {
                if client.clean_session {
                    // A Clean Start session is not retained after the client disconnects.
                    state.evict_client(client_index);
                    return;
                }

                if client
                    .current_connection
                    .as_ref()
                    .is_some_and(|conn| conn.connection_id == conn_id)
                {
                    client.current_connection = None;
                }
            }
        }
        SystemCommand::Publish { source, txn } => {
            dispatch(state, txn, PublishOrigin::System { source });
        }

        //
        SystemCommand::PublishWill {
            txn,
            willing_client,
        } => dispatch(state, txn, PublishOrigin::Local(willing_client)),

        SystemCommand::EvictClient { client_index } => {
            state.evict_client(client_index);
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
            PublishOrigin::Local(client_idx) => {
                // The topic should have been validated by the connection.
                panic!(
                    "BUG: attempting dispatch publish to invalid topic {:?} from locally connected client {:?}: {e:?}",
                    publish.topic,
                    state.clients[client_idx].client_id
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
            PublishOrigin::Local(client_idx) => {
                // The topic should have been validated by the connection.
                panic!(
                    "BUG: attempting dispatch publish to restricted topic {topic:?} from locally connected client {:?}",
                    state.clients[client_idx].client_id
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
        for (&client_idx, sub) in map.iter() {
            if !state.clients.contains_key(client_idx) {
                continue;
            }

            let _span = tracing::info_span!(
                "match",
                client_id = %state.clients[client_idx].client_id,
                topic = publish.topic
            )
            .entered();

            tracing::trace!("dispatching PUBLISH to client");

            let delivered =
                state.clients[client_idx]
                    .mail_tx
                    .deliver(sub.props.qos, sub.id, publish.clone());

            if !delivered {
                tracing::trace!("client channel closed; marking as dead");
                // Memoize dead clients for asynchronous cleanup so we can continue dispatching.
                state.dead_clients.insert(client_idx);
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
