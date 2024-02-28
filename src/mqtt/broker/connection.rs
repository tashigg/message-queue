use crate::mqtt::broker::{BrokerEvent, ConnectionData, Shared, TOPIC_ALIAS_MAX, TOPIC_MAX_LENGTH};
use crate::mqtt::packets::{IncomingPacketSet, IncomingSub, OutgoingPackets, PacketId};
use crate::mqtt::publish::ValidateError;
use crate::mqtt::router::{FilterProperties, RouterConnection, RouterMessage, SubscriptionId};
use crate::mqtt::session::Session;
use crate::mqtt::trie::Filter;
use crate::mqtt::{publish, ConnectionId};
use crate::tce_message::{Transaction, TransactionData};
use bytes::{Bytes, BytesMut};

use der::Encode;
use rand::distributions::{Alphanumeric, DistString};
use rumqttd_shim::protocol;
use rumqttd_shim::protocol::{
    ConnAck, ConnAckProperties, ConnectReturnCode, Disconnect, DisconnectProperties,
    DisconnectReasonCode, Packet, PingResp, Protocol, PubAck, PubAckReason, PubRec, PubRecReason,
    Publish, PublishProperties, QoS, SubAck, Subscribe, SubscribeProperties, SubscribeReasonCode,
    UnsubAck, UnsubAckReason,
};
use std::fmt::Display;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use tashi_collections::FnvHashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

pub struct Connection {
    id: ConnectionId,

    remote_addr: SocketAddr,

    client_id: String,

    protocol: protocol::v5::V5,
    stream: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,

    token: CancellationToken,
    shared: Arc<Shared>,
    session: Session,

    // The `u16` is client-generated which would suggest that we use a keyed hash map,
    // but it seems incredibly difficult to exploit this for a HashDOS attack if it's restricted
    // to the range `[1, N]` where N << 2^16.
    //
    // This is not stored with the session state as topic aliases are explicitly per-connection:
    // https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901113
    //
    // Also note that client->broker and broker->client PUBLISHes have separate topic alias spaces,
    // so we can punt on implementing the latter.
    client_topic_aliases: FnvHashMap<u16, String>,

    incoming_packets: IncomingPacketSet,
    outgoing_packets: OutgoingPackets,
}

#[derive(Debug, thiserror::Error)]
pub enum ConnectionError {
    #[error("closing connection due to packet error: {message}")]
    Disconnect {
        reason: DisconnectReasonCode,
        message: String,
    },
    #[error("protocol error: {0}")]
    Protocol(
        #[from]
        #[source]
        protocol::Error,
    ),
    #[error("error reading from socket: {0}")]
    ReadError(#[source] io::Error),
    #[error("error writing socket: {0}")]
    WriteError(#[source] io::Error),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ConnectionStatus {
    Running,
    Closed,
}

macro_rules! disconnect (
    ($reason:ident, $($message:tt)*) => {
        return Err(ConnectionError::Disconnect {
            reason: DisconnectReasonCode::$reason,
            message: format!($($message)*),
        })
    };
);

impl Connection {
    pub fn new(
        id: ConnectionId,
        stream: TcpStream,
        remote_addr: SocketAddr,
        token: CancellationToken,
        shared: Arc<Shared>,
    ) -> Self {
        Connection {
            id,
            remote_addr,
            client_id: String::new(),
            protocol: protocol::v5::V5,
            stream,
            read_buf: BytesMut::with_capacity(8192),
            write_buf: BytesMut::with_capacity(8192),
            token,
            shared,
            session: Session::default(),
            client_topic_aliases: FnvHashMap::default(),
            incoming_packets: Default::default(),
            outgoing_packets: OutgoingPackets::new(),
        }
    }

    #[tracing::instrument(name = "Connection::run", skip_all, fields(remote_addr=%self.remote_addr))]
    pub async fn run(mut self) -> ConnectionData {
        if let Err(e) = self.run_inner().await {
            match e.downcast::<ConnectionError>() {
                Ok(ConnectionError::Disconnect { reason, message }) => {
                    let _ = self.disconnect(reason, message).await;
                }
                Ok(ConnectionError::Protocol(e)) => {
                    let _ = self.disconnect(DisconnectReasonCode::ProtocolError, e.to_string());
                }
                Ok(other) => {
                    tracing::debug!("packet error from run_inner(): {other:?}");
                }
                Err(e) => {
                    tracing::debug!("miscellaneous error from run_inner(): {e:?}");
                }
            }
        }

        ConnectionData {
            id: self.id,
            client_id: self.client_id,
            session: self.session,
        }
    }

    async fn run_inner(&mut self) -> crate::Result<()> {
        // TODO: set timeouts on the `TcpStream` so a malicious client cannot hold it open forever

        let Some(packet) = self.recv().await? else {
            return Ok(());
        };

        let Some(mut router) = self.handle_connect_packet(&packet).await? else {
            tracing::info!("Router channel closed; exiting.");
            return Ok(());
        };

        loop {
            tokio::select! {
                res = self.recv() => {
                    let Some(packet) = res? else {
                        tracing::info!("Socket closed; exiting.");
                        break;
                    };

                    self.handle_packet(&mut router, packet).await?;
                },
                maybe_msg = router.next_message() => {
                    let Some(msg) = maybe_msg else {
                        self.disconnect(DisconnectReasonCode::ServerShuttingDown, "broker shutting down").await?;
                        break;
                    };

                    self.handle_message(&mut router, msg).await?;
                }
            }
        }

        Ok(())
    }

    async fn handle_message(
        &mut self,
        _router: &mut RouterConnection,
        message: RouterMessage,
    ) -> Result<(), ConnectionError> {
        match message {
            RouterMessage::Publish { sub_id, props, txn } => {
                let packet_id = (props.qos != QoS::AtMostOnce)
                    .then(|| self.outgoing_packets.insert_publish(props.qos));

                self.send(Packet::Publish(
                    Publish::with_all(
                        false,
                        props.qos,
                        PacketId::opt_to_raw(packet_id),
                        false,
                        Bytes::copy_from_slice(txn.topic.as_bytes()),
                        txn.payload.0.clone(),
                    ),
                    (sub_id.is_some() || txn.properties.is_some()).then(|| {
                        macro_rules! clone_prop {
                            ($prop:ident) => {
                                txn.properties
                                    .as_ref()
                                    .and_then(|props| props.$prop.clone())
                            };
                        }

                        PublishProperties {
                            payload_format_indicator: clone_prop!(payload_format_indicator),
                            message_expiry_interval: clone_prop!(message_expiry_interval),
                            topic_alias: None,
                            response_topic: clone_prop!(response_topic),
                            correlation_data: clone_prop!(correlation_data).map(|bytes| bytes.0),
                            user_properties: clone_prop!(user_properties)
                                .map_or(vec![], |props| props.0),

                            // TODO: if there's multiple applicable subscriptions
                            // we can bundle them into a single packet [TG-432].
                            // It's an optional part of the protocol, however, and only applies to v5.
                            subscription_identifiers: sub_id
                                .map_or(vec![], |sub_id| vec![sub_id.into()]),

                            content_type: clone_prop!(content_type),
                        }
                    }),
                ))
                .await?;
            }
            RouterMessage::SubAck {
                packet_id,
                return_codes,
            } => {
                let pending_sub = self
                    .incoming_packets
                    .remove_sub(packet_id)
                    .expect("BUG: we forgot a packet ID");

                let return_codes = merge_return_codes(pending_sub.return_codes, return_codes);

                self.send(Packet::SubAck(
                    SubAck {
                        pkid: packet_id.get(),
                        return_codes,
                    },
                    None,
                ))
                .await?;
            }
            RouterMessage::PubAck { .. } => {}
        }

        Ok(())
    }

    async fn handle_packet(
        &mut self,
        router: &mut RouterConnection,
        packet: Packet,
    ) -> Result<(), ConnectionError> {
        match packet {
            Packet::PingReq(_) => {
                // Funny, you'd think there'd be some kind of nonce or something
                self.send(Packet::PingResp(PingResp)).await?;
            }
            Packet::Publish(publish, publish_props) => {
                return self.handle_publish(router, publish, publish_props).await;
            }
            Packet::Subscribe(sub, sub_props) => {
                return self.handle_subscribe(router, sub, sub_props).await;
            }
            Packet::Unsubscribe(unsub, _unsub_props) => {
                if unsub.filters.is_empty() {
                    disconnect!(ProtocolError, "no filters in UNSUBSCRIBE");
                }

                let reasons = unsub
                    .filters
                    .iter()
                    .map(|filter| {
                        self.session
                            .subscriptions
                            .remove(filter)
                            .map_or(UnsubAckReason::NoSubscriptionExisted, |_| {
                                UnsubAckReason::Success
                            })
                    })
                    .collect();

                self.send(Packet::UnsubAck(
                    UnsubAck {
                        pkid: unsub.pkid,
                        reasons,
                    },
                    None,
                ))
                .await?;
            }
            Packet::Connect(..) => {
                // MQTT-3.1.0-2
                disconnect!(ProtocolError, "second CONNECT packet");
            }
            _ => {
                tracing::warn!(?packet, "received unsupported");
            }
        }

        Ok(())
    }

    // Handles disconnection internally, so does not return `PacketError`
    async fn handle_connect_packet(
        &mut self,
        packet: &Packet,
    ) -> crate::Result<Option<RouterConnection>> {
        let Packet::Connect(connect, connect_properties, last_will, last_will_properties, login) =
            packet
        else {
            self.disconnect_on_connect_error(
                ConnectReturnCode::ProtocolError,
                "expected CONNECT packet",
            )
            .await?;

            return Ok(None);
        };

        tracing::trace!(?packet, "received");

        // TODO: disallow zero keep alive

        // https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901059
        let mut assigned_client_id = None;
        self.client_id = if !connect.client_id.is_empty() {
            connect.client_id.clone()
        } else {
            let client_id = Alphanumeric.sample_string(&mut rand::thread_rng(), 23);
            assigned_client_id = Some(client_id.clone());
            client_id
        };

        if let Some(login) = login {
            let Some(user) = self.shared.users.by_username.get(&login.username) else {
                self.disconnect_on_connect_error(ConnectReturnCode::NotAuthorized, "unknown user")
                    .await?;

                return Ok(None);
            };

            let verified = self
                .shared
                .password_hasher
                .verify(login.password.as_bytes(), &user.password_hash)
                .await?;

            if !verified {
                self.disconnect_on_connect_error(
                    ConnectReturnCode::NotAuthorized,
                    "invalid password",
                )
                .await?;

                return Ok(None);
            }
        } else if !self.shared.users.auth.allow_anonymous_login {
            self.disconnect_on_connect_error(
                ConnectReturnCode::NotAuthorized,
                "client tried to login anonymously, but anonymous logins are not enabled",
            )
            .await?;

            return Ok(None);
        }

        let mut resuming_session = false;

        if assigned_client_id.is_some() || connect.clean_session {
            self.send_to_broker(BrokerEvent::ConnectionAccepted(
                self.client_id.clone(),
                None,
            ))
            .await?;
        } else {
            let (session_tx, session_rx) = oneshot::channel();

            self.send_to_broker(BrokerEvent::ConnectionAccepted(
                self.client_id.clone(),
                Some(session_tx),
            ))
            .await?;

            if let Some(session) = session_rx.await? {
                self.session = session;
                resuming_session = true;
            } else {
                // It might have expired.
                tracing::debug!(
                    self.client_id,
                    "expected a session, but it wasn't available"
                );
            }
        }

        if let Some(connection_properties) = connect_properties {
            self.session.expiry = connection_properties.session_expiry_interval.into();
        }

        self.session.last_will = last_will.clone();
        self.session.last_will_properties = last_will_properties.clone();

        let router = self
            .shared
            .router
            .connected(self.id, self.client_id.clone())
            .await?;

        self.send(Packet::ConnAck(
            ConnAck {
                session_present: resuming_session,
                code: ConnectReturnCode::Success,
            },
            Some(ConnAckProperties {
                assigned_client_identifier: assigned_client_id,
                // TODO: support wildcard subscriptions
                wildcard_subscription_available: Some(0),
                // TODO: support shared subscriptions
                shared_subscription_available: Some(0),
                // TODO: support subscription identifiers
                subscription_identifiers_available: Some(0),
                // TODO: support retained messages
                retain_available: Some(0),
                topic_alias_max: Some(TOPIC_ALIAS_MAX),
                ..Default::default()
            }),
        ))
        .await?;

        Ok(Some(router))
    }

    async fn handle_publish(
        &mut self,
        router: &mut RouterConnection,
        publish: Publish,
        publish_props: Option<PublishProperties>,
    ) -> Result<(), ConnectionError> {
        let qos = publish.qos();
        let pkid = publish.pkid();

        if (pkid == 0) != (qos == QoS::AtMostOnce) {
            disconnect!(
                ProtocolError,
                "packet ID must be nonzero if QoS is not 0, otherwise both must be 0"
            );
        }

        let transaction = match publish::validate_and_convert(
            publish,
            publish_props,
            TOPIC_ALIAS_MAX,
            &mut self.client_topic_aliases,
        ) {
            // `transaction` is validated and its topic alias has been resolved (if applicable)
            Ok(transaction) => Transaction {
                data: TransactionData::Publish(transaction),
            },
            Err(ValidateError::Disconnect { reason, message }) => {
                return self.disconnect(reason, message).await;
            }
            Err(ValidateError::Reject(reject)) => {
                match qos {
                    QoS::AtMostOnce => {
                        // No response is specified for QoS 0, even on an error.
                        tracing::debug!("rejecting QoS 0 publish: {reject:?}");
                    }
                    QoS::AtLeastOnce => {
                        self.send(reject.into_pub_ack(pkid)).await?;
                    }
                    QoS::ExactlyOnce => {
                        self.send(reject.into_pub_rec(pkid)).await?;
                    }
                }

                return Ok(());
            }
        };

        tracing::trace!("submitting transaction: {transaction:?}");

        self.shared
            .tce_platform
            .send(transaction.to_der().or_else(|e| {
                tracing::error!(?transaction, ?e, "failed to encode transaction");
                disconnect!(
                    ProtocolError,
                    "PUBLISH exceeded consensus protocol size limit"
                );
            })?)
            // If this fails, we'll drop the connection without sending a PUBACK/PUBREC,
            // so the client will know we died before taking ownership and can try another broker.
            .or_else(|_| disconnect!(ServerShuttingDown, "broker shutting down"))?;

        router.transaction(transaction).await;

        match qos {
            QoS::AtMostOnce => {
                // Do nothing.
            }
            QoS::AtLeastOnce => {
                self.send(Packet::PubAck(
                    PubAck {
                        pkid,
                        reason: PubAckReason::Success,
                    },
                    None,
                ))
                .await?;
            }
            QoS::ExactlyOnce => {
                self.send(Packet::PubRec(
                    PubRec {
                        pkid,
                        reason: PubRecReason::Success,
                    },
                    None,
                ))
                .await?;
            }
        }

        Ok(())
    }

    async fn handle_subscribe(
        &mut self,
        router: &mut RouterConnection,
        sub: Subscribe,
        sub_props: Option<SubscribeProperties>,
    ) -> Result<(), ConnectionError> {
        if sub.filters.is_empty() {
            disconnect!(ProtocolError, "no filters in SUBSCRIBE");
        }

        let Some(packet_id) = PacketId::new(sub.pkid) else {
            disconnect!(ProtocolError, "packet ID cannot be zero");
        };

        if self.incoming_packets.contains(packet_id) {
            disconnect!(
                ProtocolError,
                "packet ID {} is already in use",
                packet_id.get()
            );
        }

        let sub_id = sub_props
            .as_ref()
            .and_then(|props| props.id)
            .map(SubscriptionId::try_from)
            .transpose()
            .or_else(|e| disconnect!(ProtocolError, "{e}"))?;

        let mut filters = vec![None; sub.filters.len()];
        let mut return_codes = vec![SubscribeReasonCode::Unspecified; sub.filters.len()];

        let mut has_valid_filters = false;

        for ((filter, filter_out), code_out) in sub
            .filters
            .into_iter()
            .zip(&mut filters)
            .zip(&mut return_codes)
        {
            if !protocol::valid_filter(&filter.path) {
                disconnect!(ProtocolError, "invalid filter: {filter:?}");
            }

            if filter.path.len() > TOPIC_MAX_LENGTH {
                *code_out = SubscribeReasonCode::TopicFilterInvalid;
                continue;
            }

            let Ok(filter_parsed) = filter.path.parse::<Filter>() else {
                *code_out = SubscribeReasonCode::TopicFilterInvalid;
                continue;
            };

            *filter_out = Some((
                filter_parsed,
                FilterProperties {
                    qos: filter.qos,
                    nolocal: filter.nolocal,
                    preserve_retain: filter.preserve_retain,
                    retain_forward_rule: filter.retain_forward_rule,
                },
            ));

            has_valid_filters = true;
        }

        if has_valid_filters {
            self.incoming_packets
                .insert_sub(packet_id, IncomingSub { return_codes })
                .expect("BUG: we should have checked `.contains()` above");

            router.subscribe(packet_id, sub_id, filters).await;
        } else {
            self.send(Packet::SubAck(
                SubAck {
                    pkid: sub.pkid,
                    return_codes,
                },
                None,
            ))
            .await?;
        }

        Ok(())
    }

    async fn recv(&mut self) -> Result<Option<Packet>, ConnectionError> {
        loop {
            let mut read_len = match self.protocol.read_mut(&mut self.read_buf, usize::MAX) {
                Ok(packet) => {
                    tracing::trace!(?packet, "received");
                    return Ok(Some(packet));
                }
                Err(protocol::Error::InsufficientBytes(expected)) => expected,
                Err(e) => {
                    return Err(e.into());
                }
            };

            while read_len > 0 {
                tokio::select! {
                    res = self.stream.read_buf(&mut self.read_buf) => {
                        let read = res.map_err(ConnectionError::ReadError)?;

                        if read == 0 {
                            tracing::debug!("connection closed by remote peer");
                            return Ok(None);
                        }

                        read_len = read_len.saturating_sub(read);
                    }
                    _ = self.token.cancelled() => {
                        return Ok(None);
                    }
                }
            }
        }
    }

    async fn send(&mut self, packet: Packet) -> Result<ConnectionStatus, ConnectionError> {
        tracing::trace!(?packet, "sending");

        self.protocol.write(packet, &mut self.write_buf)?;

        tokio::select! {
            // TODO: we don't necessarily have to wait for the whole buffer to be flushed,
            // but then we need to be careful to limit how many packets we buffer at once.
            res = self.stream.write_all_buf(&mut self.write_buf) => {
                res.map_err(ConnectionError::WriteError)?;
                Ok(ConnectionStatus::Running)
            }
            _ = self.token.cancelled() => {
                Ok(ConnectionStatus::Closed)
            }
        }
    }

    async fn send_to_broker(&mut self, event: BrokerEvent) -> Result<(), ConnectionError> {
        self.shared
            .broker_tx
            .send(event)
            .await
            .or_else(|_| disconnect!(ServerShuttingDown, "broker shutting down"))
    }

    async fn disconnect(
        &mut self,
        reason_code: DisconnectReasonCode,
        reason: impl Into<String>,
    ) -> Result<(), ConnectionError> {
        let reason_string = Some(reason.into());

        tracing::debug!(?reason_code, reason_string, "sending DISCONNECT");

        self.send(Packet::Disconnect(
            Disconnect { reason_code },
            Some(DisconnectProperties {
                session_expiry_interval: self.session.expiry.into(),
                reason_string,
                user_properties: vec![],
                server_reference: None,
            }),
        ))
        .await?;
        self.stream
            .shutdown()
            .await
            .map_err(ConnectionError::WriteError)?;
        Ok(())
    }

    /// Disconnect after a CONNECT error,
    async fn disconnect_on_connect_error(
        &mut self,
        code: ConnectReturnCode,
        reason: impl Display + Into<String>,
    ) -> crate::Result<()> {
        // The MQTT spec actually recommends not responding if anything goes wrong with
        // CONNECT handling, as it could accidentally advertise the presence of an MQTT server
        // to a port scanner.
        //
        // https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901073
        //
        // We want this behavior toggleable, however, to make it easier to debug issues
        // when hardening isn't a concern.
        if self.shared.users.auth.silent_connect_errors {
            tracing::debug!(?code, "silently disconnecting client: {reason}");
        } else {
            self.send(Packet::ConnAck(
                ConnAck {
                    session_present: false,
                    code,
                },
                Some(ConnAckProperties {
                    reason_string: Some(reason.into()),
                    ..Default::default()
                }),
            ))
            .await?;
        }

        self.stream.shutdown().await?;
        Ok(())
    }
}

fn merge_return_codes(
    mut left: Vec<SubscribeReasonCode>,
    right: Vec<SubscribeReasonCode>,
) -> Vec<SubscribeReasonCode> {
    for (a, b) in left.iter_mut().zip(right) {
        if *a == SubscribeReasonCode::Unspecified {
            *a = b;
        }
    }

    left
}
