use crate::mqtt::broker::{BrokerEvent, ConnectionData, Shared, TOPIC_ALIAS_MAX, TOPIC_MAX_LENGTH};
use crate::mqtt::packets::{IncomingPacketSet, IncomingSub, IncomingUnsub, PacketId};
use crate::mqtt::publish::ValidateError;
use crate::mqtt::router::{FilterProperties, RouterConnection, RouterMessage, SubscriptionId};
use crate::mqtt::session::{Session, SessionStore};
use crate::mqtt::trie::Filter;
use crate::mqtt::{client_id, publish, ClientId, ClientIndex, ConnectionId};
use crate::tce_message::{Transaction, TransactionData};
use bytes::BytesMut;

use crate::mqtt::mailbox::OpenMailbox;
use der::Encode;
use protocol::{
    ConnAck, ConnAckProperties, ConnectReturnCode, Disconnect, DisconnectProperties,
    DisconnectReasonCode, Packet, PingResp, Protocol, PubAck, PubAckReason, PubRec, PubRecReason,
    Publish, PublishProperties, QoS, SubAck, Subscribe, SubscribeProperties, SubscribeReasonCode,
    UnsubAck, UnsubAckReason, Unsubscribe,
};
use rumqttd_protocol as protocol;
use rumqttd_protocol::{PubComp, PubCompReason, PubRel, PubRelReason};
use std::fmt::Display;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::{cmp, io, iter};
use tashi_collections::FnvHashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

// TODO: make this configurable
/// Default value for the Receive Maximum we send to the client,
/// as well as the send quota we apply to outgoing traffic.
const DEFAULT_RECEIVE_MAXIMUM: u16 = 1024;

pub struct Connection {
    id: ConnectionId,

    remote_addr: SocketAddr,

    client_id: Option<ClientId>,
    client_index: Option<ClientIndex>,

    protocol: protocol::v5::V5,
    stream: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,

    token: CancellationToken,
    shared: Arc<Shared>,

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

    /// The Receive Maximum reported by the client:
    ///
    /// https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901049
    client_receive_maximum: u16,
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
            client_id: None,
            client_index: None,
            protocol: protocol::v5::V5,
            stream,
            read_buf: BytesMut::with_capacity(8192),
            write_buf: BytesMut::with_capacity(8192),
            token,
            shared,
            client_topic_aliases: FnvHashMap::default(),
            incoming_packets: Default::default(),
            client_receive_maximum: DEFAULT_RECEIVE_MAXIMUM,
        }
    }

    #[tracing::instrument(name = "Connection::run", skip_all, fields(remote_addr=%self.remote_addr))]
    pub async fn run(mut self) -> ConnectionData {
        let mut store = SessionStore::default();

        if let Err(e) = self.run_inner(&mut store).await {
            match e.downcast::<ConnectionError>() {
                Ok(ConnectionError::Disconnect { reason, message }) => {
                    let _ = self.disconnect(reason, message).await;
                }
                Ok(ConnectionError::Protocol(e)) => {
                    let _ = self
                        .disconnect(DisconnectReasonCode::ProtocolError, e.to_string())
                        .await;
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
            client_index: self.client_index,
            store,
        }
    }

    async fn run_inner(&mut self, store: &mut SessionStore) -> crate::Result<()> {
        // TODO: set timeouts on the `TcpStream` so a malicious client cannot hold it open forever

        let Some(packet) = self.recv().await? else {
            return Ok(());
        };

        let Some((client_id, router)) = self.handle_connect_packet(store, &packet).await? else {
            tracing::info!("Router channel closed; exiting.");
            return Ok(());
        };

        self.run_session(client_id, store, router).await
    }

    // Instrument with client ID once we have one assigned.
    #[tracing::instrument(skip_all, fields(%client_id))]
    async fn run_session(
        &mut self,
        client_id: ClientId,
        store: &mut SessionStore,
        mut router: RouterConnection,
    ) -> crate::Result<()> {
        let mut mailbox = store.mailbox.open();

        loop {
            tokio::select! {
                res = self.recv() => {
                    let Some(packet) = res? else {
                        tracing::info!("Socket closed; exiting.");
                        break;
                    };

                    self.handle_packet(&mut store.session, &mut mailbox, &mut router, packet).await?;
                },
                maybe_msg = router.next_message() => {
                    let Some(msg) = maybe_msg else {
                        self.disconnect(DisconnectReasonCode::ServerShuttingDown, "broker shutting down").await?;
                        break;
                    };

                    self.handle_message(&mut router, msg).await?;
                }
                () = mailbox.process_deliveries() => {}
            }

            self.handle_mail(&mut mailbox).await?;
        }

        Ok(())
    }

    async fn handle_message(
        &mut self,
        _router: &mut RouterConnection,
        message: RouterMessage,
    ) -> Result<(), ConnectionError> {
        match message {
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

            RouterMessage::UnsubAck {
                packet_id,
                return_codes,
            } => {
                let pending_unsub = self
                    .incoming_packets
                    .remove_unsub(packet_id)
                    .expect("BUG: we forgot a packet ID");

                let return_codes = merge_return_codes_2(pending_unsub.return_codes, return_codes);
                self.send(Packet::UnsubAck(
                    UnsubAck {
                        pkid: packet_id.get(),
                        reasons: return_codes,
                    },
                    None,
                ))
                .await?;
            }
        }

        Ok(())
    }

    async fn handle_packet(
        &mut self,
        _session: &mut Session,
        mailbox: &mut OpenMailbox<'_>,
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
            Packet::PubAck(puback, _) => {
                let Some(packet_id) = PacketId::new(puback.pkid) else {
                    disconnect!(ProtocolError, "PUBACK cannot use packet ID 0");
                };

                // The reason code doesn't actually matter:
                // If PUBACK or PUBREC is received containing a Reason Code of 0x80 or greater
                // the corresponding PUBLISH packet is treated as acknowledged,
                // and MUST NOT be retransmitted [MQTT-4.4.0-2].
                if let Err(e) = mailbox.puback(packet_id) {
                    tracing::trace!(?packet_id, "invalid PUBACK: {e}");
                }
            }
            Packet::PubRec(pubrec, _) => {
                let Some(packet_id) = PacketId::new(pubrec.pkid) else {
                    disconnect!(ProtocolError, "PUBREC cannot use packet ID 0");
                };

                // The reason code doesn't actually matter:
                // If PUBACK or PUBREC is received containing a Reason Code of 0x80 or greater
                // the corresponding PUBLISH packet is treated as acknowledged,
                // and MUST NOT be retransmitted [MQTT-4.4.0-2].
                let reason = match mailbox.pubrec(packet_id) {
                    Ok(_) => PubRelReason::Success,
                    Err(e) => {
                        // Not fatal
                        tracing::trace!(?packet_id, "invalid PUBREL: {e}");
                        PubRelReason::PacketIdentifierNotFound
                    }
                };

                self.send(Packet::PubRel(
                    PubRel {
                        pkid: packet_id.get(),
                        reason,
                    },
                    None,
                ))
                .await?;
            }
            Packet::PubComp(pubcomp, _) => {
                let Some(packet_id) = PacketId::new(pubcomp.pkid) else {
                    disconnect!(ProtocolError, "PUBCOMP cannot use packet ID 0");
                };

                if let Err(e) = mailbox.pubcomp(packet_id) {
                    tracing::trace!(?packet_id, "invalid PUBCOMP: {e}");
                }
            }
            Packet::PubRel(pubrel, _) => {
                let Some(packet_id) = PacketId::new(pubrel.pkid) else {
                    disconnect!(ProtocolError, "PUBREL cannot use packet ID 0");
                };

                let reason = self
                    .incoming_packets
                    .remove_pub(packet_id)
                    .map_or(PubCompReason::PacketIdentifierNotFound, |_| {
                        PubCompReason::Success
                    });

                self.send(Packet::PubComp(
                    PubComp {
                        pkid: packet_id.get(),
                        reason,
                    },
                    None,
                ))
                .await?;
            }
            Packet::Subscribe(sub, sub_props) => {
                return self.handle_subscribe(router, sub, sub_props).await;
            }
            Packet::Unsubscribe(unsub, _unsub_props) => {
                return self.handle_unsubscribe(router, unsub).await;
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

    async fn handle_mail(&mut self, mailbox: &mut OpenMailbox<'_>) -> crate::Result<()> {
        while let Some(mail) = mailbox.next_ordered_unread() {
            // Check send quota:
            // https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901251
            //
            // This is technically only part of the v5 version of the protocol,
            // but there is nothing in v4 (3.1.1) that precludes us from applying flow control
            // to those clients as well.
            if mailbox.read_len() >= (self.client_receive_maximum as usize) {
                break;
            }

            self.send(publish::txn_to_packet(
                &mail.publish,
                mail.duplicated,
                mail.effective_qos,
                Some(mail.packet_id),
                &mail.subscription_ids,
            ))
            .await?;

            mailbox.mark_ordered_read();
        }

        while let Some(mail) = mailbox.pop_unordered() {
            self.send(publish::txn_to_packet(
                &mail.publish,
                false,
                QoS::AtMostOnce,
                None,
                &mail.subscription_ids,
            ))
            .await?;
        }

        Ok(())
    }

    // Handles disconnection internally, so does not return `PacketError`
    async fn handle_connect_packet(
        &mut self,
        store: &mut SessionStore,
        packet: &Packet,
    ) -> crate::Result<Option<(ClientId, RouterConnection)>> {
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
        let client_id = match ClientId::from_str(&connect.client_id) {
            Ok(client_id) => Some(client_id),
            Err(client_id::ParseError::Empty) => {
                // Let the broker assign a client ID.
                None
            }
            Err(e) => {
                self.disconnect_on_connect_error(ConnectReturnCode::ClientIdentifierNotValid, e)
                    .await?;
                return Ok(None);
            }
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

        // An empty client ID in the CONNECT does *not* imply a clean start.
        // There shouldn't be an existing session,
        // but we must retain session state after disconnect.
        let clean_session = /* assigned_client_id.is_some() || */ connect.clean_session;

        let (response_tx, response_rx) = oneshot::channel();

        self.send_to_broker(BrokerEvent::ConnectionAccepted {
            connection_id: self.id,
            client_id,
            clean_session,
            response_tx,
        })
        .await?;

        let response = response_rx.await?;

        let client_id = client_id
            .or(response.assigned_client_id)
            .expect("BUG: broker should have assigned a client ID if the client didn't");

        self.client_id = Some(client_id);
        self.client_index = Some(response.client_index);

        let session_present = if let Some(existing_session) = response.session {
            *store = existing_session;
            true
        } else {
            false
        };

        if let Some(connection_properties) = connect_properties {
            store.expiry = connection_properties.session_expiry_interval.into();

            if let Some(receive_maximum) = connection_properties.receive_maximum {
                self.client_receive_maximum =
                    cmp::min(self.client_receive_maximum, receive_maximum);
            }
        }

        store.session.last_will = last_will.clone();
        store.session.last_will_properties = last_will_properties.clone();

        // TODO: don't enable this for v4
        store.mailbox.coalesce_deliveries(true);

        let router = self
            .shared
            .router
            .connected(
                self.id,
                response.client_index,
                client_id,
                store.mailbox.sender(),
                clean_session,
            )
            .await?;

        self.send(Packet::ConnAck(
            ConnAck {
                session_present,
                code: ConnectReturnCode::Success,
            },
            Some(ConnAckProperties {
                assigned_client_identifier: response.assigned_client_id.map(Into::into),
                // A value is 0 means that Wildcard Subscriptions are not supported.
                // A value of 1 means Wildcard Subscriptions are supported.
                // If not present, then Wildcard Subscriptions are supported.
                // (I know it's just one byte, but why waste the bandwidth if `true` is the default?)
                //
                // https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901091
                wildcard_subscription_available: None,
                // TODO: support shared subscriptions
                shared_subscription_available: Some(0),
                // Same as `wildcard_subscriptions_available`
                // https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901092
                subscription_identifiers_available: Some(0),
                // TODO: support retained messages
                retain_available: Some(0),
                topic_alias_max: Some(TOPIC_ALIAS_MAX),
                ..Default::default()
            }),
        ))
        .await?;

        Ok(Some((client_id, router)))
    }

    async fn handle_publish(
        &mut self,
        router: &mut RouterConnection,
        publish: Publish,
        publish_props: Option<PublishProperties>,
    ) -> Result<(), ConnectionError> {
        let qos = publish.qos();
        let pkid = publish.pkid();

        let packet_id = PacketId::new(pkid);

        if let Some(packet_id) = packet_id {
            if qos == QoS::AtMostOnce {
                disconnect!(
                    ProtocolError,
                    "QoS set to 0 but packet ID was nonzero: {packet_id:?}"
                );
            }

            if self.incoming_packets.contains(packet_id) {
                match qos {
                    QoS::AtMostOnce => unreachable!("we just checked QoS 0 above"),
                    QoS::AtLeastOnce => {
                        self.send(Packet::PubAck(
                            PubAck {
                                pkid,
                                reason: PubAckReason::PacketIdentifierInUse,
                            },
                            None,
                        ))
                        .await?;
                    }
                    QoS::ExactlyOnce => {
                        // Client is not supposed to send a `PUBREL` after this.
                        self.send(Packet::PubRec(
                            PubRec {
                                pkid,
                                reason: PubRecReason::PacketIdentifierInUse,
                            },
                            None,
                        ))
                        .await?;
                    }
                }

                return Ok(());
            }
        } else if qos != QoS::AtMostOnce {
            disconnect!(
                ProtocolError,
                "QoS set to {} but packet ID was 0",
                qos as u8,
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

                self.incoming_packets
                    .insert_pub(packet_id.expect("BUG: `PacketId` should be `Some`"))
                    .expect("BUG: QoS 2 PUBLISH should have been validated already");
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

    async fn handle_unsubscribe(
        &mut self,
        router: &mut RouterConnection,
        unsub: Unsubscribe,
    ) -> Result<(), ConnectionError> {
        let Some(packet_id) = PacketId::new(unsub.pkid) else {
            disconnect!(ProtocolError, "packet ID cannot be zero");
        };

        if self.incoming_packets.contains(packet_id) {
            self.send(Packet::UnsubAck(
                UnsubAck {
                    pkid: unsub.pkid,
                    reasons: iter::repeat(UnsubAckReason::PacketIdentifierInUse)
                        .take(unsub.filters.len())
                        .collect(),
                },
                None,
            ))
            .await?;

            return Ok(());
        }

        let mut reasons = vec![UnsubAckReason::UnspecifiedError; unsub.filters.len()];
        // not sure what's more expensive long run, overallocating when most of the filters aren't valid, or underallocating when they are.
        let mut filters = Vec::new();

        for (filter, code_out) in unsub.filters.into_iter().zip(&mut reasons) {
            if filter.len() > TOPIC_MAX_LENGTH {
                *code_out = UnsubAckReason::TopicFilterInvalid;
                continue;
            }

            let Ok(filter) = filter.parse::<Filter>() else {
                *code_out = UnsubAckReason::TopicFilterInvalid;
                continue;
            };

            filters.push(filter);
        }

        if !filters.is_empty() {
            self.incoming_packets
                .insert_unsub(
                    packet_id,
                    IncomingUnsub {
                        return_codes: reasons,
                    },
                )
                .expect("BUG: we should have checked `.contains()` above");

            router.unsubscribe(packet_id, filters).await;
        } else {
            self.send(Packet::UnsubAck(
                UnsubAck {
                    pkid: unsub.pkid,
                    reasons,
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
                // The Session Expiry Interval MUST NOT be sent on a DISCONNECT by the Server [MQTT-3.14.2-2].
                session_expiry_interval: None,
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
        reason: impl Display,
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
                    reason_string: Some(reason.to_string()),
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

fn merge_return_codes_2(
    mut left: Vec<UnsubAckReason>,
    right: Vec<UnsubAckReason>,
) -> Vec<UnsubAckReason> {
    let mut right = right.into_iter();
    for it in left.iter_mut() {
        if *it == UnsubAckReason::UnspecifiedError {
            // maybe it would be better to panic here
            if let Some(code) = right.next() {
                *it = code
            }
        }
    }

    left
}
