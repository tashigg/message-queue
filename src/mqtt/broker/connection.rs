use std::cmp;
use std::fmt::{Debug, Display};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use color_eyre::eyre;
use der::Encode;
use futures::future::OptionFuture;
use tokio::sync::oneshot;
use tokio::time::{Instant, Sleep};
use tokio_util::sync::CancellationToken;

use crate::collections::FnvHashMap;

use protocol::{
    ConnAck, ConnAckProperties, ConnectReturnCode, Disconnect, DisconnectProperties,
    DisconnectReasonCode, Packet, PingResp, Protocol, PubAck, PubAckReason, PubRec, PubRecReason,
    Publish, PublishProperties, QoS, SubAck, Subscribe, SubscribeProperties, SubscribeReasonCode,
    UnsubAck, UnsubAckReason, Unsubscribe,
};
use rumqttd_protocol as protocol;
use rumqttd_protocol::{PubComp, PubCompReason, PubRel, PubRelReason};

use crate::mqtt::broker::socket::MqttSocket;
use crate::mqtt::broker::{BrokerEvent, ConnectionData, Shared, TOPIC_ALIAS_MAX, TOPIC_MAX_LENGTH};
use crate::mqtt::connect::ConnectPacket;
use crate::mqtt::mailbox::OpenMailbox;
use crate::mqtt::packets::{IncomingPacketSet, IncomingSub, IncomingUnsub, PacketId};
use crate::mqtt::publish::ValidateError;
use crate::mqtt::router::{
    FilterProperties, RouterConnection, RouterMessage, SubscribeRequest, SubscriptionId,
};
use crate::mqtt::session::{Session, SessionStore};
use crate::mqtt::trie::Filter;
use crate::mqtt::KeepAlive;
use crate::mqtt::{client_id, connect, publish, ClientId, ClientIndex, ConnectionId, DynProtocol};
use crate::transaction::{PublishMeta, Transaction, TransactionData};

// TODO: make this configurable
/// Default value for the Receive Maximum we send to the client,
/// as well as the send quota we apply to outgoing traffic.
const DEFAULT_RECEIVE_MAXIMUM: u16 = 1024;

/// Default read timeout while waiting for the `CONNECT` packet.
///
/// Shorter than `max_keep_alive` because otherwise it opens us up to a DoS attack.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

pub struct Connection<S: MqttSocket> {
    id: ConnectionId,

    client_id: Option<ClientId>,
    client_index: Option<ClientIndex>,
    keep_alive: KeepAlive,

    protocol: DynProtocol,
    socket: S,
    read_buf: BytesMut,
    write_buf: Vec<u8>,

    // Cache the `Sleep` instance for the read timeout to avoid the overhead of re-registering it
    // every time `recv()` is cancelled due to the `select!{}` in `run()`.
    read_timeout: Option<Pin<Box<Sleep>>>,

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
    ReadError(#[source] eyre::Error),
    #[error("error writing socket: {0}")]
    WriteError(#[source] eyre::Error),
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

impl<S: MqttSocket> Connection<S> {
    pub fn new(id: ConnectionId, socket: S, token: CancellationToken, shared: Arc<Shared>) -> Self {
        Connection {
            id,
            client_id: None,
            client_index: None,
            keep_alive: KeepAlive::default(),
            // This doesn't actually matter until after we read the CONNECT packet.
            protocol: DynProtocol::V5,
            socket,
            read_buf: BytesMut::with_capacity(8192),
            write_buf: Vec::with_capacity(8192),
            read_timeout: Some(Box::pin(tokio::time::sleep(CONNECT_TIMEOUT))),
            token,
            shared,
            client_topic_aliases: FnvHashMap::default(),
            incoming_packets: Default::default(),
            client_receive_maximum: DEFAULT_RECEIVE_MAXIMUM,
        }
    }

    #[tracing::instrument(name = "Connection::run", skip_all, fields(remote_addr=%self.socket.remote_addr()))]
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
        // Read CONNECT packet
        let Some((client_id, router)) = self.handle_connect(store).await? else {
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
                biased;
                // We always want to check if we have new router messages before
                // anything else; this way we can be sure we send out `SUBACK` packets
                // in response to a new subscription before we handle any retained messages
                // that arrive in our mailbox shortly afterward.
                maybe_msg = router.next_message() => {
                    let Some(msg) = maybe_msg else {
                        self.disconnect(DisconnectReasonCode::ServerShuttingDown, "broker shutting down").await?;
                        break;
                    };

                    self.handle_message(&mut router, msg).await?;
                }
                () = mailbox.process_deliveries() => {}
                res = self.recv() => {
                    let Some(packet) = res? else {
                        tracing::info!("Socket closed; exiting.");
                        break;
                    };

                    self.handle_packet(&mut store.session, &mut mailbox, &mut router, packet).await?;
                },
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
        session: &mut Session,
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
            Packet::Disconnect(disconnect, _disconnect_props) => {
                // todo: we should be complaining really loudly if we get a packet after this (MQTT-3.14.4-1) since the client MUST NOT send a packet and MUST hang up.

                // todo: handle session expiry props
                // if we recieve a non-zero `session_expiry_interval` when the session expiry interval *was* zero, we need to ignore the packet and `disconnect!` with a protocol error.

                if matches!(
                    disconnect.reason_code,
                    DisconnectReasonCode::NormalDisconnection
                ) {
                    // kill the will (prevent us from sending it later).
                    session.last_will = None;
                }

                // > On receipt of DISCONNECT, the receiver:
                // > SHOULD close the Network Connection.
                self.socket
                    .shutdown()
                    .await
                    .map_err(ConnectionError::WriteError)?;
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
                mail.delivery_meta,
                Some(mail.packet_id),
                &mail.subscription_ids,
                mail.include_broker_timestamps,
            ))
            .await?;

            mailbox.mark_ordered_read();
        }

        while let Some(mail) = mailbox.pop_unordered() {
            self.send(publish::txn_to_packet(
                &mail.publish,
                PublishMeta::new(QoS::AtMostOnce, mail.retain(), false),
                None,
                mail.subscription_ids(),
                mail.include_broker_timestamps,
            ))
            .await?;
        }

        Ok(())
    }

    // Handles disconnection internally, so does not return `PacketError`
    async fn handle_connect(
        &mut self,
        store: &mut SessionStore,
    ) -> crate::Result<Option<(ClientId, RouterConnection)>> {
        let packet = self
            .recv_with(|read_buf| connect::read(read_buf, usize::MAX))
            .await?;

        let Some(packet) = packet else {
            return Ok(None);
        };

        let ConnectPacket {
            protocol,
            connect,
            connect_props,
            last_will,
            last_will_props,
            login,
        } = packet;

        self.protocol = protocol;

        let mut user = "".to_string();

        if connect_props.as_ref().is_some_and(|props| {
            props.authentication_method.is_some() || props.authentication_data.is_some()
        }) {
            self.disconnect_on_connect_error(
                ConnectReturnCode::BadAuthenticationMethod,
                "unsupported authentication method",
            )
            .await?;
            return Ok(None);
        }

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
            let Some(logged_user) = self.shared.users.by_username.get(&login.username) else {
                self.disconnect_on_connect_error(ConnectReturnCode::NotAuthorized, "unknown user")
                    .await?;
                return Ok(None);
            };

            let verified = self
                .shared
                .password_hasher
                .verify(login.password.as_bytes(), &logged_user.password_hash)
                .await?;

            if !verified {
                self.disconnect_on_connect_error(
                    ConnectReturnCode::NotAuthorized,
                    "invalid password",
                )
                .await?;

                return Ok(None);
            }

            user = login.username;
        } else if !self.shared.users.auth.allow_anonymous_login {
            self.disconnect_on_connect_error(
                ConnectReturnCode::NotAuthorized,
                "client tried to login anonymously, but anonymous logins are not enabled",
            )
            .await?;

            return Ok(None);
        }

        let requested_keep_alive = KeepAlive::from_seconds(connect.keep_alive);
        self.keep_alive = requested_keep_alive.with_max(self.shared.max_keep_alive);
        self.reset_read_timeout();

        // we have to check the `last_will` for validity (if it even exists).
        store.session.last_will = match last_will
            .as_ref()
            .map(|it| publish::validate_and_convert_last_will(it, last_will_props.as_ref()))
        {
            Some(Ok(value)) => Some(value),
            None => None,
            Some(Err(ValidateError::Disconnect(err))) => {
                self.disconnect_on_connect_error(err.reason.into_connack_reason(), err.message)
                    .await?;
                return Ok(None);
            }
            Some(Err(ValidateError::Reject(err))) => {
                self.disconnect_on_connect_error(
                    err.reason.into_connack_reason(),
                    err.message.unwrap_or_else(String::new),
                )
                .await?;
                return Ok(None);
            }
        };

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

        if let Some(connect_props) = connect_props {
            store.expiry = connect_props.session_expiry_interval.into();

            if let Some(receive_maximum) = connect_props.receive_maximum {
                self.client_receive_maximum =
                    cmp::min(self.client_receive_maximum, receive_maximum);
            }
        }

        let router = self
            .shared
            .router
            .connected(
                self.id,
                response.client_index,
                client_id,
                user,
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
                wildcard_subscription_available: true,
                // TODO: support shared subscriptions
                shared_subscription_available: false,
                subscription_identifiers_available: true,
                retain_available: true,
                topic_alias_max: Some(TOPIC_ALIAS_MAX),
                server_keep_alive: (requested_keep_alive != self.keep_alive)
                    .then(|| self.keep_alive.as_seconds()),
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
            Err(ValidateError::Disconnect(disconnect)) => {
                return self
                    .disconnect(
                        disconnect.reason.into_disconnect_reason(),
                        disconnect.message,
                    )
                    .await;
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

        if let Some(tce_platform) = &self.shared.tce_platform {
            tce_platform
                .reserve_tx()
                .await
                // If this fails, we'll drop the connection without sending a PUBACK/PUBREC,
                // so the client will know we died before taking ownership and can try another broker.
                .or_else(|_| disconnect!(ServerShuttingDown, "broker shutting down"))?
                .send(
                    transaction
                        .to_der()
                        .map_err(Into::into)
                        .and_then(|it| it.try_into())
                        .or_else(|e| {
                            tracing::error!(?transaction, ?e, "failed to encode transaction");
                            disconnect!(
                                ProtocolError,
                                "PUBLISH exceeded consensus protocol size limit"
                            );
                        })?,
                );
        }

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

        let include_broker_timestamps = sub_props.as_ref().is_some_and(|props| {
            props.user_properties.iter().any(|(k, v)| {
                k == "include_broker_timestamps" && v.parse::<bool>().unwrap_or(false)
            })
        });

        if has_valid_filters {
            self.incoming_packets
                .insert_sub(packet_id, IncomingSub { return_codes })
                .expect("BUG: we should have checked `.contains()` above");

            router
                .subscribe(SubscribeRequest {
                    packet_id,
                    sub_id,
                    filters,
                    include_broker_timestamps,
                })
                .await;
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
                    reasons: std::iter::repeat_n(
                        UnsubAckReason::PacketIdentifierInUse,
                        unsub.filters.len(),
                    )
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
        let mut protocol = self.protocol;
        self.recv_with(|read_buf| protocol.read_mut(read_buf, usize::MAX))
            .await
    }

    async fn recv_with<T: Debug>(
        &mut self,
        mut parse: impl FnMut(&mut BytesMut) -> Result<T, protocol::Error>,
    ) -> Result<Option<T>, ConnectionError> {
        loop {
            let mut read_len = match parse(&mut self.read_buf) {
                Ok(packet) => {
                    tracing::trace!(?packet, "received");
                    // Don't reset the read timeout until a full packet has been read.
                    // That way a malicious connection can't kill time
                    // by sending one byte per timeout period.
                    self.reset_read_timeout();
                    return Ok(Some(packet));
                }
                Err(protocol::Error::InsufficientBytes(expected)) => expected,
                Err(e) => {
                    return Err(e.into());
                }
            };

            let mut read_timeout: OptionFuture<_> = self.read_timeout.as_mut().into();

            while read_len > 0 {
                // Ensure there is sufficient space in the buffer for our packet.
                self.read_buf.reserve(read_len);

                tokio::select! {
                    res = self.socket.read(&mut self.read_buf) => {
                        let read = res.map_err(ConnectionError::ReadError)?;

                        if read == 0 {
                            tracing::debug!("connection closed by remote peer");
                            return Ok(None);
                        }

                        read_len = read_len.saturating_sub(read);
                    }
                    _ = &mut read_timeout => {
                        tracing::debug!("Read timeout");
                        return Ok(None);
                    }
                    _ = self.token.cancelled() => {
                        return Ok(None);
                    }
                }
            }
        }
    }

    fn reset_read_timeout(&mut self) {
        if let Some(timeout) = self.keep_alive.as_timeout() {
            if let Some(sleep) = &mut self.read_timeout {
                sleep.as_mut().reset(Instant::now() + timeout);
            } else {
                self.read_timeout = Some(Box::pin(tokio::time::sleep(timeout)));
            }
        } else {
            self.read_timeout = None;
        }
    }

    async fn send(&mut self, packet: Packet) -> Result<ConnectionStatus, ConnectionError> {
        tracing::trace!(?packet, "sending");

        self.protocol.write(packet, &mut self.write_buf)?;

        let write_timeout: OptionFuture<_> =
            self.keep_alive.as_timeout().map(tokio::time::sleep).into();

        tokio::select! {
            // TODO: we don't necessarily have to wait for the whole buffer to be flushed,
            // but then we need to be careful to limit how many packets we buffer at once.
            res = self.socket.write_take_all(&mut self.write_buf) => {
                res.map_err(ConnectionError::WriteError)?;
                Ok(ConnectionStatus::Running)
            }
            _ = write_timeout => {
                tracing::debug!("Write timeout");
                Ok(ConnectionStatus::Closed)
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
        self.socket
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

        self.socket.shutdown().await?;
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
