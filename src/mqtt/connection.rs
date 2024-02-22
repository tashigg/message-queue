use std::net::SocketAddr;
use std::sync::Arc;

use bytes::BytesMut;
use color_eyre::eyre::WrapErr;
use der::Encode;
use rand::distributions::{Alphanumeric, DistString};
use rumqttd_shim::protocol::{
    self, ConnAck, ConnAckProperties, ConnectReturnCode, Disconnect, DisconnectProperties,
    DisconnectReasonCode, Packet, PingResp, Protocol, PubAck, PubAckReason, PubRec, PubRecReason,
    Publish, PublishProperties, QoS, SubAck, SubscribeReasonCode, UnsubAck, UnsubAckReason,
};
use tashi_collections::FnvHashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::mqtt::publish::ValidateError;
use crate::mqtt::session::Session;
use crate::mqtt::{publish, ClientId};
use crate::tce_message::{Transaction, TransactionData};

pub(crate) struct Connection {
    remote_addr: SocketAddr,

    client_id: String,

    protocol: protocol::v5::V5,
    stream: TcpStream,
    read_buf: BytesMut,
    read_len: usize,
    write_buf: BytesMut,

    token: CancellationToken,
    shared: Arc<crate::mqtt::broker::Shared>,
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
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ConnectionStatus {
    Running,
    Closed,
}

impl Connection {
    pub fn new(
        stream: TcpStream,
        remote_addr: SocketAddr,
        token: CancellationToken,
        shared: Arc<crate::mqtt::broker::Shared>,
    ) -> Self {
        Connection {
            remote_addr,
            client_id: String::new(),
            protocol: protocol::v5::V5,
            stream,
            read_buf: BytesMut::with_capacity(8192),
            read_len: 0,
            write_buf: BytesMut::with_capacity(8192),
            token,
            shared,
            session: Session::default(),
            client_topic_aliases: FnvHashMap::default(),
        }
    }

    #[tracing::instrument(name = "Connection::run", skip_all, fields(remote_addr = % self.remote_addr))]
    pub async fn run(mut self) -> (ClientId, Session) {
        let _ = self.run_inner().await;
        (self.client_id, self.session)
    }

    async fn run_inner(&mut self) -> crate::Result<()> {
        // TODO: set timeouts on the `TcpStream` so a malicious client cannot hold it open forever

        let Some(packet) = self.recv().await? else {
            return Ok(());
        };

        self.handle_connect_packet(packet).await?;

        while let Some(packet) = self.recv().await? {
            tracing::trace!(?packet, "received");

            if let Err(_e) = self.handle_packet(packet).await {
                // TODO: log as err or debug depending on the error type
            }
        }

        Ok(())
    }

    async fn handle_packet(&mut self, packet: Packet) -> crate::Result<()> {
        tracing::trace!(?packet, "received");

        match packet {
            Packet::PingReq(_) => {
                // Funny, you'd think there'd be some kind of nonce or something
                self.send(Packet::PingResp(PingResp)).await?;
            }
            Packet::Publish(publish, publish_props) => {
                return self.handle_publish(publish, publish_props).await;
            }
            Packet::Subscribe(sub, _sub_props) => {
                if sub.filters.is_empty() {
                    return self
                        .disconnect(
                            DisconnectReasonCode::ProtocolError,
                            "no filters in SUBSCRIBE",
                        )
                        .await;
                }

                for filter in &sub.filters {
                    if !protocol::valid_filter(&filter.path) {
                        return self
                            .disconnect(
                                DisconnectReasonCode::ProtocolError,
                                format!("invalid filter: {filter:?}"),
                            )
                            .await;
                    }
                }

                let mut return_codes = Vec::with_capacity(sub.filters.len());
                self.session.subscriptions.reserve(sub.filters.len());

                for filter in sub.filters {
                    if filter.path.len() > crate::mqtt::broker::TOPIC_MAX_LENGTH {
                        return_codes.push(SubscribeReasonCode::TopicFilterInvalid);
                        continue;
                    }

                    let qos = filter.qos;

                    self.session
                        .subscriptions
                        .insert(filter.path.clone(), filter);

                    return_codes.push(match qos {
                        QoS::AtMostOnce => SubscribeReasonCode::QoS0,
                        QoS::AtLeastOnce => SubscribeReasonCode::QoS1,
                        QoS::ExactlyOnce => SubscribeReasonCode::QoS2,
                    });
                }

                self.send(Packet::SubAck(
                    SubAck {
                        pkid: sub.pkid,
                        return_codes,
                    },
                    None,
                ))
                .await?;
            }
            Packet::Unsubscribe(unsub, _unsub_props) => {
                if unsub.filters.is_empty() {
                    return self
                        .disconnect(
                            DisconnectReasonCode::ProtocolError,
                            "no filters in UNSUBSCRIBE",
                        )
                        .await;
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
                self.disconnect(DisconnectReasonCode::ProtocolError, "second CONNECT packet")
                    .await?;
            }
            _ => {
                tracing::warn!(?packet, "received unsupported");
            }
        }

        Ok(())
    }

    async fn handle_connect_packet(&mut self, packet: Packet) -> crate::Result<()> {
        let Packet::Connect(
            connect,
            connect_properties,
            mut last_will,
            mut last_will_properties,
            login,
        ) = packet
        else {
            // MQTT-3.1.0-1
            self.disconnect(
                DisconnectReasonCode::ProtocolError,
                "expected CONNECT packet",
            )
            .await?;

            return Ok(());
        };

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
                return self
                    .disconnect(DisconnectReasonCode::NotAuthorized, "")
                    .await;
            };

            let verified = self
                .shared
                .password_hasher
                .verify(login.password.as_bytes(), &user.password_hash)
                .await?;

            if !verified {
                return self
                    .disconnect(DisconnectReasonCode::NotAuthorized, "")
                    .await;
            }
        }

        let mut resuming_session = false;

        if assigned_client_id.is_some() || connect.clean_session {
            self.send_to_broker(crate::mqtt::broker::BrokerEvent::ConnectionAccepted(
                self.client_id.clone(),
                None,
            ))
            .await?;
        } else {
            let (session_tx, session_rx) = oneshot::channel();

            self.send_to_broker(crate::mqtt::broker::BrokerEvent::ConnectionAccepted(
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

        if let Some(last_will) = last_will.take() {
            self.session.last_will_and_properties = Some((last_will, last_will_properties.take()));
        }

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
                // TODO: support retained messages TG-414
                retain_available: Some(0),
                topic_alias_max: Some(crate::mqtt::broker::MAX_TOPIC_ALIAS),
                ..Default::default()
            }),
        ))
        .await?;

        Ok(())
    }

    // This is very similar to `MqttBroker::publish_last_will`.
    async fn handle_publish(
        &mut self,
        publish: Publish,
        publish_props: Option<PublishProperties>,
    ) -> crate::Result<()> {
        let qos = publish.qos();
        let pkid = publish.pkid();

        let transaction = match publish::validate_and_convert(
            publish,
            publish_props,
            crate::mqtt::broker::MAX_TOPIC_ALIAS,
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

        self.shared
            .platform
            .send(
                transaction
                    .to_der()
                    .wrap_err("failed to encode Transaction")?,
            )
            // If this fails, we'll drop the connection without sending a PUBACK/PUBREC,
            // so the client will know we died before taking ownership and can try another broker.
            .wrap_err("TCE has shut down")?;

        // TODO: dispatch `transaction` locally

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

    async fn recv(&mut self) -> crate::Result<Option<Packet>> {
        loop {
            match self.protocol.read_mut(&mut self.read_buf, usize::MAX) {
                Ok(packet) => return Ok(Some(packet)),
                Err(protocol::Error::InsufficientBytes(expected)) => {
                    self.read_len = expected;
                }
                Err(e) => {
                    // Sending a disconnect packet on protocol error is optional (4.13.1).
                    self.disconnect(DisconnectReasonCode::ProtocolError, e.to_string())
                        .await?;

                    return Err(e).wrap_err("failed to read the packet");
                }
            }

            if self.do_io().await? == ConnectionStatus::Closed {
                return Ok(None);
            }
        }
    }

    async fn send(&mut self, packet: Packet) -> crate::Result<ConnectionStatus> {
        self.protocol
            .write(packet, &mut self.write_buf)
            .wrap_err("protocol error")?;
        self.do_io().await
    }

    async fn send_to_broker(
        &mut self,
        event: crate::mqtt::broker::BrokerEvent,
    ) -> crate::Result<()> {
        self.shared
            .broker_tx
            .send(event)
            .await
            .wrap_err("send to broker")
    }

    async fn disconnect(
        &mut self,
        reason_code: DisconnectReasonCode,
        reason: impl Into<String>,
    ) -> crate::Result<()> {
        let reason_string = Some(reason.into());

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
        self.stream.shutdown().await?;
        Ok(())
    }

    /// Read into `read_buf` at least once, and continue until `read_len` is satisfied,
    /// and drain `write_buf` if it is not empty.
    ///
    /// Returns `Ok(Running)` if I/O requirements have been satisfied.
    ///
    /// Returns `Ok(Cancelled)` if the connection was closed remotely or the local
    /// `CancellationToken` was signalled.
    ///
    /// Returns `Err` on error.
    async fn do_io(&mut self) -> crate::Result<ConnectionStatus> {
        loop {
            // This lets us call async methods that require `mut` concurrently.
            let (mut reader, mut writer) = self.stream.split();

            tokio::select! {
                res = reader.read_buf(&mut self.read_buf) => {
                    let read = res.wrap_err("error reading from socket")?;

                    if read == 0 {
                        tracing::debug!("connection closed by remote peer");
                        return Ok(ConnectionStatus::Closed);
                    }

                    self.read_len = self.read_len.saturating_sub(read);
                }
                res = writer.write_buf(&mut self.write_buf), if !self.write_buf.is_empty() => {
                    res.wrap_err("error writing to socket")?;
                }
                _ = self.token.cancelled() => {
                    return Ok(ConnectionStatus::Closed);
                }
            }

            if self.read_len == 0 && self.write_buf.is_empty() {
                return Ok(ConnectionStatus::Running);
            }
        }
    }
}
