use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::BytesMut;
use color_eyre::eyre::Context;
use rand::distributions::{Alphanumeric, DistString};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::users::Users;
use crate::mqtt::protocol::{
    ConnAck, ConnAckProperties, ConnectReturnCode, Disconnect, DisconnectProperties,
    DisconnectReasonCode, Packet, PingResp, Protocol, QoS, SubAck, SubscribeReasonCode, UnsubAck,
    UnsubAckReason,
};
use crate::mqtt::session::{InactiveSessions, Session};
use crate::mqtt::{protocol, ClientId};
use crate::password::PasswordHashingPool;

// The MQTT spec imposes a maximum topic length of 64 KiB but implementations can impose a smaller limit
const TOPIC_MAX_LENGTH: usize = 1024;

pub struct MqttBroker {
    listen_addr: SocketAddr,

    listener: TcpListener,

    token: CancellationToken,

    tasks: JoinSet<(ClientId, Session)>,

    shared: Arc<Shared>,

    broker_rx: mpsc::Receiver<BrokerEvent>,

    inactive_sessions: InactiveSessions,
}

enum BrokerEvent {
    /// The session sender must only be set if the client wants to resume their
    /// session, i.e. the clean session flag isn't set on connect.
    ConnectionAccepted(ClientId, Option<oneshot::Sender<Option<Session>>>),
}

struct Shared {
    password_hasher: PasswordHashingPool,
    users: Users,
    broker_tx: mpsc::Sender<BrokerEvent>,
}

impl MqttBroker {
    pub async fn bind(listen_addr: SocketAddr, users: Users) -> crate::Result<Self> {
        let listener = TcpListener::bind(listen_addr)
            .await
            .wrap_err_with(|| format!("failed to bind listen_addr: {}", listen_addr))?;

        let (broker_tx, broker_rx) = mpsc::channel(100);

        Ok(MqttBroker {
            listen_addr,
            listener,
            token: CancellationToken::new(),
            tasks: JoinSet::new(),
            shared: Arc::new(Shared {
                password_hasher: PasswordHashingPool::new(
                    std::thread::available_parallelism().unwrap_or(NonZeroUsize::MIN),
                ),
                users,
                broker_tx: broker_tx.clone(),
            }),
            broker_rx,
            inactive_sessions: Default::default(),
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
                Some(Ok((client_id, session))) = self.tasks.join_next() => {
                    handle_connection_lost(&mut self.inactive_sessions, client_id, session);
                }
                res = self.listener.accept() => {
                    self.handle_accept(res);
                }
                Some(event) = self.broker_rx.recv() => {
                    self.handle_event(event).await;
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

                let conn =
                    Connection::new(stream, remote_addr, self.token.clone(), self.shared.clone());

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

        self.token.cancel();

        while let Some(Ok((client_id, session))) = self.tasks.join_next().await {
            handle_connection_lost(&mut self.inactive_sessions, client_id, session);
            tracing::info!("{} connections remaining", self.tasks.len());
        }

        Ok(())
    }
}

// This is a free function because it would require a borrow of Broker while
// listener is partially moved in shutdown.
fn handle_connection_lost(
    inactive_sessions: &mut InactiveSessions,
    client_id: ClientId,
    session: Session,
) {
    tracing::trace!(client_id, "connection lost");

    if session.should_save() {
        tracing::trace!(client_id, "session saved");
        inactive_sessions.insert(client_id, session);
    } else {
        tracing::trace!(client_id, "session expired");
    }
}

struct Connection {
    remote_addr: SocketAddr,

    client_id: String,

    protocol: protocol::v5::V5,
    stream: TcpStream,
    read_buf: BytesMut,
    read_len: usize,
    write_buf: BytesMut,

    token: CancellationToken,
    shared: Arc<Shared>,
    session: Session,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ConnectionStatus {
    Running,
    Closed,
}

impl Connection {
    fn new(
        stream: TcpStream,
        remote_addr: SocketAddr,
        token: CancellationToken,
        shared: Arc<Shared>,
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
        }
    }

    #[tracing::instrument(name = "Connection::run", skip_all, fields(remote_addr=%self.remote_addr))]
    async fn run(mut self) -> (ClientId, Session) {
        let _ = self.run_inner().await;
        (self.client_id, self.session)
    }

    async fn run_inner(&mut self) -> crate::Result<()> {
        // TODO: set timeouts on the `TcpStream` so a malicious client cannot hold it open forever

        let Some(packet) = self.recv().await? else {
            return Ok(());
        };

        self.handle_connect_packet(&packet).await?;

        while let Some(packet) = self.recv().await? {
            tracing::trace!(?packet, "received");

            if let Err(_e) = self.handle_packet(packet).await {
                // TODO: log as err or debug depending on the error type
            }
        }

        Ok(())
    }

    async fn handle_packet(&mut self, packet: Packet) -> crate::Result<()> {
        match packet {
            Packet::PingReq(_) => {
                // Funny, you'd think there'd be some kind of nonce or something
                self.send(Packet::PingResp(PingResp)).await?;
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
                    if filter.path.len() > TOPIC_MAX_LENGTH {
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

    async fn handle_connect_packet(&mut self, packet: &Packet) -> crate::Result<()> {
        let Packet::Connect(connect, connect_properties, last_will, last_will_properties, login) =
            packet
        else {
            // MQTT-3.1.0-1
            self.disconnect(
                DisconnectReasonCode::ProtocolError,
                "expected CONNECT packet",
            )
            .await?;

            return Ok(());
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
                ..Default::default()
            }),
        ))
        .await?;

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

    async fn send_to_broker(&mut self, event: BrokerEvent) -> crate::Result<()> {
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
