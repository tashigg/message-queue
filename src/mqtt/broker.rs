use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;

use bytes::BytesMut;
use color_eyre::eyre::Context;
use rand::distributions::{Alphanumeric, DistString};
use tashi_collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::users::Users;
use crate::mqtt::protocol;
use crate::mqtt::protocol::{
    ConnAck, ConnAckProperties, ConnectReturnCode, Disconnect, DisconnectProperties,
    DisconnectReasonCode, Filter, Packet, PingResp, Protocol, QoS, SubAck, SubscribeReasonCode,
    UnsubAck, UnsubAckReason,
};
use crate::password::PasswordHashingPool;

// The MQTT spec imposes a maximum topic length of 64 KiB but implementations can impose a smaller limit
const TOPIC_MAX_LENGTH: usize = 1024;

pub struct MqttBroker {
    listen_addr: SocketAddr,

    listener: TcpListener,

    token: CancellationToken,

    tasks: JoinSet<crate::Result<()>>,

    shared: Arc<Shared>,
}

struct Shared {
    password_hasher: PasswordHashingPool,
    users: Users,
}

impl MqttBroker {
    pub async fn bind(listen_addr: SocketAddr, users: Users) -> crate::Result<Self> {
        let listener = TcpListener::bind(listen_addr)
            .await
            .wrap_err_with(|| format!("failed to bind listen_addr: {}", listen_addr))?;

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
            }),
        })
    }

    pub async fn run(&mut self) -> crate::Result<()> {
        tracing::info!(listen_addr = %self.listen_addr, "listening for connections");

        loop {
            let (stream, remote_addr) =
                self.listener.accept().await.wrap_err("error from socket")?;

            tracing::info!(%remote_addr, "connection received");

            let conn = Connection::new(stream, remote_addr, self.token.clone());

            self.tasks.spawn(conn.run(self.shared.clone()));
        }
    }

    pub fn connections(&self) -> usize {
        self.tasks.len()
    }

    pub async fn shutdown(mut self) -> crate::Result<()> {
        // Closes any pending connections and stops listening for new ones.
        drop(self.listener);

        self.token.cancel();

        while let Some(_res) = self.tasks.join_next().await {
            tracing::info!("{} connections remaining", self.tasks.len());
        }

        Ok(())
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

    subscriptions: HashMap<String, Filter>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ConnectionStatus {
    Running,
    Closed,
}

impl Connection {
    fn new(stream: TcpStream, remote_addr: SocketAddr, token: CancellationToken) -> Self {
        Connection {
            remote_addr,
            client_id: String::new(),
            protocol: protocol::v5::V5,
            stream,
            read_buf: BytesMut::with_capacity(8192),
            read_len: 0,
            write_buf: BytesMut::with_capacity(8192),
            token,
            subscriptions: HashMap::with_capacity_and_hasher(256, Default::default()),
        }
    }

    #[tracing::instrument(name = "Connection::run", skip_all, fields(remote_addr=%self.remote_addr))]
    async fn run(mut self, shared: Arc<Shared>) -> crate::Result<()> {
        // TODO: set timeouts on the `TcpStream` so a malicious client cannot hold it open forever

        let Some(packet) = self.recv().await? else {
            return Ok(());
        };

        match &packet {
            Packet::Connect(connect, .., login) => {
                tracing::debug!(?packet, "received CONNECT");

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
                    let Some(user) = shared.users.by_username.get(&login.username) else {
                        return self
                            .disconnect(DisconnectReasonCode::NotAuthorized, "")
                            .await;
                    };

                    let verified = shared
                        .password_hasher
                        .verify(login.password.as_bytes(), &user.password_hash)
                        .await?;

                    if !verified {
                        return self
                            .disconnect(DisconnectReasonCode::NotAuthorized, "")
                            .await;
                    }
                }

                self.send(Packet::ConnAck(
                    ConnAck {
                        session_present: false,
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
                        ..Default::default()
                    }),
                ))
                .await?;
            }
            _ => {
                self.disconnect(
                    DisconnectReasonCode::ProtocolError,
                    "expected CONNECT packet",
                )
                .await?;

                return Ok(());
            }
        }

        while let Some(packet) = self.recv().await? {
            tracing::trace!(?packet, "received packet");

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
                    self.subscriptions.reserve(sub.filters.len());

                    for filter in sub.filters {
                        if filter.path.len() > TOPIC_MAX_LENGTH {
                            return_codes.push(SubscribeReasonCode::TopicFilterInvalid);
                            continue;
                        }

                        let qos = filter.qos;

                        self.subscriptions.insert(filter.path.clone(), filter);

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
                            self.subscriptions
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
                _ => {
                    tracing::warn!(?packet, "received unsupported packet");
                }
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
                Err(e) => return Err(e).wrap_err("protocol error"),
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

    async fn disconnect(
        &mut self,
        reason_code: DisconnectReasonCode,
        reason: impl Into<String>,
    ) -> crate::Result<()> {
        let reason_string = Some(reason.into());

        self.send(Packet::Disconnect(
            Disconnect { reason_code },
            Some(DisconnectProperties {
                session_expiry_interval: None,
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
