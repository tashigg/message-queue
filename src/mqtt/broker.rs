use std::net::SocketAddr;

use bytes::BytesMut;
use color_eyre::eyre::Context;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::mqtt::protocol;
use crate::mqtt::protocol::{
    ConnAck, ConnectReturnCode, Disconnect, DisconnectProperties, DisconnectReasonCode, Packet,
    Protocol,
};

pub struct MqttBroker {
    listen_addr: SocketAddr,

    listener: TcpListener,

    token: CancellationToken,

    tasks: JoinSet<crate::Result<()>>,
}

impl MqttBroker {
    pub async fn bind(listen_addr: SocketAddr) -> crate::Result<Self> {
        let listener = TcpListener::bind(listen_addr)
            .await
            .wrap_err_with(|| format!("failed to bind listen_addr: {}", listen_addr))?;

        Ok(MqttBroker {
            listen_addr,
            listener,
            token: CancellationToken::new(),
            tasks: JoinSet::new(),
        })
    }

    pub async fn run(&mut self) -> crate::Result<()> {
        tracing::info!(listen_addr = %self.listen_addr, "listening for connections");

        loop {
            let (stream, remote_addr) =
                self.listener.accept().await.wrap_err("error from socket")?;

            tracing::info!(%remote_addr, "connection received");

            let conn = Connection::new(stream, remote_addr, self.token.clone());

            self.tasks.spawn(conn.run());
        }
    }

    pub fn connections(&self) -> usize {
        self.tasks.len()
    }

    pub async fn shutdown(mut self) -> crate::Result<()> {
        // Closes any pending connections and stops listening for new ones.
        drop(self.listener);

        self.token.cancel();

        while let Some(_) = self.tasks.join_next().await {
            tracing::info!("{} connections remaining", self.tasks.len());
        }

        Ok(())
    }
}

struct Connection {
    remote_addr: SocketAddr,

    protocol: protocol::v5::V5,
    stream: TcpStream,
    read_buf: BytesMut,
    read_len: usize,
    write_buf: BytesMut,

    token: CancellationToken,
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
            protocol: protocol::v5::V5,
            stream,
            read_buf: BytesMut::with_capacity(8192),
            read_len: 0,
            write_buf: BytesMut::with_capacity(8192),
            token,
        }
    }

    #[tracing::instrument(name = "Connection::run", skip(self), fields(remote_addr=%self.remote_addr))]
    async fn run(mut self) -> crate::Result<()> {
        let Some(packet) = self.recv().await? else {
            return Ok(());
        };

        match &packet {
            Packet::Connect(connect, ..) => {
                tracing::debug!(?packet, "received CONNECT");

                self.send(Packet::ConnAck(
                    ConnAck {
                        session_present: false,
                        code: ConnectReturnCode::Success,
                    },
                    None,
                ))
                .await?;
            }
            _ => {
                self.send(Packet::Disconnect(
                    Disconnect {
                        reason_code: DisconnectReasonCode::ProtocolError,
                    },
                    Some(DisconnectProperties {
                        session_expiry_interval: None,
                        reason_string: Some("expected CONNECT packet".into()),
                        user_properties: vec![],
                        server_reference: None,
                    }),
                ))
                .await?;

                return Ok(());
            }
        }

        while let Some(packet) = self.recv().await? {
            tracing::debug!(?packet, "received packet");
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
                        return Ok(ConnectionStatus::Closed);
                    }

                    self.read_len = self.read_len.saturating_sub(read);
                }
                res = writer.write_buf(&mut self.write_buf), if !self.write_buf.is_empty() => {
                    res.wrap_err("error writing to socket")?;
                }
            }

            if self.read_len == 0 && self.write_buf.is_empty() {
                return Ok(ConnectionStatus::Running);
            }
        }
    }
}
