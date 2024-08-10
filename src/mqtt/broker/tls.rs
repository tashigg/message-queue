use std::net::SocketAddr;
use std::sync::Arc;

use color_eyre::eyre;
use color_eyre::eyre::WrapErr;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

use crate::mqtt::broker::socket::DirectSocket;

pub struct TlsAcceptor {
    acceptor: tokio_rustls::TlsAcceptor,
    listener: TcpListener,
    // To not block the main broker loop, we spawn tasks to complete TLS handshakes.
    handshaking: JoinSet<eyre::Result<MqttTlsSocket>>,
}

pub type MqttTlsSocket = DirectSocket<tokio_rustls::server::TlsStream<TcpStream>>;

impl TlsAcceptor {
    pub async fn bind(config: super::TlsConfig) -> eyre::Result<Self> {
        let listen_addr = config.socket_addr;
        let acceptor = {
            let config = tokio_rustls::rustls::ServerConfig::builder()
                .with_safe_defaults()
                .with_no_client_auth()
                .with_single_cert(config.cert_chain, config.key)?;

            tokio_rustls::TlsAcceptor::from(Arc::new(config))
        };

        let listener = TcpListener::bind(listen_addr)
            .await
            .wrap_err_with(|| format!("failed to bind listen_addr: {}", listen_addr))?;

        Ok(Self {
            acceptor,
            listener,
            handshaking: JoinSet::new(),
        })
    }

    pub async fn accept(&mut self) -> eyre::Result<MqttTlsSocket> {
        loop {
            tokio::select! {
                res = self.listener.accept() => {
                    let (stream, addr) = res.wrap_err("error from TcpListener.accept()")?;

                    self.accepted(addr, stream);
                }
                Some(res) = self.handshaking.join_next() => {
                    match res {
                        Ok(Ok(socket)) => return Ok(socket),
                        // Error is logged by `handshake()`
                        Ok(Err(_)) => (),
                        Err(e) => {
                            tracing::debug!("error from handshake: {e}");
                        }
                    }
                }
            }
        }
    }

    fn accepted(&mut self, remote_addr: SocketAddr, stream: TcpStream) {
        tracing::debug!(%remote_addr, "accepted new connection");

        // Uses `Arc` internally so clones are cheap
        let acceptor = self.acceptor.clone();

        self.handshaking
            .spawn(handshake(remote_addr, stream, acceptor));
    }
}

#[tracing::instrument(skip(stream, acceptor), err(level = tracing::Level::DEBUG))]
async fn handshake(
    remote_addr: SocketAddr,
    stream: TcpStream,
    acceptor: tokio_rustls::TlsAcceptor,
) -> eyre::Result<MqttTlsSocket> {
    // Disable Nagle's algorithm since we always send complete packets.
    // https://en.wikipedia.org/wiki/Nagle's_algorithm
    if let Err(e) = stream.set_nodelay(true) {
        // It's unclear how this could actually fail and what it means when it does.
        tracing::debug!(?e, "error setting TCP_NODELAY on socket");
    }

    let stream = acceptor
        .accept(stream)
        .await
        .wrap_err("error from TlsAcceptor.accept()")?;

    Ok(DirectSocket::new(remote_addr, stream))
}
