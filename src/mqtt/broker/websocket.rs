use std::mem;
use std::net::SocketAddr;

use bytes::BytesMut;
use color_eyre::eyre;
use color_eyre::eyre::WrapErr;
use futures::{SinkExt, TryStreamExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::handshake::server::{ErrorResponse, Request, Response};
use tokio_tungstenite::tungstenite::http::{HeaderValue, StatusCode};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use crate::mqtt::broker::socket::MqttSocket;

pub struct WebsocketAcceptor {
    listener: TcpListener,
    // To not block the main broker loop, we spawn tasks to complete Websocket upgrades.
    handshaking: JoinSet<eyre::Result<MqttWebsocket>>,
}

pub struct MqttWebsocket {
    remote_addr: SocketAddr,
    stream: WebSocketStream<TcpStream>,
}

impl WebsocketAcceptor {
    pub async fn bind(addr: SocketAddr) -> eyre::Result<Self> {
        let listener = TcpListener::bind(addr)
            .await
            .wrap_err_with(|| format!("failed to bind websockets_addr: {addr}"))?;

        Ok(Self {
            listener,
            handshaking: JoinSet::new(),
        })
    }
    pub async fn accept(&mut self) -> eyre::Result<MqttWebsocket> {
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

        self.handshaking.spawn(handshake(remote_addr, stream));
    }
}

impl MqttSocket for MqttWebsocket {
    fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    async fn read(&mut self, buf: &mut BytesMut) -> eyre::Result<usize> {
        let message = self.stream.try_next().await?;

        let Some(message) = message else { return Ok(0) };

        match message {
            Message::Binary(bytes) => {
                buf.extend_from_slice(&bytes);
                Ok(bytes.len())
            }
            // MQTT Control Packets MUST be sent in WebSocket binary data frames.
            // If any other type of data frame is received the recipient MUST
            // close the Network Connection [MQTT-6.0.0-1].
            _ => Err(eyre::eyre!("unexpected Websocket message: {message:?}")),
        }
    }

    async fn write_take_all(&mut self, buf: &mut Vec<u8>) -> eyre::Result<()> {
        // `tokio-tungstenite` _only_ works with `Vec<u8>`
        // To avoid copying, we just take the whole buffer and send it.
        self.stream.send(Message::Binary(mem::take(buf))).await?;

        Ok(())
    }

    async fn shutdown(&mut self) -> eyre::Result<()> {
        self.stream.close(None).await?;

        Ok(())
    }
}

#[tracing::instrument(skip(stream), err(level = tracing::Level::DEBUG))]
async fn handshake(remote_addr: SocketAddr, stream: TcpStream) -> eyre::Result<MqttWebsocket> {
    // Disable Nagle's algorithm since we always send complete packets.
    // https://en.wikipedia.org/wiki/Nagle's_algorithm
    if let Err(e) = stream.set_nodelay(true) {
        // It's unclear how this could actually fail and what it means when it does.
        tracing::debug!(?e, "error setting TCP_NODELAY on socket");
    }

    let stream = tokio_tungstenite::accept_hdr_async(stream, require_mqtt_subprotocol)
        .await
        .wrap_err("error from accept_sync")?;

    Ok(MqttWebsocket {
        remote_addr,
        stream,
    })
}

fn require_mqtt_subprotocol(req: &Request, mut resp: Response) -> Result<Response, ErrorResponse> {
    let protocols = req.headers().get_all("Sec-Websocket-Protocol");

    let has_mqtt_subprotocol = protocols.iter().any(|protocol| {
        // Really annoying that there still isn't something like `memchr()` in `std`.
        // We don't need to go overboard with validation here
        protocol.as_bytes().windows(4).any(|it| it == b"mqtt")
    });

    if !has_mqtt_subprotocol {
        return Err(
            Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body(Some(
                    "The client MUST include \"mqtt\" in the list of WebSocket Sub Protocols it offers [MQTT-6.0.0-3]".to_string()
                ))
                .expect("BUG: ErrorResponse construction should not fail here")
        );
    }

    resp.headers_mut()
        .insert("Sec-Websocket-Protocol", HeaderValue::from_static("mqtt"));

    Ok(resp)
}
