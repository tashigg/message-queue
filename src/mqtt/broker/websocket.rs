use std::mem;
use std::net::{IpAddr, SocketAddr};
use std::str;

use axum::async_trait;
use axum::extract::rejection::ExtensionRejection;
use axum::extract::ws::rejection::WebSocketUpgradeRejection;
use axum::extract::ws::{Message, WebSocket};
use axum::extract::{ConnectInfo, FromRequestParts, State, WebSocketUpgrade};
use axum::http::request::Parts;
use axum::http::{HeaderValue, StatusCode};
use axum::response::{ErrorResponse, IntoResponse, Response};
use axum::routing::{get, Router};
use bytes::BytesMut;
use color_eyre::eyre;
use color_eyre::eyre::WrapErr;
use futures::{SinkExt, TryStreamExt};
use crate::flatten_task_result;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::mqtt::broker::socket::MqttSocket;

pub struct WebsocketAcceptor {
    serve_task: JoinHandle<eyre::Result<()>>,
    new_sockets_rx: mpsc::Receiver<MqttWebsocket>,
}

pub struct MqttWebsocket {
    remote_addr: SocketAddr,
    websocket: WebSocket,
}

impl WebsocketAcceptor {
    pub async fn bind(addr: SocketAddr, token: CancellationToken) -> eyre::Result<Self> {
        let listener = TcpListener::bind(addr)
            .await
            .wrap_err_with(|| format!("failed to bind websockets_addr: {addr}"))?;

        // Channel allocates in slabs of 32
        let (new_sockets_tx, new_sockets_rx) = mpsc::channel(64);

        let router = Router::new()
            .route("/", get(health_check_or_handshake))
            .fallback(handshake)
            .with_state(new_sockets_tx)
            .into_make_service_with_connect_info::<SocketAddr>();

        let serve_task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(token.cancelled_owned())
                .await
                .wrap_err("error from axum::serve()")
        });

        Ok(Self {
            serve_task,
            new_sockets_rx,
        })
    }
    pub async fn accept(&mut self) -> eyre::Result<MqttWebsocket> {
        tokio::select! {
            // `None` (the channel closing) will wait for the task to exit
            Some(socket) = self.new_sockets_rx.recv() => {
                Ok(socket)
            }
            res = &mut self.serve_task => {
                flatten_task_result(res).wrap_err("error from serve_task")?;
                eyre::bail!("serve_task exited prematurely without error")
            }
        }
    }
}

impl MqttSocket for MqttWebsocket {
    fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    async fn read(&mut self, buf: &mut BytesMut) -> eyre::Result<usize> {
        let message = self.websocket.try_next().await?;

        let Some(message) = message else { return Ok(0) };

        match message {
            Message::Binary(bytes) => {
                // There isn't really any getting around this copy.
                // Multiple MQTT packets may end up packed into the same message.
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
        // The websocket API works with `Vec<u8>`, not `Bytes`.
        // To avoid copying, we just take the whole buffer and send it.
        self.websocket.send(Message::Binary(mem::take(buf))).await?;
        self.websocket.flush().await?;

        Ok(())
    }

    async fn shutdown(&mut self) -> eyre::Result<()> {
        // `.close()` resolves to inherent method `WebsocketStream::close(self)`
        SinkExt::close(&mut self.websocket).await?;

        Ok(())
    }
}

async fn health_check_or_handshake(
    state: State<mpsc::Sender<MqttWebsocket>>,
    remote_addr: RemoteAddr,
    ws_upgrade: Result<WebSocketUpgrade, WebSocketUpgradeRejection>,
) -> Result<Response, ErrorResponse> {
    match ws_upgrade {
        Ok(websocket) => handshake(state, remote_addr, websocket).await,
        Err(rejection) => {
            // If the upgrade was rejected for a non-fatal reason
            // (i.e. because a Websocket upgrade wasn't actually requested),
            // then answer `200 OK` for `GET /` as a health check.
            //
            // This is required for deploying FoxMQ behind a GKE Ingress.
            nonfatal_rejection(rejection)?;

            Ok(StatusCode::OK.into_response())
        }
    }
}

async fn handshake(
    State(new_sockets_tx): State<mpsc::Sender<MqttWebsocket>>,
    RemoteAddr(remote_addr): RemoteAddr,
    ws_upgrade: WebSocketUpgrade,
) -> Result<Response, ErrorResponse> {
    if new_sockets_tx.is_closed() {
        return Err(StatusCode::SERVICE_UNAVAILABLE.into());
    }

    Ok(ws_upgrade
        .protocols(["mqtt"])
        .on_upgrade(move |websocket| async move {
            new_sockets_tx
                .send(MqttWebsocket {
                    remote_addr,
                    websocket,
                })
                .await
                .ok();
        }))
}

fn nonfatal_rejection(
    rejection: WebSocketUpgradeRejection,
) -> Result<(), WebSocketUpgradeRejection> {
    use WebSocketUpgradeRejection::*;

    match rejection {
        // Websocket upgrade was requested but for an unsupported version
        InvalidWebSocketVersionHeader(e) => Err(e.into()),
        // `Sec-Websocket-Key` was not specified by the client; not a valid Websocket upgrade.
        WebSocketKeyHeaderMissing(e) => Err(e.into()),
        _ => Ok(()),
    }
}

/// The client socket address. May or may not be real.
///
/// The client IP will be sourced from the [`X-Forwarded-For`] header, if set.
/// This does not include the port, so the port of the remote side of the connection is used instead.
///
/// Otherwise, it is the socket address of the remote side of the connection.
///
/// Requires the use of [`axum::Router::into_make_service_with_connect_info()`]
/// or the addition of a [`axum::extract::connect_info::MockConnectInfo`] layer for testing.
///
/// [`X-Forwarded-For`]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-For
struct RemoteAddr(SocketAddr);

#[async_trait]
impl<S> FromRequestParts<S> for RemoteAddr
where
    S: Send + Sync,
{
    type Rejection = ExtensionRejection;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let ConnectInfo(socket_addr) =
            ConnectInfo::<SocketAddr>::from_request_parts(parts, state).await?;

        let connected_addr = socket_addr.ip();

        let client_addr = parts
            .headers
            .get("X-Forwarded-For")
            .and_then(|val| find_client_ip(connected_addr, val))
            .unwrap_or_else(|| socket_addr.ip());

        Ok(Self(SocketAddr::new(client_addr, socket_addr.port())))
    }
}

/// Given an `X-Forwarded-For` header, select the IP most likely to be the client's.
#[tracing::instrument]
fn find_client_ip(connected_addr: IpAddr, x_forwarded_for: &HeaderValue) -> Option<IpAddr> {
    // By convention, client IP is listed first, followed by any proxies.
    // https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Forwarded-For
    //
    // Google's external load balancer (allocated by our K8s Ingress object)
    // will thus list the client's IP and then its own:
    // https://cloud.google.com/load-balancing/docs/https#x-forwarded-for_header
    //
    // So the simplest implementation would just look at the first IP address in the list, right?
    // Easy-peasy.
    //
    // However, a malicious client could trick a naive parser by adding
    // `X-Forwarded-For: <fake IP>` to their original request; our load balancer
    // would then append the client IP and also its own IP to the list,
    // putting the actual client IP in the middle.
    //
    // Thus, what we actually want to do is step through the list in *reverse*,
    // finding the first IP address that *isn't* our load balancer,
    // which we know because it's the source IP of the connection (`connected_addr`).
    x_forwarded_for
        .as_bytes()
        .split(|b| *b == b',')
        .filter_map(|addr| {
            // By lazily validating UTF-8, we also avoid an exploit where a malicious client
            // could break parsing by setting `X-Forwarded-For: <invalid UTF-8>`
            //
            // Btw, yet another API that would be amazing if it were stable: `IpAddr::parse_ascii()`
            // https://github.com/rust-lang/rust/issues/101035
            let addr = str::from_utf8(addr)
                .map_err(|e| {
                    tracing::debug!(addr=%addr.escape_ascii(), "`addr` is not valid UTF-8: {e}");
                })
                .ok()?;

            addr.trim()
                .parse::<IpAddr>()
                .map_err(|e| {
                    tracing::debug!(addr, "`addr` is not a valid IP address: {e}");
                })
                .ok()
        })
        .rfind(|ip| ip != &connected_addr)
}
