use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;

use color_eyre::eyre::Context;
use der::Encode;
use rumqttd_shim::protocol::{
    LastWill, LastWillProperties, Packet, PubAck, PubAckReason, PubRec, PubRecReason, QoS,
};
use tashi_collections::FnvHashMap;
use tashi_consensus_engine::Platform;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::config::users::Users;
use crate::mqtt::connection::Connection;
use crate::mqtt::publish::ValidateError;
use crate::mqtt::session::{InactiveSessions, LastWillExt, LastWillPropertiesExt, Session};
use crate::mqtt::{publish, ClientId};
use crate::password::PasswordHashingPool;
use crate::tce_message::{Transaction, TransactionData};

// The MQTT spec imposes a maximum topic length of 64 KiB but implementations can impose a smaller limit
pub(crate) const TOPIC_MAX_LENGTH: usize = 1024;

// `hashbrown` uses a max load factor of 7/8 and rounds allocations up to the next power of two,
// so a maximum larger than 112 will actually lead to an allocation of 256 buckets,
// meaning up to ~56% wasted space in the table.
//
// Might as well just make it a nice round number to avoid advertising that we're using a hashmap.
pub(crate) const MAX_TOPIC_ALIAS: u16 = 100;

pub struct MqttBroker {
    listen_addr: SocketAddr,

    listener: TcpListener,

    token: CancellationToken,

    tasks: JoinSet<(ClientId, Session)>,

    shared: Arc<Shared>,

    broker_rx: mpsc::Receiver<BrokerEvent>,

    inactive_sessions: InactiveSessions,
}

pub(crate) enum BrokerEvent {
    /// The session sender must only be set if the client wants to resume their
    /// session, i.e. the clean session flag isn't set on connect.
    ConnectionAccepted(ClientId, Option<oneshot::Sender<Option<Session>>>),
}

pub(crate) struct Shared {
    pub password_hasher: PasswordHashingPool,
    pub users: Users,
    pub broker_tx: mpsc::Sender<BrokerEvent>,
    pub platform: Arc<Platform>,
}

impl MqttBroker {
    pub async fn bind(
        listen_addr: SocketAddr,
        users: Users,
        platform: Arc<Platform>,
    ) -> crate::Result<Self> {
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
                platform,
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
                self.handle_connection_accepted(client_id, session_tx).await
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

    async fn handle_connection_accepted(
        &mut self,
        client_id: ClientId,
        session_tx: Option<oneshot::Sender<Option<Session>>>,
    ) {
        match (session_tx, self.inactive_sessions.claim(&client_id)) {
            (Some(session_tx), Some(mut session)) => {
                if publish_last_will(
                    &self.shared.platform,
                    session.last_will_and_properties.take(),
                )
                .is_err()
                {
                    // TCE is shutting down.
                    return;
                }

                // The client might have disconnected, closing the channel.
                match session_tx.send(Some(session)) {
                    Ok(()) => tracing::trace!(client_id, "existing session was resumed"),
                    Err(maybe_session) => {
                        if let Some(session) = maybe_session {
                            self.inactive_sessions.save(client_id, session);
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

/// This will only return an error if TCE is shutting down.
fn publish_last_will(
    platform: &Platform,
    last_will_and_props: Option<(LastWill, Option<LastWillProperties>)>,
) -> crate::Result<()> {
    let Some((last_will, last_will_properties)) = last_will_and_props else {
        return Ok(());
    };

    let publish = last_will.into_publish();
    let publish_props = last_will_properties.map(|p| p.into_publish_properties());

    let _span = tracing::trace_span!("publish_last_will", ?publish, ?publish_props).entered();

    let transaction = match publish::validate_and_convert(
        publish,
        publish_props,
        MAX_TOPIC_ALIAS,
        // TODO: If default does any work then make the param Option instead.
        &mut FnvHashMap::default(),
    ) {
        // `transaction` is validated and its topic alias has been resolved (if applicable)
        Ok(transaction) => Transaction {
            data: TransactionData::Publish(transaction),
        },
        Err(ValidateError::Disconnect { reason, message }) => {
            tracing::error!(?reason, "unable to publish last will: {message}");
            return Ok(());
        }
        Err(ValidateError::Reject(reject)) => {
            tracing::error!(?reject, "unable to publish last will");
            return Ok(());
        }
    };

    // TODO: queue the transaction for retries to TCE.

    platform
        .send(
            transaction
                .to_der()
                .wrap_err("failed to encode Transaction")?,
        )
        .wrap_err("TCE has shut down")
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
        inactive_sessions.save(client_id, session);
    } else {
        tracing::trace!(client_id, "session expired");
    }
}
