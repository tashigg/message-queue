use crate::mqtt::mailbox::Mailbox;
use crate::mqtt::ClientId;
use futures::StreamExt;
use rumqttd_protocol::{LastWill, LastWillProperties};
use std::time::Duration;
use tashi_collections::HashMap;
use tokio_util::time::DelayQueue;

/// Sessions of clients that have disconnected. They might eventually time out, or could be
/// reclaimed by a reconnecting client.
///
/// This is only locked during client connection and disconnection.
#[derive(Default)]
pub(crate) struct InactiveSessions {
    sessions: HashMap<ClientId, (SessionStore, Option<tokio_util::time::delay_queue::Key>)>,
    expirations: DelayQueue<ClientId>,
}

impl InactiveSessions {
    pub fn insert(&mut self, client_id: ClientId, session: SessionStore) {
        let expiry_key = if let SessionExpiry::AfterConnectionClosed(duration) = session.expiry {
            Some(self.expirations.insert(client_id, duration))
        } else {
            None
        };

        let old_session = self.sessions.insert(client_id, (session, expiry_key));

        assert!(old_session.is_none());
    }

    pub fn claim(&mut self, client_id: &ClientId) -> Option<SessionStore> {
        if let Some((session, expiry_key)) = self.sessions.remove(client_id) {
            if let Some(expiry_key) = expiry_key {
                self.expirations.remove(&expiry_key);
            }

            Some(session)
        } else {
            None
        }
    }

    pub async fn next_expiration(&mut self) -> Option<ClientId> {
        let client_id = self.expirations.next().await?.into_inner();
        self.sessions.remove(&client_id);
        tracing::trace!(%client_id, "session expired");
        Some(client_id)
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) enum SessionExpiry {
    #[default]
    OnConnectionClosed,
    AfterConnectionClosed(Duration),
    Never,
}

// https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901048
impl From<Option<u32>> for SessionExpiry {
    fn from(value: Option<u32>) -> Self {
        match value {
            None | Some(0) => SessionExpiry::OnConnectionClosed,
            Some(u32::MAX) => SessionExpiry::Never,
            Some(secs) => SessionExpiry::AfterConnectionClosed(Duration::from_secs(secs.into())),
        }
    }
}

// https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901211
impl From<SessionExpiry> for Option<u32> {
    fn from(value: SessionExpiry) -> Self {
        // Returning `None` would mean "refer to the session expiry interval in the CONNECT packet",
        // but it might have been overridden, so we always return some value.
        match value {
            SessionExpiry::OnConnectionClosed => Some(0),
            SessionExpiry::AfterConnectionClosed(duration) => Some(
                duration
                    .as_secs()
                    .try_into()
                    .expect("BUG: session expiration must always fit in to a u32"),
            ),
            SessionExpiry::Never => Some(u32::MAX),
        }
    }
}

// https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Session_State
#[derive(Default)]
pub(crate) struct Session {
    /// The last will's delay can be greater than the session's expiry, but
    /// session expiry always leads to the last will being sent, dropped, or
    /// overwritten (3.1.3.2.2).
    pub last_will: Option<LastWill>,
    pub last_will_properties: Option<LastWillProperties>,
}

// Separate type to allow `Mailbox` to be borrowed for the duration of a session.
#[derive(Default)]
pub(crate) struct SessionStore {
    pub expiry: SessionExpiry,
    pub session: Session,
    pub mailbox: Mailbox,
}

impl SessionExpiry {
    pub fn should_save(&self) -> bool {
        !matches!(self, SessionExpiry::OnConnectionClosed)
        // We could choose to prevent saving sessions that don't have any useful state.
    }
}
