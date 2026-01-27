use crate::mqtt::mailbox::Mailbox;
use crate::mqtt::ClientId;
use crate::transaction::PublishTrasaction;
use futures::StreamExt;
use std::num::NonZeroU32;
use std::time::Duration;
use crate::collections::HashMap;
use tokio_util::time::{delay_queue, DelayQueue};

/// Sessions of clients that have disconnected. They might eventually time out, or could be
/// reclaimed by a reconnecting client.
///
/// This is only locked during client connection and disconnection.
#[derive(Default)]
pub(crate) struct InactiveSessions {
    sessions: HashMap<ClientId, InactiveSession>,
    expirations: DelayQueue<ClientId>,
    will_expirations: DelayQueue<ClientId>,
}

struct InactiveSession {
    session: SessionStore,
    expiration: Option<delay_queue::Key>,
    will_expiration: Option<delay_queue::Key>,
}

pub enum Event {
    Expiration(ClientId, Session),
    WillElapsed(ClientId, Will),
}

impl InactiveSessions {
    pub fn insert(&mut self, client_id: ClientId, session: SessionStore) {
        let expiry_key = if let SessionExpiry::AfterConnectionClosed(duration) = session.expiry {
            Some(self.expirations.insert(client_id, duration))
        } else {
            None
        };

        let will_expiration = if let Some(will) = &session.session.last_will {
            // it's somewhat less efficient to immediately poll the expiry, but, meh.
            let expiry = will.delay.map_or(0, NonZeroU32::get);
            let duration = Duration::from_secs(expiry as u64);

            Some(self.will_expirations.insert(client_id, duration))
        } else {
            None
        };

        let inactive = InactiveSession {
            session,
            expiration: expiry_key,
            will_expiration,
        };

        let old_session = self.sessions.insert(client_id, inactive);

        assert!(old_session.is_none());
    }

    pub fn claim(&mut self, client_id: &ClientId) -> Option<SessionStore> {
        let inactive = self.sessions.remove(client_id)?;
        if let Some(expiry_key) = inactive.expiration {
            self.expirations.remove(&expiry_key);
        }

        if let Some(will_expiration) = inactive.will_expiration {
            self.will_expirations.remove(&will_expiration);
        }

        Some(inactive.session)
    }

    pub async fn next_event(&mut self) -> Option<Event> {
        tokio::select! {
            Some(client_id) = self.expirations.next() => {
                let client_id = client_id.into_inner();
                let inactive = self
                    .sessions
                    .remove(&client_id)
                    .expect("BUG: removed already expired/claimed session");

                    // if the will hasn't been sent yet, the death of the session triggers it.
                    if let Some(will_expiration) = inactive.will_expiration {
                        self.will_expirations.remove(&will_expiration);
                    }

                tracing::trace!(%client_id, "session expired");
                Some(Event::Expiration(client_id, inactive.session.session))
            },
            Some(client_id) = self.will_expirations.next() => {
               let client_id = client_id.into_inner();
               // this happening implies a non-local bug, it isn't trivial to ignore (it would require having the event polling be in a loop so that this could be skipped... Which would be reasonable if it wasn't already a bug)
               let inactive = self.sessions.get_mut(&client_id).expect("BUG: will expiration not removed when session expired/claimed");
               let will = inactive.session.session.last_will.take().expect("BUG: Will claimed and not removed");

               tracing::trace!(%client_id, "will elapsed");
               Some(Event::WillElapsed(client_id, will))

            }


            else => None,
        }
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

pub(crate) struct Will {
    pub delay: Option<NonZeroU32>,
    // Note: We have to eagerly construct this, but the `timestamp_recieved` is actually supposed to be whenever the will goes into effect.
    // 3.1.3.2.4 Message Expiry Interval
    // > If present, the Four Byte value is the lifetime of the Will Message in seconds
    // > and is sent as the Publication Expiry Interval *when the Server publishes the Will Message*.
    pub transaction: PublishTrasaction,
}

// https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Session_State
#[derive(Default)]
pub(crate) struct Session {
    /// The last will's delay can be greater than the session's expiry, but
    /// session expiry always leads to the last will being sent, dropped, or
    /// overwritten (3.1.3.2.2).
    pub last_will: Option<Will>,
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
