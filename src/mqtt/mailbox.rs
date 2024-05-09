use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::{cmp, future};
use tashi_collections::HashSet;

use tokio::sync::mpsc;

use rumqttd_protocol::QoS;

use crate::mqtt::packets::PacketId;
use crate::mqtt::router::SubscriptionId;
use crate::tce_message::{PublishMeta, PublishTrasaction};

pub struct MailSender {
    shared: Arc<MailboxShared>,
}

pub struct Mailbox {
    shared: Arc<MailboxShared>,
    delivery_rx: mpsc::UnboundedReceiver<Delivery>,
    ordered_mail: VecDeque<OrderedMail>,
    next_packet_id: PacketId,
    // QoS 2 packet IDs that have had their `PUBREL` sent, but no `PUBCOMP` has been received yet.
    released_ids: HashSet<PacketId>,
}

struct MailboxShared {
    delivery_tx: mpsc::UnboundedSender<Delivery>,
    accepting_mail: AtomicBool,
}

pub struct OpenMailbox<'a> {
    mailbox: &'a mut Mailbox,
    next_unread: usize,
    // We can save buffer space by storing QoS 0 messages separately.
    //
    // We have no obligation to deliver these in order, or at all,
    // so we can also choose to not buffer them if the mailbox is not open.
    unordered_mail: VecDeque<UnorderedMail>,
}

/// A QoS 1 or 2 PUBLISH.
///
/// QoS 1 and 2 PUBLISHes are required to be delivered in a strict ordering.
pub struct OrderedMail {
    pub packet_id: PacketId,
    /// The flags for delivery of the message.
    ///
    /// Includes:
    /// * The DUP flag for the message, set for all opened mail on reconnect.
    /// * The RETAIN flag for the message, masked with the Retain as Published flag on the sub.
    /// * The QoS the broker is meant to deliver PUBLISHes at.
    ///    * This is the minimum of the QoS declared on the PUBLISH,
    ///      and the QoS associated with the Subscription.
    pub delivery_meta: PublishMeta,
    pub subscription_ids: Vec<SubscriptionId>,
    pub publish: Arc<PublishTrasaction>,
}

/// A QoS 0 PUBLISH.
///
/// These are not required to be delivered in-order with QoS 1 and 2 PUBLISHes.
pub struct UnorderedMail {
    pub kind: UnorderedMailKind,
    pub publish: Arc<PublishTrasaction>,
}

/// A compact way to represent the RETAIN flag for a QoS 0 publish.
///
/// To add a `retain: bool` field to `UnorderedMail` would mean wasting
/// an extra 7 bytes for padding on 64-bit, whereas an enum can use the discriminant niches
/// provided by `Vec`, making it no larger than the `Vec` itself.
pub enum UnorderedMailKind {
    /// The message is not retained.
    NotRetained {
        /// The subscription IDs that matched this message.
        subscription_ids: Vec<SubscriptionId>,
    },
    /// The message is flagged as retained.
    Retained {
        /// The subscription ID for this PUBLISH.
        ///
        /// Retained messages are not coalesced,
        /// because the Retain as Published flag may be different per filter.
        // Also, if this was a `Vec` then this enum would be 32 bytes instead of 24,
        // which defeats the purpose of this enum.
        subscription_id: Option<SubscriptionId>,
    },
}

/// Send a `PUBREL` for the given packet.
#[derive(Debug)]
#[must_use = "must send `PUBREL` to the client"]
pub struct Release(#[allow(dead_code)] pub PacketId);

struct Delivery {
    delivery_meta: PublishMeta,
    sub_id: Option<SubscriptionId>,
    publish: Arc<PublishTrasaction>,
}

impl Mailbox {
    pub fn sender(&self) -> MailSender {
        MailSender {
            shared: self.shared.clone(),
        }
    }

    pub fn open(&mut self) -> OpenMailbox<'_> {
        self.shared.accepting_mail.store(true, Ordering::Release);

        OpenMailbox {
            mailbox: self,
            // When a new connection opens the mailbox, we should treat all pending mail as unread.
            next_unread: 0,
            // We only need to accept and store QoS 0 mail while the mailbox is open.
            unordered_mail: VecDeque::with_capacity(32), // Allocate 1 KiB
        }
    }
}

impl Default for Mailbox {
    fn default() -> Self {
        let (delivery_tx, delivery_rx) = mpsc::unbounded_channel();

        Mailbox {
            shared: Arc::new(MailboxShared {
                delivery_tx,
                accepting_mail: AtomicBool::new(false),
            }),
            delivery_rx,
            next_packet_id: PacketId::START,
            released_ids: HashSet::default(),
            ordered_mail: VecDeque::new(),
        }
    }
}

impl MailSender {
    /// Send a `PUBLISH` to the connected client.
    ///
    /// Returns `true` if the mailbox is still valid, `false` if the session has vacated.
    ///
    /// Note that this is different from the mailbox not currently being open;
    /// the client may have disconnected, but we still have a responsibility to buffer messages
    /// for them in the event that they successfully reconnect.
    ///
    /// This only returns `false` if the client will never be able to receive mail again:
    /// either because the session expired, or the client ID was overwritten by a clean session.
    pub fn deliver(
        &mut self,
        subscription_qos: QoS,
        retain: bool,
        sub_id: Option<SubscriptionId>,
        publish: Arc<PublishTrasaction>,
    ) -> bool {
        let effective_qos = cmp::min(subscription_qos, publish.meta.qos());

        // TODO: make this configurable
        // We have no responsibility to buffer QoS 0 publishes for disconnected clients.
        if effective_qos == QoS::AtMostOnce && !self.shared.accepting_mail.load(Ordering::Acquire) {
            tracing::trace!(
                ?sub_id,
                "discarding QoS 0 publish because client is not accepting mail"
            );
            return !self.shared.delivery_tx.is_closed();
        }

        self.shared
            .delivery_tx
            .send(Delivery {
                delivery_meta: PublishMeta::new(effective_qos, retain, false),
                sub_id,
                publish,
            })
            .is_ok()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum AckError {
    #[error("unknown {0:?}")]
    UnknownPacket(PacketId),
    #[error("invalid operation {op} for {packet_id:?} at {qos:?}")]
    InvalidOperation {
        op: &'static str,
        packet_id: PacketId,
        qos: QoS,
    },
}

impl OpenMailbox<'_> {
    /// Receive any pending mail. Returns if at least one delivery was processed.
    pub async fn process_deliveries(&mut self) {
        // Consume as many messages as possible before returning.
        let mut received = false;
        future::poll_fn(|cx| {
            loop {
                // This will return `Pending` when the task has consumed its coop budget,
                // so we don't have to worry about this spinning forever if the router
                // is pushing faster than we can output. That scenario seems unlikely anyway.
                //
                // There's `.recv_many()` but sadly they hardcoded it for `Vec<T>`
                // instead of something like `impl Extend<T>`
                // https://docs.rs/tokio/latest/tokio/sync/mpsc/struct.UnboundedReceiver.html#method.recv_many
                let delivery = match self.mailbox.delivery_rx.poll_recv(cx) {
                    Poll::Ready(Some(delivery)) => delivery,
                    Poll::Ready(None) => unreachable!(
                        "BUG: `delivery_rx` should not be closed as long as the mailbox is open"
                    ),
                    Poll::Pending => {
                        return if received {
                            Poll::Ready(())
                        } else {
                            Poll::Pending
                        };
                    }
                };

                match delivery.delivery_meta.qos() {
                    QoS::AtMostOnce => self.push_unordered(delivery),
                    QoS::AtLeastOnce | QoS::ExactlyOnce => self.push_ordered(delivery),
                }

                received = true;
            }
        })
        .await;
    }

    fn push_unordered(&mut self, delivery: Delivery) {
        debug_assert_eq!(delivery.delivery_meta.qos(), QoS::AtMostOnce);

        if let Some(UnorderedMail {
            // We don't coalesce retained QoS 0 messages.
            // See doc comment on `UnorderedMailKind` for details.
            kind: UnorderedMailKind::NotRetained { subscription_ids },
            publish,
        }) = self.unordered_mail.back_mut()
        {
            // If this is a duplicate delivery, coalesce into one PUBLISH.
            // This is allowed by both V5 and V4.
            let do_coalesce =
                Arc::ptr_eq(publish, &delivery.publish) && !delivery.delivery_meta.retain();

            // Technically, coalescing is allowed to increase the QoS,
            // but since we store unordered messages in a separate queue it's harder to do that.
            if do_coalesce {
                // No-op for V4 clients since they don't have the concept of subscription IDs.
                subscription_ids.extend(delivery.sub_id);
                return;
            }
        }

        self.unordered_mail.push_back(UnorderedMail {
            kind: if delivery.delivery_meta.retain() {
                UnorderedMailKind::Retained {
                    subscription_id: delivery.sub_id,
                }
            } else {
                UnorderedMailKind::NotRetained {
                    subscription_ids: Vec::from_iter(delivery.sub_id),
                }
            },
            publish: delivery.publish,
        });
    }

    fn push_ordered(&mut self, delivery: Delivery) {
        debug_assert_ne!(delivery.delivery_meta.qos(), QoS::AtMostOnce);

        if let Some(mail) = self.mailbox.ordered_mail.back_mut() {
            // If this is a duplicate delivery, coalesce into one PUBLISH.
            // This is allowed by both V5 and V4.
            let do_coalesce = Arc::ptr_eq(&mail.publish, &delivery.publish)
                // Don't coalesce if the Retain flag doesn't match
                // as different filters may have different Retain as Published flags.
                && delivery.delivery_meta.retain() == mail.delivery_meta.retain()
                // Only coalesce if the mail hasn't been read yet.
                // Otherwise, we need to inform the client that a new sub ID matched.
                // Hitting this would be a weird edge case most of the time though.
                && !mail.delivery_meta.dup();

            if do_coalesce {
                // When Clients make subscriptions with Topic Filters that include
                // wildcards, it is possible for a Client’s subscriptions to overlap
                // so that a published message might match multiple filters.
                //
                // In this case the Server MUST deliver the message to the Client
                // respecting the maximum QoS of all the matching subscriptions
                // [MQTT-3.3.5-1].
                mail.delivery_meta
                    .increase_qos_to(delivery.delivery_meta.qos());

                // No-op for V4 clients since they don't have the concept of subscription IDs.
                mail.subscription_ids.extend(delivery.sub_id);
                return;
            }
        }

        self.mailbox.ordered_mail.push_back(OrderedMail {
            delivery_meta: delivery.delivery_meta,
            packet_id: self.mailbox.next_packet_id.wrapping_increment(),
            subscription_ids: Vec::from_iter(delivery.sub_id),
            publish: delivery.publish,
        });
    }

    pub fn pop_unordered(&mut self) -> Option<UnorderedMail> {
        self.unordered_mail.pop_front()
    }

    pub fn next_ordered_unread(&self) -> Option<&OrderedMail> {
        self.mailbox.ordered_mail.get(self.next_unread)
    }

    pub fn mark_ordered_read(&mut self) {
        if let Some(mail) = self.mailbox.ordered_mail.get_mut(self.next_unread) {
            mail.delivery_meta.set_dup(true);
        }

        self.next_unread = cmp::min(self.next_unread + 1, self.mailbox.ordered_mail.len());
    }

    pub fn read_len(&self) -> usize {
        self.mailbox
            .ordered_mail
            .len()
            .saturating_sub(self.next_unread)
    }

    fn pop(&mut self) {
        self.mailbox.ordered_mail.pop_front();
        self.next_unread = self.next_unread.saturating_sub(1);
    }

    pub fn puback(&mut self, packet_id: PacketId) -> Result<(), AckError> {
        // The MQTT spec requires clients to PUBACK publishes in the order they're received.
        // [MQTT-4.6.0-2]
        let front = self
            .mailbox
            .ordered_mail
            .front()
            .filter(|mail| mail.packet_id == packet_id)
            .ok_or(AckError::UnknownPacket(packet_id))?;

        if front.publish.meta.qos() != QoS::AtLeastOnce {
            return Err(AckError::InvalidOperation {
                op: "PUBACK",
                packet_id,
                qos: front.publish.meta.qos(),
            });
        }

        self.pop();

        Ok(())
    }

    pub fn pubrec(&mut self, packet_id: PacketId) -> Result<Release, AckError> {
        // The MQTT spec requires clients to PUBREC publishes in the order they're received.
        // [MQTT-4.6.0-3]
        let front = self
            .mailbox
            .ordered_mail
            .front()
            .filter(|mail| mail.packet_id == packet_id)
            .ok_or(AckError::UnknownPacket(packet_id))?;

        if front.publish.meta.qos() != QoS::ExactlyOnce {
            return Err(AckError::InvalidOperation {
                op: "PUBREC",
                packet_id,
                qos: front.publish.meta.qos(),
            });
        }

        self.pop();

        self.mailbox.released_ids.insert(packet_id);

        Ok(Release(packet_id))
    }

    pub fn pubcomp(&mut self, packet_id: PacketId) -> Result<(), AckError> {
        self.mailbox
            .released_ids
            .remove(&packet_id)
            .then_some(())
            .ok_or(AckError::UnknownPacket(packet_id))
    }
}

impl Drop for OpenMailbox<'_> {
    fn drop(&mut self) {
        self.mailbox
            .shared
            .accepting_mail
            .store(false, Ordering::Release);
    }
}

impl UnorderedMail {
    pub fn retain(&self) -> bool {
        matches!(self.kind, UnorderedMailKind::Retained { .. })
    }

    pub fn subscription_ids(&self) -> &[SubscriptionId] {
        match &self.kind {
            UnorderedMailKind::NotRetained { subscription_ids } => subscription_ids,
            UnorderedMailKind::Retained { subscription_id } => subscription_id.as_slice(),
        }
    }
}
