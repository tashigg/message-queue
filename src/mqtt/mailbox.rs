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
use crate::tce_message::PublishTrasaction;

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
    coalesce_deliveries: bool,
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
    /// The QoS the broker is meant to deliver PUBLISHes at.
    ///
    /// This is the minimmum of the QoS declared on the PUBLISH,
    /// and the QoS associated with the Subscription.
    pub effective_qos: QoS,
    pub packet_id: PacketId,
    pub duplicated: bool,
    pub subscription_ids: Vec<SubscriptionId>,
    pub publish: Arc<PublishTrasaction>,
}

/// A QoS 0 PUBLISH.
///
/// These are not required to be delivered in-order with QoS 1 and 2 PUBLISHes.
pub struct UnorderedMail {
    pub subscription_ids: Vec<SubscriptionId>,
    pub publish: Arc<PublishTrasaction>,
}

/// Send a `PUBREL` for the given packet.
#[derive(Debug)]
#[must_use = "must send `PUBREL` to the client"]
pub struct Release(pub PacketId);

struct Delivery {
    effective_qos: QoS,
    sub_id: Option<SubscriptionId>,
    publish: Arc<PublishTrasaction>,
}

impl Mailbox {
    pub fn coalesce_deliveries(&mut self, value: bool) {
        self.coalesce_deliveries = value;
    }

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
            coalesce_deliveries: false,
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
                effective_qos,
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

                match delivery.effective_qos {
                    QoS::AtMostOnce => {
                        // If this is a duplicate delivery, coalesce into one PUBLISH for v5 clients.
                        if self.mailbox.coalesce_deliveries {
                            if let (Some(mail), Some(sub_id)) =
                                (self.unordered_mail.back_mut(), delivery.sub_id)
                            {
                                if Arc::ptr_eq(&mail.publish, &delivery.publish) {
                                    mail.subscription_ids.push(sub_id);
                                    continue;
                                }
                            }
                        }

                        self.unordered_mail.push_back(UnorderedMail {
                            subscription_ids: Vec::from_iter(delivery.sub_id),
                            publish: delivery.publish,
                        });
                    }
                    QoS::AtLeastOnce | QoS::ExactlyOnce => {
                        // If this is a duplicate delivery, coalesce into one PUBLISH for v5 clients.
                        if self.mailbox.coalesce_deliveries {
                            if let (Some(mail), Some(sub_id)) =
                                (self.mailbox.ordered_mail.back_mut(), delivery.sub_id)
                            {
                                if Arc::ptr_eq(&mail.publish, &delivery.publish) {
                                    mail.subscription_ids.push(sub_id);
                                    continue;
                                }
                            }
                        }

                        self.mailbox.ordered_mail.push_back(OrderedMail {
                            effective_qos: delivery.effective_qos,
                            packet_id: self.mailbox.next_packet_id.wrapping_increment(),
                            subscription_ids: Vec::from_iter(delivery.sub_id),
                            publish: delivery.publish,
                            duplicated: false,
                        });
                    }
                }

                received = true;
            }
        })
        .await;
    }

    pub fn pop_unordered(&mut self) -> Option<UnorderedMail> {
        self.unordered_mail.pop_front()
    }

    pub fn next_ordered_unread(&self) -> Option<&OrderedMail> {
        self.mailbox.ordered_mail.get(self.next_unread)
    }

    pub fn mark_ordered_read(&mut self) {
        if let Some(mail) = self.mailbox.ordered_mail.get_mut(self.next_unread) {
            mail.duplicated = true;
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
