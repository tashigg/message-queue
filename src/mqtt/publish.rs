use std::num::NonZeroU32;
use std::ops::Not;

use bytes::Bytes;
use time::format_description::well_known::Rfc3339;

use crate::collections::FnvHashMap;

use protocol::{
    ConnectReturnCode, DisconnectReasonCode, LastWill, LastWillProperties, Packet, PubAck,
    PubAckProperties, PubAckReason, PubRec, PubRecProperties, PubRecReason, Publish,
    PublishProperties,
};
use rumqttd_protocol as protocol;
use transaction::PublishTrasaction;

use crate::mqtt::packets::PacketId;
use crate::mqtt::router::SubscriptionId;
use crate::mqtt::MAX_STRING_LEN;
use crate::transaction;
use crate::transaction::{
    BytesAsOctetString, PublishMeta, PublishTransactionProperties, TimestampSeconds, UserProperties,
};

use super::session::Will;

#[derive(Debug, PartialEq, Eq)]
pub enum ValidateError {
    /// Malformed packet; disconnect.
    Disconnect(DisconnectError),
    /// Reject the `PUBLISH` with a reason code.
    Reject(RejectError),
}

#[derive(Debug, PartialEq, Eq)]
pub struct DisconnectError {
    pub reason: DisconnectReason,
    pub message: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DisconnectReason {
    ProtocolError,
    TopicAliasInvalid,
}

impl DisconnectReason {
    pub fn into_connack_reason(self) -> ConnectReturnCode {
        match self {
            Self::ProtocolError => ConnectReturnCode::ProtocolError,
            Self::TopicAliasInvalid => panic!(
                "BUG: Invalid alias occured while handling a connect packet (which doesn't have aliases)"
            ),
        }
    }

    pub fn into_disconnect_reason(self) -> DisconnectReasonCode {
        match self {
            Self::ProtocolError => DisconnectReasonCode::ProtocolError,
            Self::TopicAliasInvalid => DisconnectReasonCode::TopicAliasInvalid,
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct RejectError {
    pub reason: RejectReason,
    pub message: Option<String>,
}

// Many variants are not constructed right now, but will likely be needed in the future.
#[allow(dead_code)]
#[derive(Debug, PartialEq, Eq)]
pub enum RejectReason {
    UnspecifiedError,
    ImplementationSpecificError,
    NotAuthorized,
    TopicNameInvalid,
    PacketIdentifierInUse,
    QuotaExceeded,
    PayloadFormatInvalid,
}

impl RejectReason {
    pub fn into_connack_reason(self) -> ConnectReturnCode {
        match self {
            RejectReason::UnspecifiedError => ConnectReturnCode::UnspecifiedError,
            RejectReason::ImplementationSpecificError => ConnectReturnCode::ImplementationSpecificError,
            RejectReason::NotAuthorized => ConnectReturnCode::NotAuthorized,
            RejectReason::TopicNameInvalid => ConnectReturnCode::TopicNameInvalid,
            RejectReason::PacketIdentifierInUse => panic!("BUG: `PacketIdentifierInUse` occurred during a connect (no packet ID should exist)"),
            RejectReason::QuotaExceeded => ConnectReturnCode::QuotaExceeded,
            RejectReason::PayloadFormatInvalid => ConnectReturnCode::PayloadFormatInvalid,
        }
    }
}

impl RejectError {
    /// Convert into a `PUBACK` packet to reject a QoS 1 publish.
    pub fn into_pub_ack(self, pkid: u16) -> Packet {
        Packet::PubAck(
            PubAck {
                pkid,
                // even though `PUBACK` and `PUBREC` have the same reason codes,
                // `rumqttd` models them as separate enums
                reason: match self.reason {
                    RejectReason::UnspecifiedError => PubAckReason::UnspecifiedError,
                    RejectReason::ImplementationSpecificError => {
                        PubAckReason::ImplementationSpecificError
                    }
                    RejectReason::NotAuthorized => PubAckReason::NotAuthorized,
                    RejectReason::TopicNameInvalid => PubAckReason::TopicNameInvalid,
                    RejectReason::PacketIdentifierInUse => PubAckReason::PacketIdentifierInUse,
                    RejectReason::QuotaExceeded => PubAckReason::QuotaExceeded,
                    RejectReason::PayloadFormatInvalid => PubAckReason::PayloadFormatInvalid,
                },
            },
            Some(PubAckProperties {
                reason_string: self.message,
                user_properties: vec![],
            }),
        )
    }

    /// Convert into a `PUBREC` packet to reject a QoS 2 publish.
    pub fn into_pub_rec(self, pkid: u16) -> Packet {
        Packet::PubRec(
            PubRec {
                pkid,
                reason: match self.reason {
                    RejectReason::UnspecifiedError => PubRecReason::UnspecifiedError,
                    RejectReason::ImplementationSpecificError => {
                        PubRecReason::ImplementationSpecificError
                    }
                    RejectReason::NotAuthorized => PubRecReason::NotAuthorized,
                    RejectReason::TopicNameInvalid => PubRecReason::TopicNameInvalid,
                    RejectReason::PacketIdentifierInUse => PubRecReason::PacketIdentifierInUse,
                    RejectReason::QuotaExceeded => PubRecReason::QuotaExceeded,
                    RejectReason::PayloadFormatInvalid => PubRecReason::PayloadFormatInvalid,
                },
            },
            Some(PubRecProperties {
                reason_string: self.message,
                user_properties: vec![],
            }),
        )
    }
}

macro_rules! protocol_err (
    ($($fmt:tt)*) => {
        return Err(ValidateError::Disconnect(DisconnectError {
            reason: DisconnectReason::ProtocolError,
            message: format!($($fmt)*)
        }))
    }
);

macro_rules! validate {
    ($condition:expr, $reason:ident) => {
        if Not::not($condition) {
            return Err(ValidateError::Reject(RejectError {
                reason: RejectReason::$reason,
                message: None,
            }));
        }
    };
    ($condition:expr, $reason:ident, $($fmt:tt)*) => {
        if Not::not($condition) {
            return Err(ValidateError::Reject(RejectError {
                reason: RejectReason::$reason,
                message: Some(format!($($fmt)*)),
            }));
        }
    }
}

pub fn validate_and_convert_last_will(
    last_will: &LastWill,
    props: Option<&LastWillProperties>,
) -> Result<Will, ValidateError> {
    Ok(Will {
        delay: props
            .as_ref()
            .and_then(|it| it.delay_interval)
            .and_then(NonZeroU32::new),
        transaction: validate_and_convert(
            Publish::with_all(
                false,
                last_will.qos,
                0,
                last_will.retain,
                last_will.topic.clone(),
                last_will.message.clone(),
            ),
            props.map(|it| PublishProperties {
                payload_format_indicator: it.payload_format_indicator,
                message_expiry_interval: it.message_expiry_interval,
                topic_alias: None,
                response_topic: it.response_topic.clone(),
                correlation_data: it.correlation_data.clone(),
                user_properties: it.user_properties.clone(),
                subscription_identifiers: Vec::new(),
                content_type: it.content_type.clone(),
            }),
            0,
            // there are no aliases, so we don't update the alias map.
            &mut Default::default(),
        )?,
    })
}

pub fn validate_and_convert(
    publish: Publish,
    props: Option<PublishProperties>,
    topic_alias_max: u16,
    topic_aliases: &mut FnvHashMap<u16, String>,
) -> Result<PublishTrasaction, ValidateError> {
    // Strictly speaking, this only exists as a sanity check because the protocol encoding itself
    // implies a max string length of 65 KiB, by virtue of using a fixed-width 2-byte length prefix.
    if publish.topic.len() > MAX_STRING_LEN {
        protocol_err!("publish topic too long");
    }

    // No idea why `rumqttd::protocol` doesn't parse this directly to `String`.
    // Occam's razor would suggest that they wanted a type that was cheaply cloneable, like `Bytes`,
    // but forgot that `Arc<str>` exists.
    //
    // Or, topic strings were not always hard-specified to be UTF-8, but since we're only supporting
    // MQTT v5 (to start, anyway), we can safely assume that they are.
    let topic_str = std::str::from_utf8(&publish.topic)
        .or_else(|_| protocol_err!("publish topic not valid UTF-8"))?;

    validate!(
        protocol::valid_topic(topic_str),
        TopicNameInvalid,
        "not a valid topic: {topic_str:?}"
    );

    validate!(
        !topic_str.starts_with('$'),
        NotAuthorized,
        "clients may not publish to topics starting with `$`: {topic_str:?}"
    );

    // If empty, we need to check if the topic alias is set and resolve it.
    let mut topic = topic_str.to_string();

    let properties = if let Some(props) = props {
        // `rumqttd::protocol` surprisingly does not perform this validation despite the fact
        // that it should never expect to decode a packet with `subscription_identifiers`
        // on the broker side. If it shared its protocol code with `rumqttc`, maybe, but it doesn't.
        //
        // It's possible that once upon a time, they _were_ shared, but that's clearly not the case
        // anymore.
        //
        // https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901120
        // > It is a Protocol Error for a PUBLISH packet to contain any Subscription Identifier
        // > other than those received in SUBSCRIBE packet which caused it to flow.
        // > A PUBLISH packet sent from a Client to a Server MUST NOT contain a Subscription Identifier
        if !props.subscription_identifiers.is_empty() {
            protocol_err!("publish from client cannot contain subscription identifiers");
        }

        if let Some(response_topic) = &props.response_topic {
            // Sanity check; see above.
            if response_topic.len() > MAX_STRING_LEN {
                protocol_err!("response topic too long");
            }

            validate!(
                protocol::valid_topic(topic_str),
                TopicNameInvalid,
                "not a valid response topic: {topic_str:?}"
            );
        }

        if let Some(content_type) = &props.content_type {
            // Sanity check; see above.
            if content_type.len() > MAX_STRING_LEN {
                protocol_err!("content type too long");
            }
        }

        // NOTE: topic alias handling should come last since it updates the `topic_aliases` map
        if let Some(topic_alias) = props.topic_alias {
            macro_rules! invalid_alias (
                ($($fmt:tt)*) => {
                    return Err(ValidateError::Disconnect(DisconnectError {
                        reason: DisconnectReason::TopicAliasInvalid,
                        message: format!($($fmt)*)
                    }));
                }
            );

            if topic_alias == 0 {
                // The spec isn't clear if this a protocol error or an invalid topic alias error
                invalid_alias!("topic alias cannot be zero");
            }

            if topic_alias_max == 0 {
                invalid_alias!("topic aliases are disabled on this broker");
            }

            if topic_alias > topic_alias_max {
                // Also not clear in the spec what error this should be,
                // though it indicates a client bug since we already told them what the max is
                invalid_alias!("topic alias exceeds maximum: {topic_alias} > {topic_alias_max}");
            }

            if topic.is_empty() {
                let Some(resolved_topic) = topic_aliases.get(&topic_alias) else {
                    invalid_alias!("unknown topic alias: {topic_alias}");
                };

                topic.clone_from(resolved_topic);
            } else {
                topic_aliases.insert(topic_alias, topic.clone());
            }
        } else if topic.is_empty() {
            protocol_err!("topic is empty but topic alias was not set");
        }

        Some(PublishTransactionProperties {
            payload_format_indicator: props.payload_format_indicator,
            message_expiry_interval: props.message_expiry_interval,
            content_type: props.content_type,
            response_topic: props.response_topic,
            correlation_data: props.correlation_data.map(BytesAsOctetString),
            // `der` doesn't have an equivalent of `skip_serializing_if`
            user_properties: if !props.user_properties.is_empty() {
                Some(UserProperties(props.user_properties))
            } else {
                None
            },
        })
    } else {
        None
    };

    Ok(PublishTrasaction {
        topic,
        meta: PublishMeta::new(publish.qos(), publish.retain, publish.dup()),
        payload: BytesAsOctetString(publish.payload),
        timestamp_received: TimestampSeconds::now(),
        properties,
    })
}

pub fn txn_to_packet(
    txn: &PublishTrasaction,
    delivery_meta: PublishMeta,
    packet_id: Option<PacketId>,
    sub_ids: &[SubscriptionId],
    include_broker_timestamps: bool,
) -> Packet {
    Packet::Publish(
        Publish::with_all(
            delivery_meta.dup(),
            delivery_meta.qos(),
            PacketId::opt_to_raw(packet_id),
            delivery_meta.retain(),
            Bytes::copy_from_slice(txn.topic.as_bytes()),
            txn.payload.0.clone(),
        ),
        (!sub_ids.is_empty() || txn.properties.is_some() || include_broker_timestamps).then(|| {
            macro_rules! clone_prop {
                ($prop:ident) => {
                    txn.properties
                        .as_ref()
                        .and_then(|props| props.$prop.clone())
                };
            }

            let mut user_properties =
                clone_prop!(user_properties).map_or(vec![], |props: UserProperties| props.0);

            if include_broker_timestamps {
                let timestamp_received =
                    time::OffsetDateTime::from_unix_timestamp(txn.timestamp_received.0 as i64)
                        .expect("Time overflow");
                let timestamp_received = timestamp_received
                    .format(&Rfc3339)
                    .expect("formatting error");

                user_properties.push(("timestamp_received".to_owned(), timestamp_received))
            }

            PublishProperties {
                payload_format_indicator: clone_prop!(payload_format_indicator),
                message_expiry_interval: clone_prop!(message_expiry_interval),
                topic_alias: None,
                response_topic: clone_prop!(response_topic),
                correlation_data: clone_prop!(correlation_data).map(|bytes| bytes.0),
                user_properties,

                // TODO: now that `rumqttd-protocol` is in-tree, just change the type there
                subscription_identifiers: sub_ids.iter().copied().map(Into::into).collect(),

                content_type: clone_prop!(content_type),
            }
        }),
    )
}
