use crate::tce_message;
use rumqttd_shim::protocol;
use rumqttd_shim::protocol::{
    DisconnectReasonCode, Packet, PubAck, PubAckProperties, PubAckReason, PubRec, PubRecProperties,
    PubRecReason, Publish, PublishProperties,
};
use tce_message::PublishTrasaction;

use crate::tce_message::{
    BytesAsOctetString, PublishMeta, PublishTransactionProperties, TimestampSeconds,
};
use std::ops::Not;
use tashi_collections::FnvHashMap;

const MAX_STRING_LEN: usize = 65535;

#[derive(Debug, PartialEq, Eq)]
pub enum ValidateError {
    /// Malformed packet; disconnect.
    Disconnect {
        reason: DisconnectReasonCode,
        message: String,
    },
    /// Reject the `PUBLISH` with a reason code.
    Reject(RejectError),
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
        return Err(ValidateError::Disconnect {
            reason: DisconnectReasonCode::ProtocolError,
            message: format!($($fmt)*)
        })
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

pub fn validate_and_convert(
    publish: Publish,
    props: Option<PublishProperties>,
    max_topic_alias: u16,
    topic_aliases: &mut FnvHashMap<u16, String>,
) -> Result<PublishTrasaction, ValidateError> {
    // `rumqttd` doesn't actually seem to validate the length
    if publish.topic.len() > MAX_STRING_LEN {
        protocol_err!("publish topic too long");
    }

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
        if !props.subscription_identifiers.is_empty() {
            protocol_err!("publish from client cannot contain subscription identifiers");
        }

        if let Some(response_topic) = &props.response_topic {
            if response_topic.len() > MAX_STRING_LEN {
                protocol_err!("response topic too long");
            }
        }

        if let Some(content_type) = &props.content_type {
            if content_type.len() > MAX_STRING_LEN {
                protocol_err!("content type too long");
            }
        }

        // NOTE: topic alias handling should come last since it updates the `topic_aliases` map
        if let Some(topic_alias) = props.topic_alias {
            macro_rules! invalid_alias (
                ($($fmt:tt)*) => {
                    return Err(ValidateError::Disconnect {
                        reason: DisconnectReasonCode::TopicAliasInvalid,
                        message: format!($($fmt)*)
                    });
                }
            );

            if topic_alias == 0 {
                // The spec isn't clear if this a protocol error or an invalid topic alias error
                invalid_alias!("topic alias cannot be zero");
            }

            if max_topic_alias == 0 {
                invalid_alias!("topic aliases are disabled on this broker");
            }

            if topic_alias > max_topic_alias {
                // Also not clear in the spec what error this should be,
                // though it indicates a client bug since we already told them what the max is
                invalid_alias!("topic alias exceeds maximum: {topic_alias} > {max_topic_alias}");
            }

            if topic.is_empty() {
                let Some(resolved_topic) = topic_aliases.get(&topic_alias) else {
                    invalid_alias!("unknown topic alias: {topic_alias}");
                };

                topic = resolved_topic.clone();
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
            user_properties: if !props.user_properties.is_empty() {
                Some(
                    props
                        .user_properties
                        .into_iter()
                        .map(|(k, v)| [k, v])
                        .collect(),
                )
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
