use bytes::Bytes;
use std::fmt::Debug;
use std::time::SystemTime;

use der::asn1::{BitStringRef, OctetString, OctetStringRef, Utf8StringRef};
use der::{Decode, Encode, Header, Length, Reader, Tag, TagNumber, Writer};

use rumqttd_shim::protocol::QoS;

#[derive(der::Sequence, Debug)]
pub struct Transaction {
    pub data: TransactionData,
}

#[derive(der::Choice, Debug)]
#[asn1(tag_mode = "EXPLICIT")]
pub enum TransactionData {
    #[asn1(context_specific = "2", constructed = "true")]
    Publish(PublishTrasaction),
}

// Transcoding to DER was chosen so that we are not baking-in a specific version of the MQTT protocol.
/// DER mapping of [`rumqttd::protocol::Packet::Publish`].
#[derive(der::Sequence, Debug)]
pub struct PublishTrasaction {
    pub topic: String,
    pub meta: PublishMeta,
    pub payload: BytesAsOctetString,
    /// A timestamp which can be compared with the timestamp of the event containing this transaction
    /// to determine how long the message was in the TCE transaction queue on the originating broker
    /// and calculate an appropriate correction for `message_expiry_interval`.
    ///
    /// The necessary correction is expected to be negligible unless the broker is disconnected from
    /// the network for an extended period of time.
    ///
    /// This value is in seconds because `message_expiry_interval` is in seconds.
    pub timestamp_received: TimestampSeconds,
    #[asn1(optional = "true")]
    pub properties: Option<PublishTransactionProperties>,
}

/// DER mapping of [`rumqttd::protocol::PublishProperties`].
///
/// `topic_alias` and `subscription_identifiers` are omitted as they are only used between
/// a client and a broker.
#[derive(der::Sequence, Debug)]
pub struct PublishTransactionProperties {
    // Note: these match the tag bytes used by the MQTT protocol, where possible.
    #[asn1(context_specific = "1", optional = "true")]
    pub payload_format_indicator: Option<u8>,

    /// The lifetime of the message, in seconds elapsed from its receipt.
    ///
    /// Per section 3.3.2.3.3 of the MQTT v5 spec:
    /// > The PUBLISH packet sent to a Client by the Server MUST contain a Message Expiry Interval
    /// > set to the received value minus the time that the Application Message
    /// > has been waiting in the Server
    ///
    /// Brokers receiving this message via consensus should use the consensus timestamp as a basis.
    #[asn1(context_specific = "2", optional = "true")]
    pub message_expiry_interval: Option<u32>,

    #[asn1(context_specific = "3", optional = "true")]
    pub content_type: Option<String>,

    #[asn1(context_specific = "8", optional = "true")]
    pub response_topic: Option<String>,

    #[asn1(context_specific = "9", optional = "true")]
    pub correlation_data: Option<BytesAsOctetString>,

    // Specified as 38 (0x26) but context_specific tags don't go that high
    #[asn1(context_specific = "30", optional = "true")]
    pub user_properties: Option<UserProperties>,
}

impl TryFrom<Transaction> for tashi_consensus_engine::ApplicationTransaction {
    type Error = color_eyre::eyre::Error;

    fn try_from(value: Transaction) -> Result<Self, Self::Error> {
        value.to_der()?.try_into()
    }
}

// Layout:
// 7 dup (1 bit)
// 6 retain (1 bit)
// 5..=4 qos (2 bit)
// 4..=0 unused (4 bit)
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct PublishMeta(u8);

impl der::FixedTag for PublishMeta {
    const TAG: der::Tag = der::Tag::BitString;
}
impl der::EncodeValue for PublishMeta {
    fn value_len(&self) -> der::Result<der::Length> {
        BitStringRef::new(4, &[self.0])?.value_len()
    }

    fn encode_value(&self, encoder: &mut impl der::Writer) -> der::Result<()> {
        BitStringRef::new(4, &[self.0])?.encode_value(encoder)
    }
}

impl<'a> der::DecodeValue<'a> for PublishMeta {
    fn decode_value<R: der::Reader<'a>>(reader: &mut R, header: der::Header) -> der::Result<Self> {
        let bit_string = BitStringRef::decode_value(reader, header)?;
        if bit_string.bit_len() != 4 {
            return Err(reader.error(der::ErrorKind::Value {
                tag: der::Tag::BitString,
            }));
        }

        let byte = *bit_string
            .raw_bytes()
            .first()
            .expect("4 bit bitstring should have a byte");

        // check for unused bits,
        if byte & 0b0000_1111 != 0 {
            return Err(reader.error(der::ErrorKind::Value {
                tag: der::Tag::BitString,
            }));
        }

        let qos = Self::unpack_qos(byte).ok_or_else(|| {
            reader.error(der::ErrorKind::Value {
                tag: der::Tag::BitString,
            })
        })?;

        // yes, this unpacks the bits of `Self` just to pack them up again.
        // In practice I doubt it's even remotely expensive.
        Ok(Self::new(
            qos,
            Self::unpack_retain(byte),
            Self::unpack_dup(byte),
        ))
    }
}

impl PublishMeta {
    const DUP_SHIFT: u8 = 7;
    const RETAIN_SHIFT: u8 = 6;
    /// quality of service takes 2 bits, there are two mental models for this shift:
    /// move the leftmost bit from bit 1 to bit 5, shift the rightmost bit to bit 4.
    const QOS_SHIFT: u8 = 5 - 1;

    pub const fn new(qos: QoS, retain: bool, dup: bool) -> Self {
        let dup = (dup as u8) << Self::DUP_SHIFT;
        let retain = (retain as u8) << Self::RETAIN_SHIFT;

        let qos = Self::pack_qos(qos);

        Self(dup | retain | qos)
    }

    const fn unpack_retain(byte: u8) -> bool {
        (byte >> Self::RETAIN_SHIFT) & 1 == 1
    }

    #[must_use = "this function has no side-effects"]
    pub const fn retain(self) -> bool {
        Self::unpack_retain(self.0)
    }

    const fn unpack_dup(byte: u8) -> bool {
        (byte >> Self::DUP_SHIFT) & 1 == 1
    }

    #[must_use = "this function has no side-effects"]
    pub const fn dup(self) -> bool {
        Self::unpack_dup(self.0)
    }

    const fn pack_qos(qos: QoS) -> u8 {
        // we're special so we use a special encoding
        // high bit: "do we have QoS"
        // low bit: "exactly once?"
        // 0b01 is currently invalid, but could be changed to mean "extended" or some such.
        // Why not pack this directly from the enum?
        let bits = match qos {
            QoS::AtMostOnce => 0b00,
            QoS::AtLeastOnce => 0b10,
            QoS::ExactlyOnce => 0b11,
        };

        bits << Self::QOS_SHIFT
    }

    const fn unpack_qos(byte: u8) -> Option<QoS> {
        match (byte >> Self::QOS_SHIFT) & 0b11 {
            0b00 => Some(QoS::AtMostOnce),
            0b01 => None,
            0b10 => Some(QoS::AtLeastOnce),
            0b11 => Some(QoS::ExactlyOnce),
            _ => unreachable!(),
        }
    }

    #[must_use = "this function has no side-effects"]
    pub const fn qos(self) -> QoS {
        match Self::unpack_qos(self.0) {
            Some(it) => it,
            None => unreachable!(),
        }
    }
}

impl Debug for PublishMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublishMeta")
            .field("qos", &self.qos())
            .field("retain", &self.retain())
            .field("dup", &self.dup())
            .finish()
    }
}

// FIXME: remove when `der 0.8.0` is released:
// https://github.com/RustCrypto/formats/issues/1356#issuecomment-1956225669
#[derive(Clone, Debug)]
pub struct BytesAsOctetString(pub Bytes);

impl der::FixedTag for BytesAsOctetString {
    const TAG: Tag = Tag::OctetString;
}

impl der::EncodeValue for BytesAsOctetString {
    fn value_len(&self) -> der::Result<Length> {
        OctetStringRef::new(&self.0)?.value_len()
    }

    fn encode_value(&self, encoder: &mut impl Writer) -> der::Result<()> {
        OctetStringRef::new(&self.0)?.encode_value(encoder)
    }
}

impl<'a> der::DecodeValue<'a> for BytesAsOctetString {
    fn decode_value<R: Reader<'a>>(reader: &mut R, header: Header) -> der::Result<Self> {
        Ok(BytesAsOctetString(
            OctetString::decode_value(reader, header)?
                .into_bytes()
                .into(),
        ))
    }
}

/// A timestamp in seconds from the Unix epoch, UTC.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct TimestampSeconds(pub u64);

impl der::FixedTag for TimestampSeconds {
    const TAG: Tag = Tag::Application {
        constructed: false,
        number: TagNumber::N0,
    };
}

impl der::EncodeValue for TimestampSeconds {
    fn value_len(&self) -> der::Result<Length> {
        u64::value_len(&self.0)
    }

    fn encode_value(&self, encoder: &mut impl Writer) -> der::Result<()> {
        u64::encode_value(&self.0, encoder)
    }
}

impl<'de> der::DecodeValue<'de> for TimestampSeconds {
    fn decode_value<R: Reader<'de>>(reader: &mut R, header: Header) -> der::Result<Self> {
        Ok(Self(u64::decode_value(reader, header)?))
    }
}

impl TimestampSeconds {
    pub fn now() -> Self {
        TimestampSeconds(
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("Fatal error: system time set before Unix epoch!")
                .as_secs(),
        )
    }
}

// The `der` crate doesn't support tuples,
// so we have to transcode to/from `SEQUENCE OF (SEQUENCE OF STRING)`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserProperties(pub Vec<(String, String)>);

impl UserProperties {
    pub fn new<I, K, V>(props: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        Self(
            props
                .into_iter()
                .map(|(k, v)| (k.into(), v.into()))
                .collect(),
        )
    }
}

impl der::FixedTag for UserProperties {
    const TAG: Tag = Tag::Sequence;
}

impl<'de> der::DecodeValue<'de> for UserProperties {
    fn decode_value<R: Reader<'de>>(reader: &mut R, header: Header) -> der::Result<Self> {
        reader.read_nested(header.length, |reader| {
            let mut properties = Vec::new();

            while !reader.is_finished() {
                let seq_header = der::Header::decode(reader)?;

                if seq_header.tag != Tag::Sequence {
                    return Err(reader.error(der::ErrorKind::TagUnexpected {
                        expected: Some(Tag::Sequence),
                        actual: seq_header.tag,
                    }));
                }

                let pair = reader.read_nested(seq_header.length, |reader| {
                    let key = String::decode(reader)?;
                    let value = String::decode(reader)?;
                    Ok((key, value))
                })?;

                properties.push(pair);
            }

            Ok(UserProperties(properties))
        })
    }
}

impl der::EncodeValue for UserProperties {
    fn value_len(&self) -> der::Result<Length> {
        self.0.iter().try_fold(Length::ZERO, |sum, (key, val)| {
            sum + encoded_pair_len(key, val)?
        })
    }

    fn encode_value(&self, encoder: &mut impl Writer) -> der::Result<()> {
        for (key, val) in &self.0 {
            let pair_len = encoded_pair_len(key, val)?;
            let header = Header::new(Tag::Sequence, pair_len)?;

            header.encode(encoder)?;
            key.encode(encoder)?;
            val.encode(encoder)?;
        }

        Ok(())
    }
}

fn encoded_pair_len(key: &str, val: &str) -> der::Result<Length> {
    // https://github.com/RustCrypto/formats/issues/1365
    (Utf8StringRef::new(key)?.encoded_len() + Utf8StringRef::new(val)?.encoded_len()?)?.for_tlv()
}

#[cfg(test)]
mod tests {
    use crate::tce_message::{PublishMeta, QoS, UserProperties};

    use der::{Decode, Encode};

    #[test]
    fn publish_meta() {
        let meta = PublishMeta::new(QoS::AtMostOnce, false, false);
        assert_eq!(meta.0, 0b0000_0000);
        assert_eq!(meta.qos(), QoS::AtMostOnce);
        assert!(!meta.retain());
        assert!(!meta.dup());

        let meta = PublishMeta::new(QoS::AtLeastOnce, true, true);
        assert_eq!(dbg!(meta).0, 0b1110_0000);
        assert_eq!(meta.qos(), QoS::AtLeastOnce);
        assert!(meta.retain());
        assert!(meta.dup());

        let meta = PublishMeta::new(QoS::ExactlyOnce, false, true);
        assert_eq!(meta.0, 0b1011_0000);
        assert_eq!(meta.qos(), QoS::ExactlyOnce);
        assert!(!meta.retain());
        assert!(meta.dup());
    }

    #[test]
    fn user_properties() {
        let props = UserProperties(vec![]);
        let encoded_props = props.to_der().unwrap();
        let decoded_props = UserProperties::from_der(&encoded_props).unwrap();

        assert_eq!(props, decoded_props);

        let props = UserProperties::new([("foo", "bar")]);
        let encoded_props = props.to_der().unwrap();
        let decoded_props = UserProperties::from_der(&encoded_props).unwrap();

        assert_eq!(props, decoded_props);

        let props = UserProperties::new([("foo", "bar"), ("hello", "world"), ("tashi", "gg")]);
        let encoded_props = props.to_der().unwrap();
        let decoded_props = UserProperties::from_der(&encoded_props).unwrap();

        assert_eq!(props, decoded_props);
    }
}
