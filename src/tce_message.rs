use std::fmt::Debug;

use der::asn1::BitStringRef;
use der::Encode;

#[derive(der::Sequence)]
pub struct Transaction {
    data: TransactionData,
}

#[derive(der::Choice)]
#[asn1(tag_mode = "EXPLICIT")]
pub enum TransactionData {
    #[asn1(context_specific = "2", constructed = "true")]
    Publish(PublishTrasaction),
}

#[derive(der::Sequence)]
pub struct PublishTrasaction {
    topic: String,
    meta: PublishMeta,
    payload: Vec<u8>,
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
        BitStringRef::new(4, &[self.0])?.encoded_len()
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

/// Quality of service.
#[derive(Clone, Copy, Eq, PartialEq, Debug)]
pub enum QoS {
    AtMostOnce,
    AtLeastOnce,
    ExactlyOnce,
}

#[cfg(test)]
mod tests {
    use crate::tce_message::{PublishMeta, QoS};

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
}
