use roaring::RoaringBitmap;
use std::num::NonZeroU16;

use tashi_collections::HashMap;

use rumqttd_shim::protocol::{QoS, SubscribeReasonCode};

#[derive(Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash, Debug)]
pub struct PacketId(NonZeroU16);

impl PacketId {
    pub fn new(id: u16) -> Option<PacketId> {
        NonZeroU16::new(id).map(Self)
    }

    pub fn get(self) -> u16 {
        self.0.get()
    }

    pub fn opt_to_raw(opt: Option<PacketId>) -> u16 {
        opt.map_or(0, Self::get)
    }
}

#[derive(Debug, Default)]
pub struct IncomingPacketSet {
    // Arbitrary packet IDs are allowed, which means we should probably not use `FnvHashMap`,
    // though the fact that they are only `u16`s makes a HashDOS attack seem unlikely.
    packets: HashMap<PacketId, IncomingPacketKind>,
}

#[derive(Debug)]
pub enum IncomingPacketKind {
    Pub(IncomingPub),
    Sub(IncomingSub),
    Unsub(IncomingUnsub),
}

#[derive(Debug)]
pub struct IncomingPub {}

#[derive(Debug)]
pub struct IncomingSub {
    /// Reason codes for the pending `SUBACK`.
    pub return_codes: Vec<SubscribeReasonCode>,
}

#[derive(Debug)]
pub struct IncomingUnsub {}

#[derive(Debug, thiserror::Error)]
#[error("replaced {packet_id:?} with {replaced:?}")]
pub struct ReplacedPacketError {
    pub packet_id: PacketId,
    pub replaced: IncomingPacketKind,
}

#[derive(Debug, thiserror::Error)]
pub enum RemovePacketError {
    #[error("unknown packet {packet_id:?}")]
    Unknown { packet_id: PacketId },
    #[error("expected packet {pattern} for {packet_id:?}, got {actual:?}")]
    WrongKind {
        packet_id: PacketId,
        pattern: &'static str,
        actual: IncomingPacketKind,
    },
}

#[derive(Debug)]
pub struct OutgoingPackets {
    // We generate packet IDs locally, but it's conceivable that a malicious client could manipulate
    // this using a clever ordering of `PUBACKS` and `PUBLISH`es from a different connection.
    publishes: HashMap<PacketId, OutgoingPublish>,
    free_packet_ids: RoaringBitmap,
}

#[derive(Debug)]
pub struct OutgoingPublish {
    pub qos: QoS,
}

macro_rules! remove_packet {
    ($this:expr, $packet_id:ident, $pattern:pat => $extract:ident) => {
        match $this.packets.remove(&$packet_id) {
            Some($pattern) => Ok($extract),
            Some(other) => Err(RemovePacketError::WrongKind {
                packet_id: $packet_id,
                pattern: stringify!($pattern),
                actual: other,
            }),
            None => Err(RemovePacketError::Unknown {
                packet_id: $packet_id,
            }),
        }
    };
}

impl IncomingPacketSet {
    pub fn contains(&self, packet: PacketId) -> bool {
        self.packets.contains_key(&packet)
    }

    // Instead of panicking internally, I figured we could `.expect()` the result at the call site,
    // so it's more obvious when there's a bug.
    //
    // Not checking the `Result` will trigger a lint warning.
    pub fn insert_sub(
        &mut self,
        packet_id: PacketId,
        sub: IncomingSub,
    ) -> Result<(), ReplacedPacketError> {
        self.insert(packet_id, IncomingPacketKind::Sub(sub))
    }

    fn insert(
        &mut self,
        packet_id: PacketId,
        kind: IncomingPacketKind,
    ) -> Result<(), ReplacedPacketError> {
        self.packets
            .insert(packet_id, kind)
            .map_or(Ok(()), |replaced| {
                Err(ReplacedPacketError {
                    packet_id,
                    replaced,
                })
            })
    }

    pub fn remove_sub(&mut self, packet_id: PacketId) -> Result<IncomingSub, RemovePacketError> {
        remove_packet!(self, packet_id, IncomingPacketKind::Sub(sub) => sub)
    }

    // TODO: other packet kinds
}

impl OutgoingPackets {
    pub fn new() -> Self {
        let mut free_packet_ids = RoaringBitmap::new();
        // A valid packet ID cannot be zero.
        free_packet_ids.insert_range(1..=65535);

        OutgoingPackets {
            publishes: HashMap::default(),
            free_packet_ids,
        }
    }

    pub fn len(&self) -> usize {
        self.publishes.len()
    }

    pub fn insert_publish(&mut self, qos: QoS) -> PacketId {
        assert_ne!(
            qos,
            QoS::AtMostOnce,
            "QoS 0 PUBLISHes cannot be assigned a packet ID"
        );

        let packet_id = self
            .free_packet_ids
            .min()
            .expect("BUG: out of free packet IDs");

        let packet_id = PacketId(
            NonZeroU16::new(
                u16::try_from(packet_id).expect("BUG: packet IDs exceeded max value of `u16`"),
            )
            .expect("BUG: packet IDs cannot be zero"),
        );

        self.free_packet_ids.remove(packet_id.0.get() as u32);

        self.publishes
            .insert(packet_id, OutgoingPublish { qos })
            .expect("BUG: replaced existing packet");

        packet_id
    }

    pub fn ack_publish(&mut self, packet_id: PacketId) -> Option<OutgoingPublish> {
        let publish = self.publishes.remove(&packet_id)?;

        self.free_packet_ids.insert(packet_id.0.get() as u32);

        Some(publish)
    }
}
