use std::num::NonZeroU16;

use tashi_collections::HashMap;

use rumqttd_protocol::{SubscribeReasonCode, UnsubAckReason};

#[derive(Copy, Clone, PartialEq, Eq, Ord, PartialOrd, Hash, Debug)]
pub struct PacketId(NonZeroU16);

impl PacketId {
    pub const START: PacketId = PacketId(NonZeroU16::MIN);

    pub fn new(id: u16) -> Option<PacketId> {
        NonZeroU16::new(id).map(Self)
    }

    pub fn get(self) -> u16 {
        self.0.get()
    }

    pub fn opt_to_raw(opt: Option<PacketId>) -> u16 {
        opt.map_or(0, Self::get)
    }

    /// Increment `self` or wrap around to 1, returning the previous value.
    pub fn wrapping_increment(&mut self) -> Self {
        let ret = *self;
        *self = PacketId(self.0.checked_add(1).unwrap_or(NonZeroU16::MIN));
        ret
    }
}

#[derive(Debug, Default)]
pub struct IncomingPacketSet {
    // Arbitrary packet IDs are allowed, which means we should probably not use `FnvHashMap`,
    // though the fact that they are only `u16`s makes a HashDOS attack seem unlikely.
    packets: HashMap<PacketId, IncomingPacketKind>,
}

#[allow(dead_code)]
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
pub struct IncomingUnsub {
    /// Reason codes for the pending `UNSUBACK`.
    pub return_codes: Vec<UnsubAckReason>,
}

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

    pub fn insert_unsub(
        &mut self,
        packet_id: PacketId,
        unsub: IncomingUnsub,
    ) -> Result<(), ReplacedPacketError> {
        self.insert(packet_id, IncomingPacketKind::Unsub(unsub))
    }

    pub fn insert_pub(&mut self, packet_id: PacketId) -> Result<(), ReplacedPacketError> {
        self.insert(packet_id, IncomingPacketKind::Pub(IncomingPub {}))
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

    pub fn remove_unsub(
        &mut self,
        packet_id: PacketId,
    ) -> Result<IncomingUnsub, RemovePacketError> {
        remove_packet!(self, packet_id, IncomingPacketKind::Unsub(unsub) => unsub)
    }

    pub fn remove_pub(&mut self, packet_id: PacketId) -> Result<IncomingPub, RemovePacketError> {
        remove_packet!(self, packet_id, IncomingPacketKind::Pub(p) => p)
    }
}
