//! Our MQTT broker implementation.
//!
//! Currently, the only version of MQTT we plan to support is version 5.
//!
//! The protocol specification for MQTT v5 can be found at:
//! https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
use bytes::BytesMut;

pub use client_id::ClientId;
pub use keep_alive::KeepAlive;
pub use rumqttd_protocol as protocol;
use rumqttd_protocol::{Packet, Protocol};

pub use router::TceState;

pub mod broker;
pub mod client_id;

mod connect;

mod session;
pub mod trie;

mod mailbox;

mod publish;

mod retain;

mod router;

mod keep_alive;
mod packets;

slotmap::new_key_type! {
    struct ConnectionId;

    /// `SlotMap` key for known clients.
    struct ClientIndex;
}

/// Max length of a UTF-8 string allowed by the MQTT spec.
///
/// This is a hard limit as the protocol encoding of UTF-8 strings uses a fixed-width 2-byte prefix.
/// MQTT structures containing strings should NEVER be constructed with a string longer than this.
///
/// https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901010
pub const MAX_STRING_LEN: usize = 65535;

/// Runtime-polymorphic `Protocol` for dynamic MQTT protocol versioning.
///
/// Considered alternatives:
/// * `Box<dyn Protocol>`
///    * Pro: wouldn't allocate because `V4` and `V5` are both zero-sized.
///    * Con: two pointers wide instead of one byte
///    * Con: dynamic dispatch often acts as a pessimization
/// * `P: Protocol` parameter on `Connection`
///     * Pro: statically dispatched
///     * Con: Would require a completely separate code path for handling the `CONNECT` packet
///     * Con: codegen blowup from monomorphization (on top of the `S` parameter of `Connection`)
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum DynProtocol {
    V4,
    V5,
}

impl Protocol for DynProtocol {
    fn read_mut(
        &mut self,
        stream: &mut BytesMut,
        max_size: usize,
    ) -> Result<Packet, protocol::Error> {
        match self {
            Self::V5 => protocol::v5::V5.read_mut(stream, max_size),
            Self::V4 => protocol::v4::V4.read_mut(stream, max_size),
        }
    }

    fn write(&self, packet: Packet, write: &mut Vec<u8>) -> Result<usize, protocol::Error> {
        match self {
            Self::V5 => protocol::v5::V5.write(packet, write),
            Self::V4 => protocol::v4::V4.write(packet, write),
        }
    }
}
