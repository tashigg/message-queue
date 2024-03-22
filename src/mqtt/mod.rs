//! Our MQTT broker implementation.
//!
//! Currently, the only version of MQTT we plan to support is version 5.
//!
//! The protocol specification for MQTT v5 can be found at:
//! https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
pub use rumqttd_protocol as protocol;

pub mod broker;
mod session;
pub mod trie;

mod mailbox;

mod publish;

mod router;

mod packets;

// It's a String to match rumqtt's type, but it could be [u8; 23] if we wanted.
type ClientId = String;

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
