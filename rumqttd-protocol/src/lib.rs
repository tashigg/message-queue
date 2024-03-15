//! Source re-exports from the `rumqttd` crate to bypass coherence.
//!
//! If the files don't exist or are outdated, try `git submodule update --init --recursive`.
//!
//! Docs (for public items, anyway): https://docs.rs/rumqttd/latest/rumqttd/index.html
//!
//! This should save us having to write our own MQTT handling code.
//! `rumqttd` is published as a crate but many items are crate-private that
//! need to be accessible for any alternative broker implementation.
//! It also brings in a ton of non-optional crates that we don't strictly need.
//!
//! Additional impls for types in `rumqttd` may be added here,
//! such as getters for private fields.

// `rumqttd::protocol` which *is* public, but its packet types have crate-private fields
// that we'll need to add getters for.
#[allow(warnings)]
mod protocol;

// Vendored from https://github.com/tashigg/rumqtt
// which is forked from https://github.com/bytebeamio/rumqtt with changes.
pub use protocol::*;

use bytes::Bytes;

impl protocol::Publish {
    // Getters for crate-private fields

    pub fn with_all(
        dup: bool,
        qos: QoS,
        pkid: u16,
        retain: bool,
        topic: Bytes,
        payload: Bytes,
    ) -> Self {
        Self {
            dup,
            qos,
            pkid,
            retain,
            topic,
            payload,
        }
    }

    pub fn dup(&self) -> bool {
        self.dup
    }

    pub fn qos(&self) -> QoS {
        self.qos
    }

    pub fn pkid(&self) -> u16 {
        self.pkid
    }
}
