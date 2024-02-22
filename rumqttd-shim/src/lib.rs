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
#[path = "../../rumqtt/rumqttd/src/protocol/mod.rs"]
#[allow(warnings)]
pub mod protocol;

// `rumqttd::router`, which is not public in the upstream source.
#[path = "../../rumqtt/rumqttd/src/router/mod.rs"]
#[allow(warnings)]
pub mod router;

#[path = "../../rumqtt/rumqttd/src/segments/mod.rs"]
#[allow(warnings)]
pub mod segments;

pub use router::{shared_subs::Strategy, Router, RouterConfig};

use crate::protocol::QoS;
use router::{ConnectionId, Filter, Notification, RouterId, Topic};
use segments::Storage;

type Cursor = (u64, u64);
type Offset = (u64, u64);

impl protocol::Publish {
    // Getters for crate-private fields

    pub fn dup(&self) -> bool {
        self.dup
    }

    pub fn qos(&self) -> QoS {
        self.qos
    }

    pub fn set_qos(&mut self, qos: QoS) {
        self.qos = qos;
    }

    pub fn pkid(&self) -> u16 {
        self.pkid
    }
}
