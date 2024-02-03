pub mod broker;

/// `rumqttd::protocol`
///
/// If the files don't exist, try `git submodule update --init --recursive`.
///
/// Docs: https://docs.rs/rumqttd/latest/rumqttd/protocol/index.html
///
/// This should save us having to write our own protocol codec.
/// `rumqttd` is published as a crate but all of the packet types have crate-private fields that
/// need to be accessible for any alternative broker implementation.
/// It also brings in a ton of non-optional crates that we don't strictly need.
///
/// TODO: upstream getters for private fields we need access to, and we can get rid of the submodule
#[path = "../../rumqtt/rumqttd/src/protocol/mod.rs"]
#[allow(warnings)]
pub mod protocol;

// Other potentially useful modules:
// `routing`, `segments`
//
// We may eventually want to just use `rumqttd` as a crate anyway, like we can just use it *as*
// our local MQTT broker implementation and then we just forward messages to our TCE network.
//
// However, for QoS1 and QoS2 packets, we probably don't want to respond to the sender
// with `PUBACK` and `PUBREL` respectively until we have some notion that the message has actually
// been acknowledged by other peers. Which means we need a lot of access to the broker internals.
