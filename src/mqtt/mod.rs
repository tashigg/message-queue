pub use rumqttd_shim::{protocol, router};

pub mod broker;
mod connection;
mod publish;
mod session;

// It's a String to match rumqtt's type, but it could be [u8; 23] if we wanted.
type ClientId = String;
