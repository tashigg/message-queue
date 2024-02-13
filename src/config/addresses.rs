use std::net::SocketAddr;
use std::path::Path;

use tashi_consensus_engine::PublicKey;

#[derive(serde::Deserialize, serde::Serialize)]
pub struct Addresses {
    pub addresses: Vec<Address>,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct Address {
    pub key: PublicKey,
    pub addr: SocketAddr,
}

/// NOTE: uses blocking I/O internally.
pub fn read(path: &Path) -> crate::Result<Addresses> {
    super::read_toml("addresses", path)
}
