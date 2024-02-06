use std::net::SocketAddr;
use std::path::Path;
use std::{fs, io};

use color_eyre::eyre::WrapErr;
use tashi_consensus_engine::PublicKey;

#[derive(serde::Deserialize, serde::Serialize)]
pub struct Config {
    pub addresses: Vec<Address>,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct Address {
    pub key: PublicKey,
    pub addr: SocketAddr,
}

/// NOTE: uses blocking I/O internally.
pub fn read(path: &Path) -> crate::Result<Config> {
    let config_toml = if path == Path::new("-") {
        io::read_to_string(io::stdin().lock()).wrap_err("error reading from stdin")?
    } else {
        fs::read_to_string(path)
            .wrap_err_with(|| format!("error reading from {}", path.display()))?
    };

    toml::from_str(&config_toml).wrap_err("error parsing config from TOML")
}
