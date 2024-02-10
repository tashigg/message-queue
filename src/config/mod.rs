use color_eyre::eyre::WrapErr;
use serde::de::DeserializeOwned;
use std::path::Path;
use std::{fs, io};

pub mod addresses;

pub mod users;

fn read_toml<T: DeserializeOwned>(name: &str, path: &Path) -> crate::Result<T> {
    let config_toml = if path == Path::new("-") {
        io::read_to_string(io::stdin().lock()).wrap_err("error reading from stdin")?
    } else {
        fs::read_to_string(path)
            .wrap_err_with(|| format!("error reading from {}", path.display()))?
    };

    toml::from_str(&config_toml)
        .wrap_err_with(|| format!("error parsing {name} from {}", path.display()))
}
