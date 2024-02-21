use color_eyre::eyre::{eyre, WrapErr};
use serde::de::DeserializeOwned;
use std::path::Path;
use std::{fs, io};

pub mod addresses;

pub mod users;

fn read_toml<T: DeserializeOwned>(name: &str, path: &Path) -> crate::Result<T> {
    read_toml_optional(name, path)?.ok_or_else(|| {
        eyre!(
            "error reading {name} from {}: file not found",
            path.display()
        )
    })
}

fn read_toml_optional<T: DeserializeOwned>(name: &str, path: &Path) -> crate::Result<Option<T>> {
    let config_toml = if path == Path::new("-") {
        io::read_to_string(io::stdin().lock())
            .wrap_err_with(|| format!("error reading {name} from stdin"))?
    } else {
        match fs::read_to_string(path) {
            Ok(s) => s,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => {
                return Err(eyre!(e).wrap_err(format!("error reading from {}", path.display())))
            }
        }
    };

    toml::from_str(&config_toml)
        .wrap_err_with(|| format!("error parsing {name} from {}", path.display()))
}
