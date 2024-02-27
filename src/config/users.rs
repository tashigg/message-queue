use std::path::Path;

use tashi_collections::HashMap;

#[derive(serde::Deserialize, serde::Serialize, Default)]
pub struct Users {
    #[serde(rename = "users")]
    pub by_username: HashMap<String, User>,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct User {
    pub password_hash: String,
}

/// NOTE: uses blocking I/O internally.
pub fn read(path: &Path) -> crate::Result<Users> {
    Ok(
        super::read_toml_optional("users", path)?.unwrap_or_else(|| {
            // TODO: forbid this unless anonymous logins are allowed [TG-400]
            tracing::debug!(
                "users file not found at {}; assuming no users specified",
                path.display()
            );

            Users::default()
        }),
    )
}
