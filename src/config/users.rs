use std::path::Path;

use tashi_collections::HashMap;

#[derive(serde::Deserialize)]
pub struct Users {
    pub users: HashMap<String, User>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct User {
    pub password_hash: String,
}

/// NOTE: uses blocking I/O internally.
pub fn read(path: &Path) -> crate::Result<Users> {
    super::read_toml("users", path)
}
