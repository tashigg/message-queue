use std::path::Path;

use tashi_collections::HashMap;

#[derive(serde::Deserialize, serde::Serialize, Default)]
pub struct UsersConfig {
    #[serde(default, rename = "users")]
    pub by_username: HashMap<String, User>,

    #[serde(default, skip_serializing_if = "AuthConfig::is_default")]
    pub auth: AuthConfig,
}

#[derive(serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub struct User {
    pub password_hash: String,
}

#[derive(serde::Deserialize, serde::Serialize, clap::Args, Clone, Debug, PartialEq, Eq)]
#[serde(default, rename_all = "kebab-case")]
pub struct AuthConfig {
    /// Allow clients to authenticate without providing user credentials.
    #[clap(long)]
    pub allow_anonymous_login: bool,

    /// If enabled, don't respond if CONNECT handling (including authentication) for a client fails.
    ///
    /// This can be used to avoid advertising that an MQTT broker is running on the given port,
    /// which makes it harder to categorize in a port scanning attack and identify as a potential
    /// target.
    ///
    /// By default, a response is sent to the client detailing the reason why the handshake failed.
    ///
    /// If enabled, the error is simply logged at DEBUG level and the socket is silently closed.
    #[clap(long)]
    pub silent_connect_errors: bool,
}

impl AuthConfig {
    /// Merge any overridden values from another `AuthConfig`, e.g. from CLI.
    pub fn merge(&mut self, overrides: &AuthConfig) {
        self.allow_anonymous_login |= overrides.allow_anonymous_login;
    }

    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

// NOTE: explicit implementation because the values here should be specified explicitly.
#[allow(clippy::derivable_impls)]
impl Default for AuthConfig {
    fn default() -> Self {
        AuthConfig {
            // Forbid anonymous logins by default.
            allow_anonymous_login: false,
            // Respond with CONNACK and an error reason code if CONNECT handling fails.
            silent_connect_errors: false,
        }
    }
}

/// NOTE: uses blocking I/O internally.
pub fn read(path: &Path) -> crate::Result<UsersConfig> {
    Ok(
        super::read_toml_optional("users", path)?.unwrap_or_else(|| {
            tracing::debug!(
                "users file not found at {}; assuming no users specified",
                path.display()
            );

            UsersConfig::default()
        }),
    )
}
