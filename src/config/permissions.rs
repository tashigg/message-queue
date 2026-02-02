use std::{path::Path, str::FromStr};

use crate::collections::HashMap;

use crate::mqtt::trie::Filter;

#[derive(serde::Deserialize, Default)]
pub struct PermissionsConfig {
    #[serde(default)]
    pub permissions: HashMap<String, TopicsConfig>,
}

#[derive(serde::Deserialize, Debug)]
pub struct TopicsConfig {
    pub topic: Vec<TopicPermissions>,
}

#[derive(serde::Deserialize, Debug)]
pub struct TopicPermissions {
    #[serde(deserialize_with = "from_str")]
    pub filter: Filter,
    pub allowed: Vec<TransactionType>,
    #[serde(default)]
    pub denied: Vec<TransactionType>,
}

fn from_str<'de, D>(deserializer: D) -> Result<Filter, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let s: String = serde::Deserialize::deserialize(deserializer)?;

    Filter::from_str(&s).map_err(serde::de::Error::custom)
}

#[derive(serde::Deserialize, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
pub enum TransactionType {
    Subscribe,
    Publish,
}

impl PermissionsConfig {
    pub fn get_topics_acl_config(&self, user: &str) -> Option<&TopicsConfig> {
        match self.permissions.get(user) {
            Some(permission) => Some(permission),
            None => self.permissions.get("*"),
        }
    }

    pub fn check_acl_config(
        &self,
        topics_config: Option<&TopicsConfig>,
        topic_name: &str,
        transaction_type: TransactionType,
    ) -> bool {
        // Allows everything if no topics config was found.
        topics_config.is_none_or(|perms| {
            perms
                .topic
                .iter()
                .find(|k| k.filter.matches_topic(topic_name))
                .is_none_or(|k| {
                    k.allowed.contains(&transaction_type)
                        || !k.denied.iter().all(|k| *k == transaction_type)
                })
        })
    }
}

pub fn read(path: &Path) -> crate::Result<PermissionsConfig> {
    Ok(
        super::read_toml_optional("permissions", path)?.unwrap_or_else(|| {
            tracing::debug!(
                "permissions file not found at {}; any user can do anything with the topics.",
                path.display()
            );

            PermissionsConfig::default()
        }),
    )
}
