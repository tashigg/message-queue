use std::path::Path;

use tashi_collections::HashMap;

use crate::mqtt::trie::Filter;

#[derive(serde::Deserialize, serde::Serialize, Default)]
pub struct AclConfig {
    pub permissions: HashMap<String, TopicsConfig>,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct TopicsConfig {
    pub topic: Vec<TopicPermissions>,
}

#[derive(serde::Deserialize, serde::Serialize)]
pub struct TopicPermissions {
    pub filter: String,
    pub allowed: Vec<TransactionType>,
    pub denied: Vec<TransactionType>,
}

#[derive(serde::Deserialize, serde::Serialize, PartialEq, Eq)]
pub enum TransactionType {
    Subscribe,
    Publish,
}

impl AclConfig {
    pub fn get_topics_acl_config(&self, user: &str) -> Option<&TopicsConfig> {
        match self.permissions.get(user) {
            Some(permission) => Some(permission),
            None => self.permissions.get("*"),
        }
    }

    pub fn check_acl_config(
        &self,
        topics_config: Option<&TopicsConfig>,
        filter: &Filter,
        transaction_type: TransactionType,
    ) -> bool {
        // Allows everything if no topics config was found.
        topics_config.map_or(true, |perms| {
            !perms.topic.iter().any(|k| {
                k.allowed.iter().any(|k| *k == transaction_type) && filter.matches_topic(&k.filter)
            })
        })
    }
}

pub fn read(path: &Path) -> crate::Result<AclConfig> {
    Ok(super::read_toml_optional("acl", path)?.unwrap_or_else(|| {
        tracing::debug!(
            "acl file not found at {}; any user can do anything with the topics.",
            path.display()
        );

        AclConfig::default()
    }))
}
