use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub client: ClientConfig,
    pub event_databases: BTreeMap<i64, DbConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientConfig {
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DbConfig {
    pub url: String,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            url: "sqlite:memory:".to_string(),
        }
    }
}
