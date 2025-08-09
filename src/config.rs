use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub client: ClientConfig,
    pub databases: BTreeMap<String, DbConfig>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientConfig {
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DbConfig {
    pub url: String,
}
