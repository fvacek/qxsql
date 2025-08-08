use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub client: ClientConfig,
    pub database: DbConfig,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientConfig {
    pub url: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbConfig {
    pub url: String,
}
