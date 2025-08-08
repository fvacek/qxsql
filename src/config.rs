use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub client: ClientConfig,
    pub database: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientConfig {
    pub url: String,
}
