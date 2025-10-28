use serde::{Deserialize, Serialize};
use shvrpc::client::ClientConfig;
use url::Url;

#[derive(Debug, Serialize, Deserialize)]
#[derive(Default)]
pub struct Config {
    pub client: ClientConfig,
    pub db: DbConfig,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DbConfig {
    pub url: Url,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            url: Url::parse("sqlite::memory:").unwrap(),
        }
    }
}
