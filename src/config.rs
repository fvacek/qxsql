use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub client: ClientConfig,
    pub database: DbConfig,
    pub qbe_path: String,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            qbe_path: "qbe".to_string(),
            client: Default::default(),
            database: Default::default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClientConfig {
    pub url: String,
}
impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            url: "tcp://localhost:3755?user=test&password=password".to_string(),
        }
    }
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
