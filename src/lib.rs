//! QxSQLd - A SQL database interface for SHV (Silicon Heaven) protocol
//!
//! This library provides functionality to bridge SQL databases with the SHV protocol,
//! allowing SQL operations to be performed via SHV RPC calls.

pub mod sql;
pub mod sql_utils;

// Re-export commonly used types
pub use shvclient::AppState;
pub use sql::{
    DbValue, QueryAndParams, QueryAndParamsList, RecChng, RecDeleteParam,
    RecInsertParam, RecOp, RecReadParam, RecUpdateParam, SqlOperation
};
pub use sql_utils::{
    replace_named_with_positional_params, postgres_query_positional_args_from_sqlite
};

use std::sync::OnceLock;

// Global configuration for database access control
#[derive(Debug)]
pub struct GlobalConfig {
    pub write_database_token: Option<String>,
}

static GLOBAL_CONFIG: OnceLock<GlobalConfig> = OnceLock::new();

/// Initialize the global configuration
pub fn init_global_config(write_database_token: Option<String>) -> Result<(), &'static str> {
    GLOBAL_CONFIG
        .set(GlobalConfig {
            write_database_token,
        })
        .map_err(|_| "Global config should only be set once")
}

/// Check write authorization for database operations
pub fn check_write_authorization(request: &shvrpc::RpcMessage) -> Result<(), shvrpc::rpcmessage::RpcError> {
    use shvrpc::{rpcmessage::{RpcError, RpcErrorCode}, RpcMessageMetaTags};

    let config = GLOBAL_CONFIG
        .get()
        .expect("Global config should be initialized");
    if let Some(write_token) = &config.write_database_token {
        if let Some(user_id) = request.user_id()
            && let Some(user_token) = user_id.split(';').next()
            && user_token != write_token
        {
            return Ok(());
        }

        return Err(RpcError::new(
            RpcErrorCode::PermissionDenied,
            "Unauthorized",
        ));
    }
    Ok(())
}
