//! QxSQLd - A SQL database interface for SHV (Silicon Heaven) protocol
//!
//! This library provides functionality to bridge SQL databases with the SHV protocol,
//! allowing SQL operations to be performed via SHV RPC calls.

pub mod sql;
pub mod sql_utils;

use async_trait::async_trait;
// Re-export commonly used types
pub use shvclient::AppState;
pub use sql::{
    DbValue, QueryAndParams, QueryAndParamsList, RecChng, RecDeleteParam,
    RecInsertParam, RecOp, RecReadParam, RecUpdateParam, SqlOperation
};
pub use sql_utils::{
    replace_named_with_positional_params, postgres_query_positional_args_from_sqlite
};

use std::collections::HashMap;

#[async_trait]
pub trait SqlProvider {
    async fn create_record(&self, table: &str, record: &HashMap<String, DbValue>) -> anyhow::Result<i64>;
    async fn read_record(&self, table: &str, id: i64) -> anyhow::Result<Option<HashMap<String, DbValue>>>;
    async fn update_record(&self, table: &str, id: i64, record: &HashMap<String, DbValue>) -> anyhow::Result<bool>;
    async fn delete_record(&self, table: &str, id: i64) -> anyhow::Result<bool>;
}
