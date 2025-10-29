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
