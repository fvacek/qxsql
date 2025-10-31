//! QxSQLd - A SQL database interface for SHV (Silicon Heaven) protocol
//!
//! This library provides functionality to bridge SQL databases with the SHV protocol,
//! allowing SQL operations to be performed via SHV RPC calls.

pub mod sql;
pub mod sql_utils;

// Re-export commonly used types
pub use sql::{
    DbValue, QueryAndParams, QueryAndParamsList, RecChng, RecDeleteParam,
    RecInsertParam, RecOp, RecReadParam, RecUpdateParam, SqlOperation,
    Record,
    RecListParam,
    QueryResult,
    ExecResult
};

pub fn string_list_to_ref_vec(fields: &Option<Vec<String>>) -> Option<Vec<&str>> {
    fields.as_ref().map(|v| v.iter().map(|s| s.as_str()).collect())
}
