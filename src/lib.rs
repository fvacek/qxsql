//! QxSQLd - A SQL database interface for SHV (Silicon Heaven) protocol
//!
//! This library provides functionality to bridge SQL databases with the SHV protocol,
//! allowing SQL operations to be performed via SHV RPC calls.

mod logger;
pub mod sql;
pub mod sql_utils;
#[cfg(feature = "recchng")]
pub use sql::recchng::QxSqlApiRecChng;

pub use qxsql_derive::{ToRecord, TryFromRecord};
pub use sql::FromDbValue;
pub use logger::setup_flexi_logger;
pub use sql::QxSqlApi;
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
