//! QxSQL Daemon - A SQL database bridge for SHV (Silicon Heaven) protocol
//!
//! This library provides functionality to expose SQL databases through the SHV protocol,
//! allowing remote clients to execute SQL queries via SHV RPC calls.

pub mod config;
pub mod sql;
pub mod shvnode;
mod sql_utils;

pub use sql::{SqlOperation, SqlInfo, parse_sql_info};

// pub use config::{Config, DbConfig};
// pub use sql::{DbPool, sql_exec, sql_select};
// pub use shvnode::{dir, DirParam, LsParam};

// Re-export commonly used types from dependencies
// pub use shvproto::RpcValue;
// pub use shvrpc::Result as ShvResult;
// pub use sqlx::{Pool, Postgres, Sqlite};
