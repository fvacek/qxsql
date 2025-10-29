use std::collections::HashMap;

use async_trait::async_trait;
use chrono::FixedOffset;
use serde::{Deserialize, Serialize};
use shvproto::{RpcValue, from_rpcvalue};

#[async_trait]
pub trait SqlProvider {
    async fn query(&self, query: &str, params: Option<&Record>) -> anyhow::Result<SelectResult>;
    async fn exec(&self, query: &str, params: Option<&Record>) -> anyhow::Result<ExecResult>;
    async fn list_records(&self, table: &str, fields: Option<Vec<&str>>, ids_greater_than: Option<i64>, limit: Option<i64>) -> anyhow::Result<Vec<Record>>;
    async fn create_record(&self, table: &str, record: &Record) -> anyhow::Result<i64>;
    async fn read_record(&self, table: &str, id: i64, fields: Option<Vec<&str>>) -> anyhow::Result<Option<Record>>;
    async fn update_record(&self, table: &str, id: i64, record: &Record) -> anyhow::Result<bool>;
    async fn delete_record(&self, table: &str, id: i64) -> anyhow::Result<bool>;
}

pub type DateTime = chrono::DateTime<FixedOffset>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum DbValue {
    String(String),
    Int(i64),
    Bool(bool),
    DateTime(DateTime),
    Null,
}

impl From<i32> for DbValue {
    fn from(value: i32) -> Self {
        DbValue::Int(value as i64)
    }
}

impl From<i64> for DbValue {
    fn from(value: i64) -> Self {
        DbValue::Int(value)
    }
}

impl From<&str> for DbValue {
    fn from(value: &str) -> Self {
        DbValue::String(value.to_string())
    }
}

impl From<String> for DbValue {
    fn from(value: String) -> Self {
        DbValue::String(value)
    }
}

impl From<bool> for DbValue {
    fn from(value: bool) -> Self {
        DbValue::Bool(value)
    }
}

impl From<()> for DbValue {
    fn from(_value: ()) -> Self {
        DbValue::Null
    }
}

impl From<DateTime> for DbValue {
    fn from(value: DateTime) -> Self {
        DbValue::DateTime(value)
    }
}

pub type Record = HashMap<String, DbValue>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SqlOperation {
    Insert,
    Update,
    Delete,
}

/// Query and parameters tuple struct.
///
/// Supports deserialization from JSON arrays:
/// - `["SELECT * FROM users"]` - query only, params default to empty HashMap
/// - `["SELECT * WHERE id = :id", {"id": 42}]` - query with parameters
/// - `["SELECT * FROM table", {}]` - query with empty parameters object
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryAndParams(
    pub String,                                     // query
    #[serde(default)]
    pub Option<Record>, // params
    #[serde(default)]
    pub Option<String>,           // issuer
);

impl QueryAndParams {
    pub fn query(&self) -> &str {
        &self.0
    }

    pub fn params(&self) -> Option<&Record> {
        self.1.as_ref()
    }

    pub fn issuer(&self) -> Option<&str> {
        self.2.as_deref()
    }
}

impl TryFrom<&RpcValue> for QueryAndParams {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        from_rpcvalue(value).map_err(|e| e.to_string())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryAndParamsList(pub String, #[serde(default)] pub Vec<Vec<DbValue>>);
impl TryFrom<&RpcValue> for QueryAndParamsList {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        from_rpcvalue(value).map_err(|e| e.to_string())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RecUpdateParam {
    pub table: String,
    pub id: i64,
    pub record: Record,
    #[serde(default)]
    pub issuer: String,
}
impl TryFrom<&RpcValue> for RecUpdateParam {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        from_rpcvalue(value).map_err(|e| e.to_string())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RecInsertParam {
    pub table: String,
    pub record: Record,
    #[serde(default)]
    pub issuer: String,
}
impl TryFrom<&RpcValue> for RecInsertParam {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        from_rpcvalue(value).map_err(|e| e.to_string())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RecListParam {
    pub table: String,
    #[serde(default)]
    pub fields: Option<Vec<String>>,
    #[serde(default)]
    pub ids_above: Option<i64>, /// IDs greater than
    #[serde(default)]
    pub limit: Option<i64>,
}
impl TryFrom<&RpcValue> for RecListParam {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        from_rpcvalue(value).map_err(|e| e.to_string())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RecReadParam {
    pub table: String,
    pub id: i64,
    #[serde(default)]
    pub fields: Option<Vec<String>>,
}
impl TryFrom<&RpcValue> for RecReadParam {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        from_rpcvalue(value).map_err(|e| e.to_string())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RecDeleteParam {
    pub table: String,
    pub id: i64,
    #[serde(default)]
    pub issuer: String,
}
impl TryFrom<&RpcValue> for RecDeleteParam {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        from_rpcvalue(value).map_err(|e| e.to_string())
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct DbField {
    pub name: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ExecResult {
    pub rows_affected: i64,
    pub insert_id: Option<i64>,
}
#[derive(Debug, Serialize, Deserialize, Default, PartialEq)]
pub struct SelectResult {
    pub fields: Vec<DbField>,
    pub rows: Vec<Vec<DbValue>>,
}
impl SelectResult {
    pub fn record(&self, row: usize) -> Option<Record> {
        self.rows.get(row).map(|row| {
            row.iter()
                .zip(self.fields.iter())
                .map(|(value, field)| (field.name.clone(), value.clone()))
                .collect()
        })
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub enum RecOp {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct RecChng {
    pub table: String,
    pub id: i64,
    pub record: Option<Record>,
    pub op: RecOp,
    pub issuer: String,
}
