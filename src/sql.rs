use async_trait::async_trait;
use std::collections::HashMap;

use chrono::FixedOffset;
use serde::{Deserialize, Serialize};
use shvproto::{RpcValue, from_rpcvalue};

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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SqlOperation {
    Insert,
    Update,
    Delete,
    Other(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SqlInfo {
    pub operation: SqlOperation,
    pub table_name: String,
    pub is_returning_id: bool,
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
    #[serde(default)] pub HashMap<String, DbValue>, // params
    #[serde(default)] pub Option<String>,           // issuer
);

impl QueryAndParams {
    pub fn query(&self) -> &str {
        &self.0
    }

    pub fn params(&self) -> &HashMap<String, DbValue> {
        &self.1
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
    pub record: HashMap<String, DbValue>,
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
    pub record: HashMap<String, DbValue>,
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
pub struct RecReadParam {
    pub table: String,
    pub id: i64,
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
    pub insert_id: i64,
    pub info: SqlInfo,
}
#[derive(Debug, Serialize, Deserialize, Default, PartialEq)]
pub struct SelectResult {
    pub fields: Vec<DbField>,
    pub rows: Vec<Vec<DbValue>>,
}
impl SelectResult {
    pub fn record(&self, row: usize) -> Option<HashMap<String, DbValue>> {
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
    pub record: Option<HashMap<String, DbValue>>,
    pub op: RecOp,
    pub issuer: String,
}

#[async_trait]
pub trait Sql {
    async fn create_record(&self, table: &str, record: &HashMap<String, DbValue>) -> anyhow::Result<i64>;
    async fn read_record(&self, table: &str, id: i64) -> anyhow::Result<Option<HashMap<String, DbValue>>>;
    async fn update_record(&self, table: &str, id: i64, record: &HashMap<String, DbValue>) -> anyhow::Result<bool>;
    async fn delete_record(&self, table: &str, id: i64) -> anyhow::Result<bool>;
}
