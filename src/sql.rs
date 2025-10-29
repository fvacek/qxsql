use std::collections::HashMap;
use anyhow::anyhow;
use async_trait::async_trait;
use chrono::FixedOffset;
use serde::{Deserialize, Serialize};
use shvproto::{RpcValue, from_rpcvalue};

pub enum ListId {
    IdIsEqual(i64),
    IdsGreaterThan(i64),
    None
}
impl ListId {
    pub fn new_is_equal(id: Option<i64>) -> Self {
        match id {
            Some(id) => ListId::IdIsEqual(id),
            None => ListId::None,
        }
    }
    pub fn new_greater_than(id: Option<i64>) -> Self {
        match id {
            Some(id) => ListId::IdsGreaterThan(id),
            None => ListId::None,
        }
    }
}

#[async_trait]
pub trait SqlProvider: Send + Sync + Sized {
    async fn query(&self, query: &str, params: Option<&Record>) -> anyhow::Result<SelectResult>;
    async fn exec(&self, query: &str, params: Option<&Record>) -> anyhow::Result<ExecResult>;

    async fn list_records(&self, table: &str, fields: Option<Vec<&str>>, ids_greater_than: Option<i64>, limit: Option<i64>) -> anyhow::Result<Vec<Record>> {
        list_one_or_more_records(self, table, fields, ListId::new_greater_than(ids_greater_than), limit).await
    }
    async fn create_record(&self, table: &str, record: &Record) -> anyhow::Result<i64> {
        let keys = record.keys().map(|k| k.to_string()).collect::<Vec<_>>().join(", ");
        let vals = record.keys().map(|k| format!(":{k}")).collect::<Vec<_>>().join(", ");
        let query = format!("INSERT INTO {table} ({keys}) VALUES ({vals}) RETURNING id");
        let result = self.exec(&query, Some(record)).await?;
        result.insert_id.ok_or_else(|| anyhow!("Insert should return an ID"))
    }
    async fn read_record(&self, table: &str, id: i64, fields: Option<Vec<&str>>) -> anyhow::Result<Option<Record>> {
        let records = list_one_or_more_records(self, table, fields, ListId::new_is_equal(Some(id)), None).await?;
        Ok(records.into_iter().next())
    }
    async fn update_record(&self, table: &str, id: i64, record: &Record) -> anyhow::Result<bool> {
        let key_vals = record.keys().map(|k| format!("{k} = :{k}")).collect::<Vec<_>>().join(", ");
        let query = format!("UPDATE {table} SET {key_vals} WHERE id = {id}");
        self.exec(&query, Some(record)).await.and_then(|exec_result| {
            Ok(exec_result.rows_affected == 1)
        })
    }
    async fn delete_record(&self, table: &str, id: i64) -> anyhow::Result<bool> {
        let query = format!("DELETE FROM {table} WHERE id = {id}");
        self.exec(&query, None).await.and_then(|exec_result| {
            Ok(exec_result.rows_affected == 1)
        })
    }
}

async fn list_one_or_more_records<T: SqlProvider>(
    sql: &T,
    table: &str, fields: Option<Vec<&str>>,
    id: ListId,
    limit: Option<i64>,
) -> anyhow::Result<Vec<Record>> {
    let fields_str = fields.unwrap_or_else(|| vec!["*"]).join(", ");
    let mut qs = format!("SELECT {} FROM {}", fields_str, table);
    match id {
        ListId::IdIsEqual(id) => {
            qs.push_str(&format!(" WHERE id = {}", id));
        },
        ListId::IdsGreaterThan(id) => {
            qs.push_str(&format!(" WHERE id > {}", id));
        },
        ListId::None => {}
    }
    if let Some(limit) = limit {
        qs.push_str(&format!(" LIMIT {}", limit));
    }
    let result = sql.query(&qs, None).await?;
    Ok((0..result.rows.len())
        .filter_map(|i| result.record(i))
        .collect())
}

pub type DateTime = chrono::DateTime<FixedOffset>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum DbValue {
    String(String),
    Blob(Vec<u8>),
    Int(i64),
    Double(f64),
    Bool(bool),
    DateTime(DateTime),
    Null,
}
impl DbValue {
    pub fn to_bool(&self) -> bool {
        match self {
            DbValue::Bool(value) => *value,
            DbValue::Int(value) => *value != 0,
            _ => false,
        }
    }
    pub fn to_datetime(&self) -> Option<DateTime> {
        match self {
            DbValue::DateTime(value) => Some(*value),
            DbValue::String(value) => value.parse().ok(),
            _ => None,
        }
    }
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

impl From<f64> for DbValue {
    fn from(value: f64) -> Self {
        DbValue::Double(value)
    }
}

impl From<&[u8]> for DbValue {
    fn from(value: &[u8]) -> Self {
        DbValue::Blob(value.to_vec())
    }
}

impl From<Vec<u8>> for DbValue {
    fn from(value: Vec<u8>) -> Self {
        DbValue::Blob(value)
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
pub fn record_from_slice(arr: &[(&str, DbValue)]) -> Record {
    arr.iter()
        .map(|(key, value)| (key.to_string(), value.clone()))
        .collect()
}

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
