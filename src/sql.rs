use std::backtrace::Backtrace;
use std::collections::HashMap;

use chrono::Utc;
use log::error;
use serde::{Deserialize, Serialize};
use shvproto::{from_rpcvalue, RpcValue};
use sqlx::prelude::FromRow;
use sqlx::{Pool, Postgres, Sqlite, sqlite::SqliteRow, postgres::PgRow};
use sqlx::{Column, Row, TypeInfo, ValueRef, postgres::PgPool, SqlitePool};
use anyhow::{anyhow, bail};

use crate::sql_utils::{self, parse_rfc3339_datetime, postgres_query_positional_args_from_sqlite, SqlInfo};

pub enum DbPool {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum DbValue {
    String(String),
    Int(i64),
    Bool(bool),
    DateTime(chrono::DateTime<Utc>),
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

/// Query and parameters tuple struct.
///
/// Supports deserialization from JSON arrays:
/// - `["SELECT * FROM users"]` - query only, params default to empty HashMap
/// - `["SELECT * WHERE id = :id", {"id": 42}]` - query with parameters
/// - `["SELECT * FROM table", {}]` - query with empty parameters object
#[derive(Debug,Serialize,Deserialize)]
pub struct QueryAndParams(
    pub String, // query
    #[serde(default)]
    pub HashMap<String, DbValue>, // params
    #[serde(default)]
    pub Option<String>, // issuer
);

impl QueryAndParams {
#[cfg(test)]
pub fn new(query: String, params: HashMap<String, DbValue>, issuer: Option<&str>) -> Self {
        QueryAndParams(query, params, issuer.map(|s| s.to_string()))
    }

    pub fn query(&self) -> &str {
        &self.0
    }

    pub fn params(&self) -> &HashMap<String, DbValue> {
        &self.1
    }

    #[cfg(test)]
    pub fn query_mut(&mut self) -> &mut String {
        &mut self.0
    }

    #[cfg(test)]
    pub fn params_mut(&mut self) -> &mut HashMap<String, DbValue> {
        &mut self.1
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

#[derive(Debug,Serialize,Deserialize)]
pub struct QueryAndParamsList(
    pub String,
    #[serde(default)]
    pub Vec<Vec<DbValue>>
);
impl TryFrom<&RpcValue> for QueryAndParamsList {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        from_rpcvalue(value).map_err(|e| e.to_string())
    }
}

#[derive(Debug, Serialize,Deserialize, PartialEq)]
pub struct DbField {
    pub name: String,
}

#[derive(Debug, Serialize,Deserialize)]
pub struct ExecResult {
    pub rows_affected: i64,
    pub insert_id: i64,
    pub info: SqlInfo
}
#[derive(Debug, Serialize,Deserialize, Default, PartialEq)]
pub struct SelectResult {
    pub fields: Vec<DbField>,
    pub rows: Vec<Vec<DbValue>>,
}

pub async fn sql_select(db: &DbPool, query: &QueryAndParams) -> anyhow::Result<SelectResult> {
    match db {
        DbPool::Sqlite(pool) => sql_select_sqlite(pool, query).await,
        DbPool::Postgres(pool) => sql_select_postgres(pool, query).await,
    }
}

#[derive(Debug, Serialize,Deserialize, PartialEq)]
pub struct RecChng {
    pub table: String,
    pub id: i64,
    pub record: Option<HashMap<String, DbValue>>,
    pub issuer: String,
}

pub async fn sql_exec(db: &DbPool, query: &QueryAndParams) -> anyhow::Result<ExecResult> {
    match db {
        DbPool::Sqlite(pool) => sql_exec_sqlite(pool, query).await,
        DbPool::Postgres(pool) => sql_exec_postgres(pool, query).await,
    }
}

pub async fn sql_exec_transaction(db: &DbPool, query: &QueryAndParamsList) -> anyhow::Result<()> {
    match db {
        DbPool::Sqlite(pool) => sql_exec_transaction_sqlite(pool, query).await,
        DbPool::Postgres(pool) => sql_exec_transaction_postgres(pool, query).await,
    }
}

fn prepare_sql_with_params(query: &QueryAndParams, repl_char: char) -> String {
    let keys = query.params().keys().map(|s| s.as_str()).collect::<Vec<_>>();
    sql_utils::replace_named_with_positional_params(query.query(), &keys, repl_char)
}

// Helper macro to bind parameters to a query - eliminates duplication in transactions
macro_rules! bind_db_values {
    ($query:expr, $params:expr) => {{
        let mut q = $query;
        for val in $params {
            q = match val {
                DbValue::String(s) => q.bind(s),
                DbValue::Int(i) => q.bind(i),
                DbValue::Bool(b) => q.bind(b),
                DbValue::DateTime(dt) => q.bind(dt.to_rfc3339()),
                DbValue::Null => q.bind(None::<&str>),
            };
        }
        q
    }};
}

// Common logic for SQL execution
fn parse_sql_info(query: &QueryAndParams) -> anyhow::Result<SqlInfo> {
    match crate::sql_utils::parse_sql_info(query.query()) {
        Ok(sql_info) => {
            Ok(sql_info)
        },
        Err(e) => {
            bail!("sql_exec: parse SQL query error: {}", e)
        }
    }
}

macro_rules! sql_exec_impl {
    ($db_pool:expr, $query:expr, $repl_char:expr) => {{
        let sql = prepare_sql_with_params($query, $repl_char);
        let q = sqlx::query(&sql);
        let q = bind_db_values!(q, $query.params().values());
        let info = parse_sql_info($query)?;
        if info.is_returning_id {
            let row = q.fetch_one($db_pool).await.map_err(sqlx2_to_anyhow)?;
            let insert_id = row.get(0);
            Ok(ExecResult { rows_affected: 1, insert_id, info })
        } else {
            let result = q.execute($db_pool).await?;
            Ok(ExecResult { rows_affected: result.rows_affected() as i64, insert_id: 0, info })
        }
    }};
}

async fn sql_exec_sqlite(db_pool: &Pool<Sqlite>, query: &QueryAndParams) -> anyhow::Result<ExecResult> {
    sql_exec_impl!(db_pool, query, '?')

}

async fn sql_exec_postgres(db_pool: &Pool<Postgres>, query: &QueryAndParams) -> anyhow::Result<ExecResult> {
    sql_exec_impl!(db_pool, query, '$')
}

async fn sql_exec_transaction_sqlite(db_pool: &Pool<Sqlite>, query_list: &QueryAndParamsList) -> anyhow::Result<()> {
    let mut tx = db_pool.begin().await?;
    let sql = &query_list.0;
    for param in &query_list.1 {
        let q = sqlx::query(sql);
        let q = bind_db_values!(q, param);
        let _result = q.execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

async fn sql_exec_transaction_postgres(db_pool: &Pool<Postgres>, query_list: &QueryAndParamsList) -> anyhow::Result<()> {
    let mut tx = db_pool.begin().await?;
    let sql = postgres_query_positional_args_from_sqlite(&query_list.0);
    for param in &query_list.1 {
        let q = sqlx::query(&sql);
        let q = bind_db_values!(q, param);
        let _result = q.execute(&mut *tx).await?;
    }
    tx.commit().await?;
    Ok(())
}

fn process_rows<R>(rows: &[R], value_extractor: impl Fn(&R, usize) -> anyhow::Result<DbValue>) -> anyhow::Result<SelectResult>
where
    R: Row,
{
    let mut result: SelectResult = Default::default();
    for (ix, rowx) in rows.iter().enumerate() {
        let cols = rowx.columns();
        let mut row: Vec<DbValue> = Vec::with_capacity(cols.len());
        for (i, col) in cols.iter().enumerate() {
            let col_name = col.name();
            if ix == 0 {
                result.fields.push(DbField { name: col_name.to_string() });
            }
            let val = value_extractor(rowx, i)?;
            row.push(val);
        }
        result.rows.push(row);
    }
    Ok(result)
}

async fn sql_select_sqlite(db_pool: &Pool<Sqlite>, query: &QueryAndParams) -> anyhow::Result<SelectResult> {
    let sql = prepare_sql_with_params(query, '?');
    let q = sqlx::query(&sql);
    let q = bind_db_values!(q, query.params().values());
    let rows = q.fetch_all(db_pool).await?;
    process_rows(&rows, db_value_from_sqlite_row)
}

async fn sql_select_postgres(db_pool: &Pool<Postgres>, query: &QueryAndParams) -> anyhow::Result<SelectResult> {
    let sql = prepare_sql_with_params(query, '$');
    let q = sqlx::query(&sql);
    let q = bind_db_values!(q, query.params().values());
    let rows = q.fetch_all(db_pool).await?;
    process_rows(&rows, db_value_from_postgres_row)
}

fn sqlx_to_anyhow(err: Box<dyn std::error::Error + Send + Sync>) -> anyhow::Error {
    error!("SQL Error: {err}\nbacktrace: {}", Backtrace::capture());
    anyhow!("SQL error: {}", err)
}

fn sqlx2_to_anyhow(err: sqlx::Error) -> anyhow::Error {
    error!("SQL Error: {err}\nbacktrace: {}", Backtrace::capture());
    anyhow!("SQL error: {}", err)
}

// Helper function to determine if a type is text-based
fn is_text_type(type_name: &str) -> bool {
    type_name.contains("TEXT") ||
    type_name.contains("STRING") ||
    type_name.contains("VARCHAR")
}

fn db_value_from_sqlite_row(row: &SqliteRow, index: usize) -> anyhow::Result<DbValue> {
    let raw_val = row.try_get_raw(index)?;
    let type_name = raw_val.type_info().name().to_uppercase();
    let is_null = raw_val.is_null();

    if is_null {
        return Ok(DbValue::Null);
    }

    if is_text_type(&type_name) {
        let s = <String as sqlx::decode::Decode<Sqlite>>::decode(raw_val).map_err(sqlx_to_anyhow)?;
        if s.len() > 18 && s.as_bytes()[10] == b'T' && let Some(dt) = parse_rfc3339_datetime(&s) {
            Ok(DbValue::DateTime(dt))
        } else {
            Ok(DbValue::String(s))
        }
    } else if type_name.contains("INT") {
        Ok(DbValue::Int(<i64 as sqlx::decode::Decode<Sqlite>>::decode(raw_val).map_err(sqlx_to_anyhow)?))
    } else if type_name.contains("BOOL") {
        Ok(DbValue::Bool(<bool as sqlx::decode::Decode<Sqlite>>::decode(raw_val).map_err(sqlx_to_anyhow)?))
    } else {
        anyhow::bail!("Unsupported type: {}", type_name);
    }
}

fn db_value_from_postgres_row(row: &PgRow, index: usize) -> anyhow::Result<DbValue> {
    let raw_val = row.try_get_raw(index)?;
    let type_name = raw_val.type_info().name().to_uppercase();
    let is_null = raw_val.is_null();

    if is_null {
        return Ok(DbValue::Null);
    }

    if is_text_type(&type_name) {
        let s = <String as sqlx::decode::Decode<Postgres>>::decode(raw_val).map_err(sqlx_to_anyhow)?;
        // 2025-09-12T15:04:05Z
        if s.len() > 18 && s.as_bytes()[10] == b'T' && let Some(dt) = parse_rfc3339_datetime(&s) {
            Ok(DbValue::DateTime(dt))
        } else {
            Ok(DbValue::String(s))
        }
    } else if type_name.contains("INT") {
        Ok(DbValue::Int(<i64 as sqlx::decode::Decode<Postgres>>::decode(raw_val).map_err(sqlx_to_anyhow)?))
    } else if type_name.contains("BOOL") {
        Ok(DbValue::Bool(<bool as sqlx::decode::Decode<Postgres>>::decode(raw_val).map_err(sqlx_to_anyhow)?))
    } else {
        anyhow::bail!("Unsupported type: {}", type_name);
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, FromRow)]
pub(crate) struct EventRecord {
    pub id: i64,
    pub api_token: String,
    pub owner: String,
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::sql_utils::{parse_sql_info, SqlInfo, SqlOperation};
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::postgres::PgPoolOptions;
    use log::warn;

    async fn test_sql_select_with_db(db_pool: DbPool) {
        let qp = QueryAndParams(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, age INTEGER, created TEXT)".into(),
            HashMap::new(),
            None
        );
        let res = sql_exec(&db_pool, &qp).await;
        res.unwrap();

        let dt = DbValue::DateTime(parse_rfc3339_datetime("2025-09-12T15:04:05Z").unwrap());
        let qp = QueryAndParams(
            "INSERT INTO users (id, name, email, created) VALUES (:id, :name, :name, :created)".into(),
            [ ("id".to_string(), 1.into()), ("name".to_string(), "Jane Doe".into()), ("created".to_string(), dt.clone()), ].into_iter().collect(),
            None
        );
        let res = sql_exec(&db_pool, &qp).await;
        res.unwrap();

        let qp = QueryAndParams(
            "SELECT * FROM users".into(),
            HashMap::new(),
            None
        );
        let result = sql_select(&db_pool, &qp).await;
        let expected = SelectResult {
            fields: vec![
                DbField { name: "id".to_string() },
                DbField { name: "name".to_string() },
                DbField { name: "email".to_string() },
                DbField { name: "age".to_string() },
                DbField { name: "created".to_string() },
            ],
            rows: vec![vec![1.into(), "Jane Doe".into(), "Jane Doe".into(), DbValue::Null, dt]],
        };
        assert_eq!(result.unwrap(), expected);
    }

    #[tokio::test]
    async fn test_sql_sqlite() {
        let db_pool = DbPool::Sqlite(SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await
            .unwrap());
        test_sql_select_with_db(db_pool).await;
    }

    #[tokio::test]
    async fn test_sql_postgres() {
        if let Ok(db_url) = std::env::var("QXSQLD_POSTGRES_URL") {
            let db_pool = DbPool::Postgres(PgPoolOptions::new()
                .connect(&db_url)
                .await
                .unwrap());
            let _ = match &db_pool {
                DbPool::Postgres(pool) => {
                    let qp = QueryAndParams(
                        "DROP TABLE IF EXISTS users".into(),
                        HashMap::new(),
                        None
                    );
                    sql_exec_postgres(pool, &qp).await
                },
                _ => panic!("not a postgres pool"),
            };
            test_sql_select_with_db(db_pool).await;
        } else {
            warn!(r#"export QXSQLD_POSTGRES_URL= "postgres://myuser:mypassword@localhost/mydb?options=--search_path%3Dmy_app_schema""#);
            warn!("Skipping postgres test, QXSQLD_POSTGRES_URL not set");
        }
    }

    #[test]
    fn test_query_and_params_convenience_methods() {
        let mut qp = QueryAndParams::new(
            "SELECT * FROM users WHERE id = :id".to_string(),
            [("id".to_string(), DbValue::Int(1))].into_iter().collect(),
            None
        );

        // Test getter methods
        assert_eq!(qp.query(), "SELECT * FROM users WHERE id = :id");
        assert_eq!(qp.params().get("id"), Some(&DbValue::Int(1)));

        // Test mutable methods
        *qp.query_mut() = "SELECT * FROM posts WHERE id = :id".to_string();
        qp.params_mut().insert("id".to_string(), DbValue::Int(2));

        assert_eq!(qp.query(), "SELECT * FROM posts WHERE id = :id");
        assert_eq!(qp.params().get("id"), Some(&DbValue::Int(2)));
    }

    #[test]
    fn test_serde_default_params() {
        // Test deserialization with missing params field (tuple with only first element)
        let json = r#"["SELECT * FROM users"]"#;
        let qp: QueryAndParams = serde_json::from_str(json).unwrap();

        assert_eq!(qp.query(), "SELECT * FROM users");
        assert!(qp.params().is_empty());

        // Test deserialization with params field present
        let json = r#"["SELECT * FROM users WHERE id = :id", {"id": 42}]"#;
        let qp: QueryAndParams = serde_json::from_str(json).unwrap();

        assert_eq!(qp.query(), "SELECT * FROM users WHERE id = :id");
        assert_eq!(qp.params().get("id"), Some(&DbValue::Int(42)));
    }

    #[test]
    fn test_serde_serialization_roundtrip() {
        // Test with empty params
        let qp = QueryAndParams("SELECT * FROM users".to_string(), HashMap::new(), None);
        let json = serde_json::to_string(&qp).unwrap();
        let deserialized: QueryAndParams = serde_json::from_str(&json).unwrap();

        assert_eq!(qp.query(), deserialized.query());
        assert_eq!(qp.params(), deserialized.params());

        // Test with params
        let mut params = HashMap::new();
        params.insert("id".to_string(), DbValue::Int(123));
        params.insert("name".to_string(), DbValue::String("Alice".to_string()));
        params.insert("active".to_string(), DbValue::Bool(true));
        params.insert("deleted".to_string(), DbValue::Null);

        let qp = QueryAndParams("SELECT * FROM users WHERE id = :id AND name = :name".to_string(), params, None);
        let json = serde_json::to_string(&qp).unwrap();
        let deserialized: QueryAndParams = serde_json::from_str(&json).unwrap();

        assert_eq!(qp.query(), deserialized.query());
        assert_eq!(qp.params().len(), deserialized.params().len());
        assert_eq!(qp.params().get("id"), deserialized.params().get("id"));
        assert_eq!(qp.params().get("name"), deserialized.params().get("name"));
        assert_eq!(qp.params().get("active"), deserialized.params().get("active"));
        assert_eq!(qp.params().get("deleted"), deserialized.params().get("deleted"));
    }

    #[test]
    fn test_serde_different_json_formats() {
        // Test that we can handle different JSON input formats

        // Array format with just query (uses default for params)
        let json1 = r#"["SELECT * FROM table"]"#;
        let qp1: QueryAndParams = serde_json::from_str(json1).unwrap();
        assert_eq!(qp1.query(), "SELECT * FROM table");
        assert!(qp1.params().is_empty());

        // Array format with both query and params
        let json2 = r#"["SELECT * WHERE id = :id", {"id": 100}]"#;
        let qp2: QueryAndParams = serde_json::from_str(json2).unwrap();
        assert_eq!(qp2.query(), "SELECT * WHERE id = :id");
        assert_eq!(qp2.params().get("id"), Some(&DbValue::Int(100)));

        // Array format with empty params object
        let json3 = r#"["SELECT * FROM users", {}]"#;
        let qp3: QueryAndParams = serde_json::from_str(json3).unwrap();
        assert_eq!(qp3.query(), "SELECT * FROM users");
        assert!(qp3.params().is_empty());
    }

    async fn test_sql_exec_transaction_with_db(db_pool: DbPool) {
        // Create test table
        let create_table = QueryAndParams(
            "CREATE TABLE test_users (id INTEGER, name TEXT, active BOOL)".into(),
            HashMap::new(),
            None
        );
        sql_exec(&db_pool, &create_table).await.unwrap();

        // Test successful transaction with multiple inserts
        let transaction_query = QueryAndParamsList(
            "INSERT INTO test_users (id, name, active) VALUES (?, ?, ?)".into(),
            vec![
                vec![DbValue::Int(1), DbValue::String("Alice".into()), DbValue::Bool(true)],
                vec![DbValue::Int(2), DbValue::String("Bob".into()), DbValue::Bool(false)],
                vec![DbValue::Int(3), DbValue::String("Charlie".into()), DbValue::Bool(true)],
            ],
        );

        let result = sql_exec_transaction(&db_pool, &transaction_query).await;
        assert!(result.is_ok(), "Transaction should succeed: {:?}", result);

        // Verify all rows were inserted
        let select_query = QueryAndParams(
            "SELECT COUNT(*) as count FROM test_users".into(),
            HashMap::new(),
            None
        );
        let count_result = sql_select(&db_pool, &select_query).await.unwrap();
        assert_eq!(count_result.rows.len(), 1);
        assert_eq!(count_result.rows[0][0], DbValue::Int(3));

        // Test transaction with mixed data types including NULL
        let mixed_transaction = QueryAndParamsList(
            "INSERT INTO test_users (id, name, active) VALUES (?, ?, ?)".into(),
            vec![
                vec![DbValue::Int(4), DbValue::String("David".into()), DbValue::Null],
                vec![DbValue::Int(5), DbValue::Null, DbValue::Bool(true)],
            ],
        );

        let result = sql_exec_transaction(&db_pool, &mixed_transaction).await;
        assert!(result.is_ok(), "Mixed type transaction should succeed: {:?}", result);

        // Verify the new rows
        let final_count = sql_select(&db_pool, &select_query).await.unwrap();
        assert_eq!(final_count.rows[0][0], DbValue::Int(5));
    }

    async fn test_sql_exec_transaction_rollback_with_db(db_pool: DbPool) {
        // Create test table
        let create_table = QueryAndParams(
            "CREATE TABLE rollback_test (id INTEGER UNIQUE, name TEXT)".into(),
            HashMap::new(),
            None
        );
        sql_exec(&db_pool, &create_table).await.unwrap();

        // Insert initial data
        let initial_insert = QueryAndParams(
            "INSERT INTO rollback_test (id, name) VALUES (:id, :name)".into(),
            [("id".to_string(), DbValue::Int(1)), ("name".to_string(), DbValue::String("Initial".into()))]
                .into_iter().collect(),
            None
        );
        sql_exec(&db_pool, &initial_insert).await.unwrap();

        // Test transaction that should fail due to unique constraint violation
        let failing_transaction = QueryAndParamsList(
            "INSERT INTO rollback_test (id, name) VALUES (?, ?)".into(),
            vec![
                vec![DbValue::Int(2), DbValue::String("Valid".into())],
                vec![DbValue::Int(1), DbValue::String("Duplicate".into())], // This should fail
                vec![DbValue::Int(3), DbValue::String("Never inserted".into())],
            ],
        );

        let result = sql_exec_transaction(&db_pool, &failing_transaction).await;
        assert!(result.is_err(), "Transaction with duplicate key should fail");

        // Verify rollback - only the initial row should exist
        let count_query = QueryAndParams(
            "SELECT COUNT(*) as count FROM rollback_test".into(),
            HashMap::new(),
            None
        );
        let count_result = sql_select(&db_pool, &count_query).await.unwrap();
        assert_eq!(count_result.rows[0][0], DbValue::Int(1), "Transaction should be rolled back");

        // Verify the content is still the initial data
        let select_all = QueryAndParams(
            "SELECT id, name FROM rollback_test".into(),
            HashMap::new(),
            None
        );
        let all_data = sql_select(&db_pool, &select_all).await.unwrap();
        assert_eq!(all_data.rows.len(), 1);
        assert_eq!(all_data.rows[0][0], DbValue::Int(1));
        assert_eq!(all_data.rows[0][1], DbValue::String("Initial".into()));
    }

    #[tokio::test]
    async fn test_sql_exec_transaction_sqlite() {
        // Test successful transaction
        let db_pool = DbPool::Sqlite(SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await
            .unwrap());

        test_sql_exec_transaction_with_db(db_pool).await;

        // Test rollback behavior with a new connection
        let db_pool2 = DbPool::Sqlite(SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await
            .unwrap());

        test_sql_exec_transaction_rollback_with_db(db_pool2).await;
    }

    #[tokio::test]
    async fn test_sql_exec_transaction_postgres() {
        if let Ok(db_url) = std::env::var("QXSQLD_POSTGRES_URL") {
            // Test successful transaction
            let db_pool = DbPool::Postgres(PgPoolOptions::new()
                .connect(&db_url)
                .await
                .unwrap());

            // Clean up any existing test tables
            let _ = match &db_pool {
                DbPool::Postgres(pool) => {
                    let drop1 = QueryAndParams("DROP TABLE IF EXISTS test_users".into(), HashMap::new(), None);
                    sql_exec_postgres(pool, &drop1).await.ok();
                },
                _ => panic!("not a postgres pool"),
            };

            test_sql_exec_transaction_with_db(db_pool).await;

            // Test rollback behavior with a new connection
            let db_pool2 = DbPool::Postgres(PgPoolOptions::new()
                .connect(&db_url)
                .await
                .unwrap());

            let _ = match &db_pool2 {
                DbPool::Postgres(pool) => {
                    let drop2 = QueryAndParams("DROP TABLE IF EXISTS rollback_test".into(), HashMap::new(), None);
                    sql_exec_postgres(pool, &drop2).await.ok();
                },
                _ => panic!("not a postgres pool"),
            };

            test_sql_exec_transaction_rollback_with_db(db_pool2).await;
        } else {
            warn!("Skipping postgres transaction test, QXSQLD_POSTGRES_URL not set");
        }
    }

    #[test]
    fn test_parse_sql_info_insert_basic() {
        let info = parse_sql_info("INSERT INTO users (name) VALUES ('John')").unwrap();
        assert_eq!(info.operation, SqlOperation::Insert);
        assert_eq!(info.table_name, "users");
        assert_eq!(info.is_returning_id, false);
    }

    #[test]
    fn test_parse_sql_info_insert_case_insensitive() {
        let info = parse_sql_info("insert into Products (id, name) values (1, 'Widget')").unwrap();
        assert_eq!(info.operation, SqlOperation::Insert);
        assert_eq!(info.table_name, "Products");
        assert_eq!(info.is_returning_id, false);

        let info = parse_sql_info("Insert Into Orders VALUES (100)").unwrap();
        assert_eq!(info.operation, SqlOperation::Insert);
        assert_eq!(info.table_name, "Orders");
        assert_eq!(info.is_returning_id, false);
    }

    #[test]
    fn test_parse_sql_info_update_basic() {
        let info = parse_sql_info("UPDATE users SET name = 'Jane' WHERE id = 1").unwrap();
        assert_eq!(info.operation, SqlOperation::Update);
        assert_eq!(info.table_name, "users");
        assert_eq!(info.is_returning_id, false);
    }

    #[test]
    fn test_parse_sql_info_update_case_insensitive() {
        let info = parse_sql_info("update Products set price = 100").unwrap();
        assert_eq!(info.operation, SqlOperation::Update);
        assert_eq!(info.table_name, "Products");
        assert_eq!(info.is_returning_id, false);

        let info = parse_sql_info("Update CUSTOMERS Set status='active'").unwrap();
        assert_eq!(info.operation, SqlOperation::Update);
        assert_eq!(info.table_name, "CUSTOMERS");
        assert_eq!(info.is_returning_id, false);
    }

    #[test]
    fn test_parse_sql_info_with_extra_whitespace() {
        let info = parse_sql_info("   INSERT   INTO   users   (name)   VALUES   ('John')   ").unwrap();
        assert_eq!(info.operation, SqlOperation::Insert);
        assert_eq!(info.table_name, "users");
        assert_eq!(info.is_returning_id, false);

        let info = parse_sql_info("\t\nUPDATE\t\nproducts\t\nSET price = 50").unwrap();
        assert_eq!(info.operation, SqlOperation::Update);
        assert_eq!(info.table_name, "products");
        assert_eq!(info.is_returning_id, false);
    }

    #[test]
    fn test_parse_sql_info_errors() {
        // Empty string
        assert!(parse_sql_info("").is_err());
        assert!(parse_sql_info("   ").is_err());

        // Unsupported operations
        assert_eq!(parse_sql_info("SELECT * FROM users").unwrap().operation, SqlOperation::Other("SELECT".to_string()));
        assert_eq!(parse_sql_info("CREATE TABLE users").unwrap().operation, SqlOperation::Other("CREATE".to_string()));

        // DELETE operations are supported
        assert!(parse_sql_info("DELETE FROM users").is_ok());

        // Invalid INSERT statements
        assert!(parse_sql_info("INSERT").is_err());
        assert!(parse_sql_info("INSERT users").is_err());
        assert!(parse_sql_info("INSERT FROM users").is_err()); // Should be INTO

        // Invalid UPDATE statements
        assert!(parse_sql_info("UPDATE").is_err());
        assert!(parse_sql_info("UPDATE SET name = 'John'").is_err()); // Missing table name
    }

    #[test]
    fn test_parse_sql_info_delete_basic() {
        let info = parse_sql_info("DELETE FROM users WHERE id = 1").unwrap();
        assert_eq!(info.operation, SqlOperation::Delete);
        assert_eq!(info.table_name, "users");
        assert_eq!(info.is_returning_id, false);
    }

    #[test]
    fn test_parse_sql_info_delete_case_insensitive() {
        // Test case insensitivity for DELETE operations
        let test_cases = vec![
            "delete from products where price > 100",
            "Delete From customers Where active = false",
            "DELETE FROM orders WHERE status = 'cancelled'",
        ];

        for sql in test_cases {
            let info = parse_sql_info(sql).unwrap();
            assert_eq!(info.operation, SqlOperation::Delete);
            assert!(!info.table_name.is_empty());
            assert_eq!(info.is_returning_id, false);
        }
    }

    #[test]
    fn test_parse_sql_info_table_names_with_special_chars() {
        // Table names with underscores
        let info = parse_sql_info("INSERT INTO user_profiles (data) VALUES ('test')").unwrap();
        assert_eq!(info.table_name, "user_profiles");
        assert_eq!(info.is_returning_id, false);

        // Table names with numbers
        let info = parse_sql_info("UPDATE table123 SET value = 1").unwrap();
        assert_eq!(info.table_name, "table123");
        assert_eq!(info.is_returning_id, false);

        // Quoted table names (common in some SQL dialects)
        let info = parse_sql_info("INSERT INTO `special-table` (id) VALUES (1)").unwrap();
        assert_eq!(info.table_name, "`special-table`");
        assert_eq!(info.is_returning_id, false);
    }

    #[test]
    fn test_parse_sql_info_complex_statements() {
        // Complex INSERT with subquery
        let complex_insert = "INSERT INTO target_table (col1, col2) SELECT a, b FROM source_table WHERE condition = 1";
        let info = parse_sql_info(complex_insert).unwrap();
        assert_eq!(info.operation, SqlOperation::Insert);
        assert_eq!(info.table_name, "target_table");
        assert_eq!(info.is_returning_id, false);

        // Complex UPDATE with joins (simplified parsing)
        let complex_update = "UPDATE orders SET status = 'shipped' WHERE customer_id IN (SELECT id FROM customers WHERE active = true)";
        let info = parse_sql_info(complex_update).unwrap();
        assert_eq!(info.operation, SqlOperation::Update);
        assert_eq!(info.table_name, "orders");
        assert_eq!(info.is_returning_id, false);
    }

    #[test]
    fn test_parse_sql_info_edge_cases() {
        // Minimal valid statements
        let info = parse_sql_info("INSERT INTO t VALUES(1)").unwrap();
        assert_eq!(info.table_name, "t");
        assert_eq!(info.is_returning_id, false);

        let info = parse_sql_info("UPDATE t SET x=1").unwrap();
        assert_eq!(info.table_name, "t");
        assert_eq!(info.is_returning_id, false);

        // Mixed case keywords
        let info = parse_sql_info("InSeRt InTo MixedCase VALUES (1)").unwrap();
        assert_eq!(info.operation, SqlOperation::Insert);
        assert_eq!(info.table_name, "MixedCase");
        assert_eq!(info.is_returning_id, false);
    }

    #[test]
    fn test_parse_sql_info_usage_examples() {
        // Test the examples from the function documentation
        let info = parse_sql_info("INSERT INTO users (name) VALUES ('John')").unwrap();
        assert_eq!(info.operation, SqlOperation::Insert);
        assert_eq!(info.table_name, "users");
        assert_eq!(info.is_returning_id, false);

        let info = parse_sql_info("update Products set price = 100").unwrap();
        assert_eq!(info.operation, SqlOperation::Update);
        assert_eq!(info.table_name, "Products");
        assert_eq!(info.is_returning_id, false);

        // Test practical usage
        let sql_statements = vec![
            "INSERT INTO orders (customer_id, total) VALUES (1, 99.99)",
            "UPDATE inventory SET quantity = quantity - 1 WHERE product_id = 123",
            "DELETE FROM expired_sessions WHERE created_at < NOW() - INTERVAL '1 day'",
            "insert into logs (message, timestamp) values ('test', now())",
            "delete from temp_data where processed = true",
        ];

        for sql in sql_statements {
            let result = parse_sql_info(sql);
            assert!(result.is_ok(), "Failed to parse: {}", sql);

            let info = result.unwrap();
            match info.operation {
                SqlOperation::Insert => {
                    assert!(sql.to_uppercase().starts_with("INSERT"));
                },
                SqlOperation::Update => {
                    assert!(sql.to_uppercase().starts_with("UPDATE"));
                },
                SqlOperation::Delete => {
                    assert!(sql.to_uppercase().starts_with("DELETE"));
                },
                SqlOperation::Other(op) => {
                    panic!("Unexpected operation {}", op);
                },
            }
            assert!(!info.table_name.is_empty());
        }
    }

    #[test]
    fn test_parse_sql_info_returning_id() {
        // Test INSERT with RETURNING id clause
        let info = parse_sql_info("INSERT INTO users (name) VALUES ('John') RETURNING id").unwrap();
        assert_eq!(info.operation, SqlOperation::Insert);
        assert_eq!(info.table_name, "users");
        assert_eq!(info.is_returning_id, true);

        // Test case insensitive RETURNING ID
        let info = parse_sql_info("insert into products (name) values ('Widget') returning id").unwrap();
        assert_eq!(info.operation, SqlOperation::Insert);
        assert_eq!(info.table_name, "products");
        assert_eq!(info.is_returning_id, true);

        // Test mixed case
        let info = parse_sql_info("INSERT INTO orders (total) VALUES (99.99) Returning Id").unwrap();
        assert_eq!(info.operation, SqlOperation::Insert);
        assert_eq!(info.table_name, "orders");
        assert_eq!(info.is_returning_id, true);

        // Test without RETURNING id
        let info = parse_sql_info("INSERT INTO logs (message) VALUES ('test')").unwrap();
        assert_eq!(info.operation, SqlOperation::Insert);
        assert_eq!(info.table_name, "logs");
        assert_eq!(info.is_returning_id, false);

        // Test UPDATE (should never have returning_id = true)
        let info = parse_sql_info("UPDATE users SET name = 'Jane' RETURNING id").unwrap();
        assert_eq!(info.operation, SqlOperation::Update);
        assert_eq!(info.table_name, "users");
        assert_eq!(info.is_returning_id, false);

        // Test INSERT with RETURNING other column (should be false)
        let info = parse_sql_info("INSERT INTO users (name) VALUES ('John') RETURNING name").unwrap();
        assert_eq!(info.operation, SqlOperation::Insert);
        assert_eq!(info.table_name, "users");
        assert_eq!(info.is_returning_id, false);

        // Test INSERT with complex RETURNING clause (should be false)
        let info = parse_sql_info("INSERT INTO users (name) VALUES ('John') RETURNING id, name").unwrap();
        assert_eq!(info.operation, SqlOperation::Insert);
        assert_eq!(info.table_name, "users");
        assert_eq!(info.is_returning_id, false);

        // Test INSERT with RETURNING id and extra whitespace
        let info = parse_sql_info("INSERT INTO users (name) VALUES ('John')   RETURNING   id  ").unwrap();
        assert_eq!(info.operation, SqlOperation::Insert);
        assert_eq!(info.table_name, "users");
        assert_eq!(info.is_returning_id, true);

        // Test INSERT with RETURNING ID (uppercase)
        let info = parse_sql_info("INSERT INTO users (name) VALUES ('John') RETURNING ID").unwrap();
        assert_eq!(info.operation, SqlOperation::Insert);
        assert_eq!(info.table_name, "users");
        assert_eq!(info.is_returning_id, true);

        // Test INSERT with partial match (should be false)
        let info = parse_sql_info("INSERT INTO users (name) VALUES ('John') RETURNING identifier").unwrap();
        assert_eq!(info.operation, SqlOperation::Insert);
        assert_eq!(info.table_name, "users");
        assert_eq!(info.is_returning_id, false);

        // Test INSERT that contains "returning id" but doesn't end with it
        let info = parse_sql_info("INSERT INTO users (returning_id) VALUES (1) WHERE x = 'returning id'").unwrap();
        assert_eq!(info.operation, SqlOperation::Insert);
        assert_eq!(info.table_name, "users");
        assert_eq!(info.is_returning_id, false);
    }

    #[test]
    fn test_parse_sql_info_returning_id_practical_usage() {
            // Practical example: routing INSERT queries based on returning_id flag
            let queries = vec![
                ("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')", false),
                ("INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com') RETURNING id", true),
                ("INSERT INTO orders (user_id, total) VALUES (1, 99.99) returning id", true),
                ("UPDATE users SET last_login = NOW() WHERE id = 1", false),
                ("INSERT INTO logs (message) VALUES ('System started') RETURNING ID", true),
            ];

            for (sql, expected_returning_id) in queries {
                let info = parse_sql_info(sql).unwrap();
                assert_eq!(info.is_returning_id, expected_returning_id, "Failed for SQL: {}", sql);

                // Demonstrate practical usage
                match info {
                    SqlInfo { operation: SqlOperation::Insert, is_returning_id: true, table_name } => {
                        // This query expects an ID to be returned - route to special handler
                        println!("INSERT with RETURNING id on table '{}' - will return generated ID", table_name);
                    },
                    SqlInfo { operation: SqlOperation::Insert, is_returning_id: false, table_name } => {
                        // Regular INSERT - route to standard handler
                        println!("Regular INSERT on table '{}' - no ID expected", table_name);
                    },
                    SqlInfo { operation: SqlOperation::Update, table_name, .. } => {
                        // UPDATE operations
                        println!("UPDATE on table '{}' - returning_id is always false for updates", table_name);
                    },
                    SqlInfo { operation: SqlOperation::Delete, table_name, .. } => {
                        // DELETE operations
                        println!("DELETE on table '{}' - returning_id is always false for deletes", table_name);
                    },
                    SqlInfo { operation: SqlOperation::Other(_), table_name, .. } => {
                        // Other operations
                        println!("Other operation on table '{}'", table_name);
                    },
                }
            }
        }

    #[test]
    fn test_parse_sql_info_api_demonstration() {
            // Demonstrate the public API usage

            // Basic usage
            let sql = "INSERT INTO customers (name, email) VALUES ('Alice', 'alice@example.com')";
            match parse_sql_info(sql) {
                Ok(SqlInfo { operation: SqlOperation::Insert, table_name, is_returning_id: returning_id }) => {
                    println!("INSERT operation on table: {}", table_name);
                    assert_eq!(table_name, "customers");
                    assert_eq!(returning_id, false);
                },
                Ok(SqlInfo { operation: SqlOperation::Update, table_name, is_returning_id: returning_id }) => {
                    println!("UPDATE operation on table: {}", table_name);
                    assert_eq!(returning_id, false);
                },
                Ok(SqlInfo { operation: SqlOperation::Delete, table_name, is_returning_id: returning_id }) => {
                    println!("DELETE operation on table: {}", table_name);
                    assert_eq!(returning_id, false);
                },
                Ok(SqlInfo { operation: SqlOperation::Other(_), .. }) => {
                    panic!("Unexpected operation type");
                },
                Err(e) => panic!("Parse error: {}", e),
            }

            // Pattern matching usage
            let statements = vec![
                "INSERT INTO logs (message) VALUES ('System started')",
                "UPDATE users SET last_login = NOW() WHERE id = 123",
                "DELETE FROM old_records WHERE created_at < '2023-01-01'",
                "insert into products (name, price) values ('Widget', 9.99)",
            ];

            for stmt in statements {
                if let Ok(info) = parse_sql_info(stmt) {
                    match info.operation {
                        SqlOperation::Insert => {
                            println!("Will insert into table: {}", info.table_name);
                        },
                        SqlOperation::Update => {
                            println!("Will update table: {}", info.table_name);
                        },
                        SqlOperation::Delete => {
                            println!("Will delete from table: {}", info.table_name);
                        },
                        SqlOperation::Other(_) => {
                            println!("Other operation on table: {}", info.table_name);
                        },
                    }
                }
            }
        }
}
