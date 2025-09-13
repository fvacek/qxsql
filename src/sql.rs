use std::backtrace::Backtrace;
use std::collections::HashMap;

use chrono::Utc;
use log::error;
use serde::{Deserialize, Serialize};
use shvproto::{from_rpcvalue, RpcValue};
use sqlx::prelude::FromRow;
use sqlx::{Pool, Postgres, Sqlite, sqlite::SqliteRow, postgres::PgRow};
use sqlx::{Column, Row, TypeInfo, ValueRef, postgres::PgPool, SqlitePool};
use anyhow::anyhow;

use crate::sql_replace;

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

#[derive(Debug,Serialize,Deserialize)]
pub struct QueryAndParams {
    pub query: String,
    pub params: HashMap<String, DbValue>,
}

impl TryFrom<&RpcValue> for QueryAndParams {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        from_rpcvalue(value).map_err(|e| e.to_string())
    }
}

#[derive(Debug,Serialize,Deserialize, Default)]
pub struct ExecResult {
    pub rows_affected: i64,
}
#[derive(Debug,Serialize,Deserialize, Default, PartialEq)]
pub struct SelectResult {
    pub fields: Vec<DbField>,
    pub rows: Vec<Vec<DbValue>>,
}
#[derive(Debug,Serialize,Deserialize, PartialEq)]
pub struct DbField {
    pub name: String,
}

pub async fn sql_exec(db: &DbPool, query: &QueryAndParams) -> anyhow::Result<ExecResult> {
    match db {
        DbPool::Sqlite(pool) => sql_exec_sqlite(pool, query).await,
        DbPool::Postgres(pool) => sql_exec_postgres(pool, query).await,
    }
}

pub async fn sql_select(db: &DbPool, query: &QueryAndParams) -> anyhow::Result<SelectResult> {
    match db {
        DbPool::Sqlite(pool) => sql_select_sqlite(pool, query).await,
        DbPool::Postgres(pool) => sql_select_postgres(pool, query).await,
    }
}

fn prepare_sql_with_params(query: &QueryAndParams) -> String {
    sql_replace::replace_params(&query.query, &query.params)
}

pub async fn sql_exec_sqlite(db_pool: &Pool<Sqlite>, query: &QueryAndParams) -> anyhow::Result<ExecResult> {
    let sql = prepare_sql_with_params(query);
    let q = sqlx::query(&sql);
    let result = q.execute(db_pool).await?;
    Ok(ExecResult { rows_affected: result.rows_affected() as i64 })
}

pub async fn sql_exec_postgres(db_pool: &Pool<Postgres>, query: &QueryAndParams) -> anyhow::Result<ExecResult> {
    let sql = prepare_sql_with_params(query);
    let q = sqlx::query(&sql);
    let result = q.execute(db_pool).await?;
    Ok(ExecResult { rows_affected: result.rows_affected() as i64 })
}

fn process_rows<R>(rows: &[R], value_extractor: impl Fn(&R, usize) -> anyhow::Result<DbValue>) -> anyhow::Result<SelectResult>
where
    R: Row,
{
    let mut result: SelectResult = Default::default();
    for (ix, rowx) in rows.iter().enumerate() {
        let cols = rowx.columns();
        let mut row: Vec<DbValue> = Default::default();
        row.reserve(cols.len());
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

pub async fn sql_select_sqlite(db_pool: &Pool<Sqlite>, query: &QueryAndParams) -> anyhow::Result<SelectResult> {
    let sql = prepare_sql_with_params(query);
    let q = sqlx::query(&sql);
    let rows = q.fetch_all(db_pool).await?;
    process_rows(&rows, db_value_from_sqlite_row)
}

pub(crate) async fn sql_select_postgres(db_pool: &Pool<Postgres>, query: &QueryAndParams) -> anyhow::Result<SelectResult> {
    let sql = prepare_sql_with_params(query);
    let q = sqlx::query(&sql);
    let rows = q.fetch_all(db_pool).await?;
    process_rows(&rows, db_value_from_postgres_row)
}

fn sqlx_to_anyhow(err: Box<dyn std::error::Error + Send + Sync>) -> anyhow::Error {
    error!("SQL Error: {err}\nbacktrace: {}", Backtrace::capture());
    anyhow!("SQL error: {}", err)
}

pub(crate) fn db_value_from_sqlite_row(row: &SqliteRow, index: usize) -> anyhow::Result<DbValue> {
    let raw_val = row.try_get_raw(index)?;
    let type_name = raw_val.type_info().name().to_uppercase();
    if raw_val.is_null() {
        return Ok(DbValue::Null);
    }
    let val = if type_name.contains("TEXT") || type_name.contains("STRING") {
        DbValue::String(<String as sqlx::decode::Decode<Sqlite>>::decode(raw_val).map_err(sqlx_to_anyhow)?)
    } else if type_name.contains("INT") {
        DbValue::Int(<i64 as sqlx::decode::Decode<Sqlite>>::decode(raw_val).map_err(sqlx_to_anyhow)?)
    } else if type_name.contains("BOOL") {
        DbValue::Bool(<bool as sqlx::decode::Decode<Sqlite>>::decode(raw_val).map_err(sqlx_to_anyhow)?)
    } else {
        anyhow::bail!("Unsupported type: {}", type_name);
    };
    Ok(val)
}
pub(crate) fn db_value_from_postgres_row(row: &PgRow, index: usize) -> anyhow::Result<DbValue> {
    let raw_val = row.try_get_raw(index)?;
    let type_name = raw_val.type_info().name().to_uppercase();
    if raw_val.is_null() {
        return Ok(DbValue::Null);
    }
    let val = if type_name.contains("TEXT") || type_name.contains("STRING") || type_name.contains("VARCHAR") {
        DbValue::String(<String as sqlx::decode::Decode<Postgres>>::decode(raw_val).map_err(sqlx_to_anyhow)?)
    } else if type_name.contains("INT") {
        DbValue::Int(<i64 as sqlx::decode::Decode<Postgres>>::decode(raw_val).map_err(sqlx_to_anyhow)?)
    } else if type_name.contains("BOOL") {
        DbValue::Bool(<bool as sqlx::decode::Decode<Postgres>>::decode(raw_val).map_err(sqlx_to_anyhow)?)
    } else {
        anyhow::bail!("Unsupported type: {}", type_name);
    };
    Ok(val)
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
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::postgres::PgPoolOptions;
    use log::warn;

    async fn test_sql_select_with_db(db_pool: DbPool) {
        let qp = QueryAndParams {
            query: "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)".into(),
            params: HashMap::new(),
        };
        let res = sql_exec(&db_pool, &qp).await;
        res.unwrap();

        let qp = QueryAndParams {
            query: "INSERT INTO users (id, name) VALUES (:id, :name)".into(),
            params: [("id".to_string(), 1.into()), ("name".to_string(), "Jane Doe".into())].into_iter().collect(),
        };
        let res = sql_exec(&db_pool, &qp).await;
        res.unwrap();

        let qp = QueryAndParams {
            query: "SELECT * FROM users".into(),
            params: HashMap::new(),
        };
        let result = sql_select(&db_pool, &qp).await;
        let expected = SelectResult {
            fields: vec![DbField { name: "id".to_string() }, DbField { name: "name".to_string() }],
            rows: vec![vec![1.into(), "Jane Doe".into()]],
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
                    let qp = QueryAndParams {
                        query: "DROP TABLE IF EXISTS users".into(),
                        params: HashMap::new(),
                    };
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
}
