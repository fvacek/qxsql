use std::backtrace::Backtrace;

use chrono::Utc;
use log::error;
use serde::{Deserialize, Serialize};
use shvproto::{from_rpcvalue, RpcValue};
use sqlx::prelude::FromRow;
use sqlx::{Pool, Postgres, Sqlite, sqlite::SqliteRow, postgres::PgRow};
use sqlx::{Column, Row, TypeInfo, ValueRef, postgres::PgPool, SqlitePool};
use anyhow::anyhow;

pub enum DbPool {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

#[derive(Debug,Serialize,Deserialize, PartialEq)]
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
    pub params: Vec<DbValue>,
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

pub async fn sql_exec_sqlite(db_pool: &Pool<Sqlite>, query: &QueryAndParams) -> anyhow::Result<ExecResult> {
    let sql = &query.query;
    let mut q = sqlx::query(sql);
    for val in &query.params {
        match val {
            DbValue::String(s) => q = q.bind(s),
            DbValue::Int(i) => q = q.bind(i),
            DbValue::Bool(b) => q = q.bind(b),
            DbValue::DateTime(dt) => q = q.bind(dt.to_rfc3339()),
            DbValue::Null => q = q.bind(None::<&str>),
        }
    };

    let result = q.execute(db_pool).await?;
    Ok(ExecResult { rows_affected: result.rows_affected() as i64 })
}

pub async fn sql_exec_postgres(db_pool: &Pool<Postgres>, query: &QueryAndParams) -> anyhow::Result<ExecResult> {
    let sql = postgres_query_positional_args_from_sqlite(&query.query);
    let mut q = sqlx::query(&sql);
    for val in &query.params {
        match val {
            DbValue::String(s) => q = q.bind(s),
            DbValue::Int(i) => q = q.bind(i),
            DbValue::Bool(b) => q = q.bind(b),
            DbValue::DateTime(dt) => q = q.bind(dt.to_rfc3339()),
            DbValue::Null => q = q.bind(None::<&str>),
        }
    };

    let result = q.execute(db_pool).await?;
    Ok(ExecResult { rows_affected: result.rows_affected() as i64 })
}

pub async fn sql_select_sqlite(db_pool: &Pool<Sqlite>, query: &QueryAndParams) -> anyhow::Result<SelectResult> {
    let sql = &query.query;
    let mut q = sqlx::query(sql);
    for val in &query.params {
        match val {
            DbValue::String(s) => q = q.bind(s),
            DbValue::Int(i) => q = q.bind(i),
            DbValue::Bool(b) => q = q.bind(b),
            DbValue::DateTime(dt) => q = q.bind(dt.to_rfc3339()),
            DbValue::Null => q = q.bind(None::<&str>),
        }
    };

    let rows = q.fetch_all(db_pool).await?;
    let mut result: SelectResult = Default::default();
    for (ix, rowx) in rows.iter().enumerate() {
        let cols = rowx.columns();
        let mut row: Vec<DbValue> = Vec::with_capacity(cols.len());
        for (i, col) in cols.iter().enumerate() {
            let col_name = col.name();
            if ix == 0 {
                result.fields.push(DbField { name: col_name.to_string() });
            }
            let val = db_value_from_sqlite_row(rowx, i)?;
            row.push(val);
        }
        result.rows.push(row);
    }
    Ok(result)
}

pub(crate) async fn sql_select_postgres(db_pool: &Pool<Postgres>, query: &QueryAndParams) -> anyhow::Result<SelectResult> {
    let sql = postgres_query_positional_args_from_sqlite(&query.query);
    let mut q = sqlx::query(&sql);
    for val in &query.params {
        match val {
            DbValue::String(s) => q = q.bind(s),
            DbValue::Int(i) => q = q.bind(i),
            DbValue::Bool(b) => q = q.bind(b),
            DbValue::DateTime(dt) => q = q.bind(dt.to_rfc3339()),
            DbValue::Null => q = q.bind(None::<&str>),
        }
    };

    let rows = q.fetch_all(db_pool).await?;
    let mut result: SelectResult = Default::default();
    for (ix, rowx) in rows.iter().enumerate() {
        let cols = rowx.columns();
        let mut row: Vec<DbValue> = Vec::with_capacity(cols.len());
        for (i, col) in cols.iter().enumerate() {
            let col_name = col.name();
            if ix == 0 {
                result.fields.push(DbField { name: col_name.to_string() });
            }
            let val = db_value_from_postgres_row(rowx, i)?;
            row.push(val);
        }
        result.rows.push(row);
    }
    Ok(result)
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

fn postgres_query_positional_args_from_sqlite(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut param_counter = 1;
    let mut in_single_quote = false;

    for c in input.chars() {
        match c {
            '\'' => {
                output.push(c);
                in_single_quote = !in_single_quote;
            }
            '?' if !in_single_quote => {
                output.push('$');
                output.push_str(&param_counter.to_string());
                param_counter += 1;
            }
            _ => output.push(c),
        }
    }
    output
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
            params: vec![],
        };
        let res = sql_exec(&db_pool, &qp).await;
        res.unwrap();

        let qp = QueryAndParams {
            query: "INSERT INTO users (id, name) VALUES ($1, $2)".into(),
            params:vec![1.into(), "Jane Doe".into()],
        };
        let res = sql_exec(&db_pool, &qp).await;
        res.unwrap();

        let qp = QueryAndParams {
            query: "SELECT * FROM users".into(),
            params: vec![],
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
                        params: vec![],
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
