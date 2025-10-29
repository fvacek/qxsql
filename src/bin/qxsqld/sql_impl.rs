use std::backtrace::Backtrace;
use std::collections::HashMap;

use async_trait::async_trait;
use log::error;

use sqlx::{Pool, Postgres, Sqlite, sqlite::SqliteRow, postgres::PgRow};
use sqlx::{Column, Row, TypeInfo, ValueRef, postgres::PgPool, SqlitePool};
use anyhow::{anyhow, bail};

use crate::appstate::{QxSharedAppState};
use qxsqld::sql::{DbField, DbValue, ExecResult, QueryAndParams, QueryAndParamsList, SelectResult, SqlInfo};
use qxsqld::sql_utils::{self, postgres_query_positional_args_from_sqlite};

pub enum DbPool {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

pub async fn sql_select(db: &DbPool, query: &QueryAndParams) -> anyhow::Result<SelectResult> {
    match db {
        DbPool::Sqlite(pool) => sql_select_sqlite(pool, query).await,
        DbPool::Postgres(pool) => sql_select_postgres(pool, query).await,
    }
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
    prepare_sql_with_query_params(query.query(), query.params(), repl_char)
}

fn prepare_sql_with_query_params(query: &str, params: &HashMap<String, DbValue>, repl_char: char) -> String {
    let keys = params.keys().map(|s| s.as_str()).collect::<Vec<_>>();
    sql_utils::replace_named_with_positional_params(query, &keys, repl_char)
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
                DbValue::DateTime(dt) => q.bind(dt),
                DbValue::Null => q.bind(None::<&str>),
            };
        }
        q
    }};
}

fn create_rec_update_query(table: &str, id: i64, record: &HashMap<String, DbValue>) -> String {
    let keys = record.keys().map(|k| format!("{k} = :{k}")).collect::<Vec<_>>().join(", ");
    format!("UPDATE {table} SET {keys} WHERE {table}.id={id}")
}

async fn sql_rec_update_sqlite(db_pool: &Pool<Sqlite>, table: &str, id: i64, record: &HashMap<String, DbValue>) -> anyhow::Result<bool> {
    let sql = create_rec_update_query(table, id, record);
    let q = sqlx::query(&sql);
    let q = bind_db_values!(q, record.values());
    let result = q.execute(db_pool).await?;
    let rows_affected = result.rows_affected() as i64;
    Ok (rows_affected == 1)
}

async fn sql_rec_update_postgres(db_pool: &Pool<Postgres>, table: &str, id: i64, record: &HashMap<String, DbValue>) -> anyhow::Result<bool> {
    let sql = create_rec_update_query(table, id, record);
    let sql = prepare_sql_with_query_params(&sql, &record, '$');
    let q = sqlx::query(&sql);
    let q = bind_db_values!(q, record.values());
    let result = q.execute(db_pool).await?;
    let rows_affected = result.rows_affected() as i64;
    Ok (rows_affected == 1)
}

fn create_rec_create_query(table: &str, record: &HashMap<String, DbValue>) -> String {
    let keys = record.keys().map(|k| k.to_string()).collect::<Vec<_>>().join(", ");
    let vals = record.keys().map(|k| format!(":{k}")).collect::<Vec<_>>().join(", ");
    format!("INSERT INTO {table} ({keys}) VALUES ({vals}) RETURNING id")
}

async fn sql_rec_create_sqlite(db_pool: &Pool<Sqlite>, table: &str, record: &HashMap<String, DbValue>) -> anyhow::Result<i64> {
    let sql = create_rec_create_query(table, &record);
    let q = sqlx::query(&sql);
    let q = bind_db_values!(q, record.values());
    let row = q.fetch_one(db_pool).await?;
    let insert_id = row.get(0);
    Ok(insert_id)
}

async fn sql_rec_create_postgres(db_pool: &Pool<Postgres>, table: &str, record: &HashMap<String, DbValue>) -> anyhow::Result<i64> {
    let sql = create_rec_create_query(table, &record);
    let sql = prepare_sql_with_query_params(&sql, &record, '$');
    let q = sqlx::query(&sql);
    let q = bind_db_values!(q, record.values());
    let row = q.fetch_one(db_pool).await?;
    let insert_id: i64 = row.try_get(0).unwrap_or_else(|_| {
        let id_i32: i32 = row.get(0);
        id_i32 as i64
    });
    Ok(insert_id)
}

async fn sql_rec_read_sqlite(db_pool: &Pool<Sqlite>, table: &str, id: i64) -> anyhow::Result<Option<HashMap<String, DbValue>>> {
    let sql = format!("SELECT * FROM {table} WHERE id = {id}");
    let qp = QueryAndParams(sql, Default::default(), None);
    let result = sql_select_sqlite(db_pool, &qp).await?;
    Ok(result.record(0))
}

async fn sql_rec_read_postgres(db_pool: &Pool<Postgres>, table: &str, id: i64) -> anyhow::Result<Option<HashMap<String, DbValue>>> {
    let sql = format!("SELECT * FROM {table} WHERE id = {id}");
    let qp = QueryAndParams(sql, Default::default(), None);
    let result = sql_select_postgres(db_pool, &qp).await?;
    Ok(result.record(0))
}

fn create_rec_delete_query(table: &str, id: i64) -> String {
    format!("DELETE FROM {table} WHERE id = {id}")
}

async fn sql_rec_delete_sqlite(db_pool: &Pool<Sqlite>, table: &str, id: i64) -> anyhow::Result<bool> {
    let sql = create_rec_delete_query(table, id);
    let q = sqlx::query(&sql);
    let result = q.execute(db_pool).await?;
    Ok(result.rows_affected() == 1)
}

async fn sql_rec_delete_postgres(db_pool: &Pool<Postgres>, table: &str, id: i64) -> anyhow::Result<bool> {
    let sql = create_rec_delete_query(table, id);
    let q = sqlx::query(&sql);
    let result = q.execute(db_pool).await?;
    Ok(result.rows_affected() == 1)
}

// Common logic for SQL execution
fn parse_sql_info(query: &QueryAndParams) -> anyhow::Result<SqlInfo> {
    match qxsqld::sql_utils::parse_sql_info(query.query()) {
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
    type_name.contains("NAME") ||
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
        Ok(DbValue::String(<String as sqlx::decode::Decode<Sqlite>>::decode(raw_val).map_err(sqlx_to_anyhow)?))
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
        Ok(DbValue::String(s))
    } else if type_name.contains("INT") {
        Ok(DbValue::Int(<i64 as sqlx::decode::Decode<Postgres>>::decode(raw_val).map_err(sqlx_to_anyhow)?))
    } else if type_name.contains("TIMESTAMP") {
        let dt = <chrono::DateTime<chrono::FixedOffset> as sqlx::decode::Decode<Postgres>>::decode(raw_val).map_err(sqlx_to_anyhow)?;
        Ok(DbValue::DateTime(dt))
    } else if type_name.contains("BOOL") {
        Ok(DbValue::Bool(<bool as sqlx::decode::Decode<Postgres>>::decode(raw_val).map_err(sqlx_to_anyhow)?))
    } else {
        anyhow::bail!("Unsupported type: {}", type_name);
    }
}

pub struct QxSql(pub QxSharedAppState);

#[async_trait]
impl qxsqld::SqlProvider for QxSql {
    async fn create_record(&self, table: &str, record: &HashMap<String, DbValue>) -> anyhow::Result<i64> {
        let db = self.0.read().await;
        match &*db {
            DbPool::Sqlite(pool) => sql_rec_create_sqlite(pool, table, record).await,
            DbPool::Postgres(pool) => sql_rec_create_postgres(pool, table, record).await,
        }
    }
    async fn read_record(&self, table: &str, id: i64) -> anyhow::Result<Option<HashMap<String, DbValue>>> {
        let db = self.0.read().await;
        match &*db {
            DbPool::Sqlite(pool) => sql_rec_read_sqlite(pool, table, id).await,
            DbPool::Postgres(pool) => sql_rec_read_postgres(pool, table, id).await,
        }
    }
    async fn update_record(&self, table: &str, id: i64, record: &HashMap<String, DbValue>) -> anyhow::Result<bool> {
        let db = self.0.read().await;
        match &*db {
            DbPool::Sqlite(pool) => sql_rec_update_sqlite(pool, table, id, record).await,
            DbPool::Postgres(pool) => sql_rec_update_postgres(pool, table, id, record).await,
        }
    }
    async fn delete_record(&self, table: &str, id: i64) -> anyhow::Result<bool> {
        let db = self.0.read().await;
        match &*db {
            DbPool::Sqlite(pool) => sql_rec_delete_sqlite(pool, table, id).await,
            DbPool::Postgres(pool) => sql_rec_delete_postgres(pool, table, id).await,
        }
    }
}
