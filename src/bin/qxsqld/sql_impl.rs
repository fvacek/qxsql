use std::backtrace::Backtrace;

use async_trait::async_trait;
use log::{error};

use sqlx::{Pool, Postgres, Sqlite, sqlite::SqliteRow, postgres::PgRow};
use sqlx::{Column, Row, TypeInfo, ValueRef, postgres::PgPool, SqlitePool};
use anyhow::{anyhow};

use crate::appstate::{QxSharedAppState};
use qxsqld::sql::{DbField, DbValue, ExecResult, QueryAndParamsList, Record, SelectResult};
use qxsqld::sql_utils::{self, postgres_query_positional_args_from_sqlite};

pub enum DbPool {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

pub async fn sql_exec_transaction(db: &DbPool, query: &QueryAndParamsList) -> anyhow::Result<()> {
    match db {
        DbPool::Sqlite(pool) => sql_exec_transaction_sqlite(pool, query).await,
        DbPool::Postgres(pool) => sql_exec_transaction_postgres(pool, query).await,
    }
}

fn prepare_sql_with_query_params(query: &str, params: &Record, repl_char: char) -> String {
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























macro_rules! sql_exec_impl {
    ($db_pool:expr, $query:expr, $params:expr, $repl_char:expr) => {{
        let sql = prepare_sql_with_query_params($query, $params, $repl_char);
        let q = sqlx::query(&sql);
        let q = bind_db_values!(q, $params.values());
        q.execute($db_pool).await?
    }};
}

async fn sql_exec_sqlite(db_pool: &Pool<Sqlite>, query: &str, params: &Record) -> anyhow::Result<ExecResult> {
    let result = sql_exec_impl!(db_pool, query, params, '?');
    Ok(ExecResult { rows_affected: result.rows_affected() as i64, insert_id: Some(result.last_insert_rowid()) })
}

async fn sql_exec_postgres(db_pool: &Pool<Postgres>, query: &str, params: &Record) -> anyhow::Result<ExecResult> {
    let result = sql_exec_impl!(db_pool, query, params, '?');
    Ok(ExecResult { rows_affected: result.rows_affected() as i64, insert_id: None })
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

async fn sql_select_sqlite(db_pool: &Pool<Sqlite>, query: &str, params: &Record) -> anyhow::Result<SelectResult> {
    let sql = prepare_sql_with_query_params(query, params, '?');
    let q = sqlx::query(&sql);
    let q = bind_db_values!(q, params.values());
    let rows = q.fetch_all(db_pool).await?;
    process_rows(&rows, db_value_from_sqlite_row)
}

async fn sql_select_postgres(db_pool: &Pool<Postgres>, query: &str, params: &Record) -> anyhow::Result<SelectResult> {
    let sql = prepare_sql_with_query_params(query, params, '$');
    let q = sqlx::query(&sql);
    let q = bind_db_values!(q, params.values());
    let rows = q.fetch_all(db_pool).await?;
    process_rows(&rows, db_value_from_postgres_row)
}

fn sqlx_to_anyhow(err: Box<dyn std::error::Error + Send + Sync>) -> anyhow::Error {
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

enum ListId {
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

pub struct QxSql(pub QxSharedAppState);
impl QxSql {
    async fn list_records_impl(
        &self, table: &str, fields: Option<&[&str]>,
        id: ListId,
        limit: Option<i64>,
    ) -> anyhow::Result<Vec<Record>> {
        let fields_str = fields.unwrap_or(&["*"]).join(", ");
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
        let result = qxsqld::sql::SqlProvider::query(self, &qs, None).await?;
        Ok((0..result.rows.len())
            .filter_map(|i| result.record(i))
            .collect())
    }
}

#[async_trait]
impl qxsqld::sql::SqlProvider for QxSql {
    async fn query(&self, query: &str, params: Option<&Record>) -> anyhow::Result<SelectResult> {
        let db = self.0.read().await;
        let empty_params = Record::default();
        let params = params.unwrap_or(&empty_params);
        match &*db {
            DbPool::Sqlite(pool) => sql_select_sqlite(&pool, query, params).await,
            DbPool::Postgres(pool) => sql_select_postgres(&pool, query, params).await,
        }
    }
    async fn exec(&self, query: &str, params: Option<&Record>) -> anyhow::Result<ExecResult> {
        let db = self.0.read().await;
        let empty_params = Record::default();
        let params = params.unwrap_or(&empty_params);
        match &*db {
            DbPool::Sqlite(pool) => sql_exec_sqlite(&pool, query, params).await,
            DbPool::Postgres(pool) => sql_exec_postgres(&pool, query, params).await,
        }
    }

    async fn list_records(&self, table: &str, fields: Option<&[&str]>, ids_greater_than: Option<i64>, limit: Option<i64>) -> anyhow::Result<Vec<Record>> {
        self.list_records_impl(table, fields, ListId::new_greater_than(ids_greater_than), limit).await
    }

    async fn create_record(&self, table: &str, record: &Record) -> anyhow::Result<i64> {
        let keys = record.keys().map(|k| k.to_string()).collect::<Vec<_>>().join(", ");
        let vals = record.keys().map(|k| format!(":{k}")).collect::<Vec<_>>().join(", ");
        let query = format!("INSERT INTO {table} ({keys}) VALUES ({vals}) RETURNING id");
        self.query(&query, None).await.and_then(|table| {
            if let DbValue::Int(id) = table.rows[0][0] {
                Ok(id)
            } else {
                Err(anyhow!("Insert should return int64 value type"))
            }
        })
    }
    async fn read_record(&self, table: &str, id: i64) -> anyhow::Result<Option<Record>> {
        let records = self.list_records_impl(table, None, ListId::new_is_equal(Some(id)), None).await?;
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
