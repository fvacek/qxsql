use serde::{Deserialize, Serialize};
use shvproto::{List, Map, RpcValue};
use sqlx::prelude::FromRow;
use sqlx::{Pool, Postgres, Sqlite, sqlite::SqliteRow, postgres::PgRow};
use sqlx::{Column, Row, TypeInfo, ValueRef, postgres::PgPool, SqlitePool};
use regex::Regex;

pub enum DbPool {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

pub async fn sql_exec(db: &DbPool, query: &Vec<RpcValue>) -> anyhow::Result<RpcValue> {
    match db {
        DbPool::Postgres(pool) => sql_exec_postgres(pool, query).await,
        DbPool::Sqlite(pool) => sql_exec_sqlite(pool, query).await,
    }
}

pub async fn sql_select(db: &DbPool, query: &Vec<RpcValue>) -> anyhow::Result<RpcValue> {
    match db {
        DbPool::Postgres(pool) => sql_select_postgres(pool, query).await,
        DbPool::Sqlite(pool) => sql_select_sqlite(pool, query).await,
    }
}

pub async fn sql_exec_sqlite(db_pool: &Pool<Sqlite>, query: &Vec<RpcValue>) -> anyhow::Result<RpcValue> {
    let mut sql = "".to_string();
    let mut params: Vec<RpcValue> = vec![];
    for (i, val) in query.iter().enumerate() {
        if i == 0 {
            sql = convert_postgres_to_sqlite_params(val.as_str());
        } else {
            params.push(val.clone());
        }
    }
    let mut query = sqlx::query(&sql);
    for param in &params {
        if param.is_string() {
            query = query.bind(param.as_str());
        } else if param.is_int() {
            query = query.bind(param.as_i64());
        } else if param.is_bool() {
            query = query.bind(param.as_bool());
        } else if param.is_null() {
            query = query.bind(None::<&str>);
        } else {
            todo!()
        }
    }
    let result = query.execute(db_pool).await?;
    Ok(RpcValue::from(result.rows_affected()))
}
pub async fn sql_exec_postgres(db_pool: &Pool<Postgres>, query: &Vec<RpcValue>) -> anyhow::Result<RpcValue> {
    let mut sql = "".to_string();
    let mut params: Vec<RpcValue> = vec![];
    for (i, val) in query.iter().enumerate() {
        if i == 0 {
            sql = val.as_str().to_string();
        } else {
            params.push(val.clone());
        }
    }
    let mut query = sqlx::query(&sql);
    for param in &params {
        if param.is_string() {
            query = query.bind(param.as_str());
        } else if param.is_int() {
            query = query.bind(param.as_i64());
        } else if param.is_bool() {
            query = query.bind(param.as_bool());
        } else if param.is_null() {
            query = query.bind(None::<&str>);
        } else {
            todo!()
        }
    }
    let result = query.execute(db_pool).await?;
    Ok(RpcValue::from(result.rows_affected()))
}
pub async fn sql_select_sqlite(db_pool: &Pool<Sqlite>, query: &Vec<RpcValue>) -> anyhow::Result<RpcValue> {
    let mut sql = "".to_string();
    let mut params: Vec<RpcValue> = vec![];
    for (i, val) in query.iter().enumerate() {
        if i == 0 {
            sql = convert_postgres_to_sqlite_params(val.as_str());
        } else {
            params.push(val.clone());
        }
    }
    let mut query = sqlx::query(&sql);
    for param in &params {
        if param.is_string() {
            query = query.bind(param.as_str());
        } else if param.is_int() {
            query = query.bind(param.as_i64());
        } else if param.is_bool() {
            query = query.bind(param.as_bool());
        } else if param.is_null() {
            query = query.bind(None::<&str>);
        } else {
            todo!()
        }
    }
    let rows = query.fetch_all(db_pool).await?;
    let mut result = List::new();
    for row in rows {
        let mut map = Map::new();
        for (i, col) in row.columns().iter().enumerate() {
            let col_name = col.name();
            let val = rpc_value_from_sqlite_row(&row, i)?;
            map.insert(col_name.to_string(), val);
        }
        result.push(map.into());
    }
    Ok(result.into())
}
pub async fn sql_select_postgres(db_pool: &Pool<Postgres>, query: &Vec<RpcValue>) -> anyhow::Result<RpcValue> {
    let mut sql = "".to_string();
    let mut params: Vec<RpcValue> = vec![];
    for (i, val) in query.iter().enumerate() {
        if i == 0 {
            sql = val.as_str().to_string();
        } else {
            params.push(val.clone());
        }
    }
    let mut query = sqlx::query(&sql);
    for param in &params {
        if param.is_string() {
            query = query.bind(param.as_str());
        } else if param.is_int() {
            query = query.bind(param.as_i64());
        } else if param.is_bool() {
            query = query.bind(param.as_bool());
        } else if param.is_null() {
            query = query.bind(None::<&str>);
        } else {
            todo!()
        }
    }
    let rows = query.fetch_all(db_pool).await?;
    let mut result = List::new();
    for row in rows {
        let mut map = Map::new();
        for (i, col) in row.columns().iter().enumerate() {
            let col_name = col.name();
            let val = rpc_value_from_postgres_row(&row, i)?;
            map.insert(col_name.to_string(), val);
        }
        result.push(map.into());
    }
    Ok(result.into())
}

pub fn rpc_value_from_sqlite_row(row: &SqliteRow, index: usize) -> anyhow::Result<RpcValue> {
    let val = row.try_get_raw(index)?;
    let type_name = val.type_info().name().to_uppercase();
    if val.is_null() {
        return Ok(RpcValue::null());
    }
    if type_name.contains("TEXT") || type_name.contains("STRING") {
        Ok(RpcValue::from(row.get::<Option<String>, _>(index)))
    } else if type_name.contains("INT") {
        Ok(RpcValue::from(row.get::<Option<i64>, _>(index)))
    } else if type_name.contains("BOOL") {
        Ok(RpcValue::from(row.get::<Option<bool>, _>(index)))
    } else {
        // fallback to string
        Ok(RpcValue::from(row.get::<Option<String>, _>(index)))
    }
}
pub fn rpc_value_from_postgres_row(row: &PgRow, index: usize) -> anyhow::Result<RpcValue> {
    let val = row.try_get_raw(index)?;
    let type_name = val.type_info().name().to_uppercase();
    if val.is_null() {
        return Ok(RpcValue::null());
    }
    if type_name.contains("TEXT") || type_name.contains("STRING") || type_name.contains("VARCHAR") {
        Ok(RpcValue::from(row.get::<Option<String>, _>(index)))
    } else if type_name.contains("INT") {
        Ok(RpcValue::from(row.get::<Option<i64>, _>(index)))
    } else if type_name.contains("BOOL") {
        Ok(RpcValue::from(row.get::<Option<bool>, _>(index)))
    } else {
        // fallback to string
        Ok(RpcValue::from(row.get::<Option<String>, _>(index)))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, FromRow)]
pub(crate) struct EventRecord {
    pub id: i64,
    pub api_token: String,
    pub owner: String,
}

fn convert_postgres_to_sqlite_params(sql: &str) -> String {
    let re = Regex::new(r"\$(\d+)").unwrap();
    re.replace_all(sql, "?$1").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use shvproto::{List, Map, RpcValue};
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::postgres::PgPoolOptions;
    use log::warn;

    async fn test_sql_select_with_db(db_pool: DbPool) {
        let query = vec![
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)".into(),
        ];
        let res = sql_exec(&db_pool, &query).await;
        res.unwrap();

        let query = vec![
            "INSERT INTO users (id, name) VALUES ($1, $2)".into(),
            1.into(),
            "Jane Doe".into(),
        ];
        let res = sql_exec(&db_pool, &query).await;
        res.unwrap();

        let query = vec![
            "SELECT * FROM users".into(),
        ];
        let result = sql_select(&db_pool, &query).await;
        let expected: List = vec![
            vec![
                ("id".to_string(), 1.into()),
                ("name".to_string(), "Jane Doe".into()),
            ].into_iter().collect::<Map>().into()
        ].into();
        assert_eq!(result.unwrap(), RpcValue::from(expected));
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
                    let query = vec!["DROP TABLE IF EXISTS users".into()];
                    sql_exec_postgres(pool, &query).await
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
