use shvproto::{List, Map, RpcValue};
use sqlx::{Pool, Postgres, Sqlite, sqlite::SqliteRow, postgres::PgRow};
use sqlx::{Column, Row, TypeInfo, ValueRef, postgres::PgPool, SqlitePool};

pub enum DbPool {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

pub async fn sql_exec_sqlite(db_pool: &Pool<Sqlite>, query: &Vec<RpcValue>) -> anyhow::Result<RpcValue> {
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
