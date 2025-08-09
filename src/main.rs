use ::config::File;
use anyhow::anyhow;
use clap::Parser;
use futures::select;
use log::{debug, error, info, warn};
use shvnode::PUBLIC_DIR_LS_METHODS;
use shvrpc::client::{self, LoginParams};
use shvrpc::framerw::{FrameReader, FrameWriter};
use shvrpc::rpcdiscovery::LsParam;
use shvrpc::rpcframe::RpcFrame;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::streamrw::{StreamFrameReader, StreamFrameWriter};
use shvrpc::util::login_from_url;
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use shvproto::{List, Map, RpcValue};
use sqlx::{Pool, Postgres, Sqlite, sqlite::SqliteRow, postgres::PgRow};
use tokio::sync::RwLock;
use std::backtrace::Backtrace;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Sender};
use tokio::time::sleep;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use futures_util::FutureExt;
use sqlx::{Column, Row, TypeInfo, ValueRef, postgres::PgPool, SqlitePool};

mod shvnode;
mod config;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to config file
    #[arg(short, long)]
    config: Option<String>,

    /// SHV broker URL
    #[arg(short, long)]
    url: Option<String>,

    /// Database connection string
    #[arg(short, long)]
    database: Option<String>,

    /// SHV path to mount the service on
    #[arg(short, long)]
    path: Option<String>,

    /// Print effective config
    #[arg(long)]
    print_config: bool,
}

enum DbPool {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

struct State {
    db_pools: HashMap<String, DbPool>,
}
type SharedState = Arc<RwLock<State>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    let mut settings = ::config::Config::builder();
    if let Some(config_path) = args.config {
        settings = settings.add_source(File::with_name(&config_path));
    } else {
        settings = settings.add_source(File::with_name("config.yaml"));
    }
    let settings = settings.set_default("client.url", "tcp://localhost:3755?user=test&password=password")?;
    // let settings = settings.set_default("databases", BTreeMap::<String, config::DbConfig>::new())?;

    let settings = settings.build()?;

    let mut config: config::Config = settings.try_deserialize()?;

    if let Some(url) = args.url {
        config.client.url = url;
    }
    if let Some(database) = args.database {
        config.databases.insert("db01".to_string(), config::DbConfig {
            url: database,
        });
    }

    if args.print_config {
        let yaml = serde_yaml::to_string(&config)?;
        println!("{}", yaml);
        return Ok(())
    }

    let mut db_pools = HashMap::new();
    for (db_name, db_config) in &config.databases {
        info!("Connecting to database: {}", db_name);
        let db_pool = if db_config.url.starts_with("postgres") {
            DbPool::Postgres(PgPool::connect(&db_config.url).await?)
        } else if db_config.url.starts_with("sqlite") {
            DbPool::Sqlite(SqlitePool::connect(&db_config.url).await?)
        } else {
            return Err(anyhow!("Unsupported database scheme for {}", db_name));
        };
        db_pools.insert(db_name.clone(), db_pool);
        info!("Database {} connected.", db_name);
    }

    let state = State {
        db_pools,
    };
    let state: SharedState = Arc::new(RwLock::new(state));

    let url = url::Url::parse(&config.client.url)?;
    broker_peer_loop_from_url(url, state).await?;

    Ok(())
}

async fn broker_peer_loop_from_url(url: url::Url, state: SharedState) -> anyhow::Result<()> {
    let (host, port) = (url.host_str().unwrap_or_default(), url.port().unwrap_or(3755));
    let address = format!("{}:{}", host, port);
    // Establish a connection
    debug!("Connecting to broker TCP peer: {address}");
    let stream = TcpStream::connect(&address).await?;
    let (reader, writer) = stream.into_split();
    let reader = tokio::io::BufReader::new(reader).compat();
    let writer = writer.compat_write();

    let peer_id = 1;
    let frame_reader = StreamFrameReader::new(reader).with_peer_id(peer_id);
    let frame_writer = StreamFrameWriter::new(writer).with_peer_id(peer_id);
    return broker_peer_loop(url, frame_reader, frame_writer, state).await
}

fn rpc_to_anyhow(err: shvrpc::Error) -> anyhow::Error {
    error!("RPC Error: {err}\nbacktrace: {}", Backtrace::capture());
    anyhow!("RPC error: {}", err)
}
async fn broker_peer_loop(url: url::Url, mut frame_reader: impl FrameReader + Send, mut frame_writer: impl FrameWriter + Send, state: SharedState) -> anyhow::Result<()> {
    // login
    let (user, password) = login_from_url(&url);
    let login_params = LoginParams {
        user,
        password,
        mount_point: "test/qxsql".to_owned(),
        // device_id: config.client.device_id.clone().unwrap_or_default().to_owned(),
        ..Default::default()
    };

    info!("Heartbeat interval set to: {:?}", login_params.heartbeat_interval);
    client::login(&mut frame_reader, &mut frame_writer, &login_params, false).await.map_err(rpc_to_anyhow)?;

    info!("Login to broker OK");

    let (frame_sender, mut frame_receiver) = mpsc::channel(32);

    let make_timeout_fut = || {
        Box::pin(sleep(login_params.heartbeat_interval)).fuse()
    };
    let mut fut_timeout = make_timeout_fut();
    let mut fut_rq_frame = frame_reader.receive_frame().fuse();
    let mut fut_resp_frame = Box::pin(frame_receiver.recv()).fuse();

    loop {
        select! {
            _ = fut_timeout => {
                // send heartbeat
                const METH_PING: &str = "ping";
                let msg = RpcMessage::new_request(".app", METH_PING, None);
                debug!("sending ping");
                frame_writer.send_message(msg).await.map_err(rpc_to_anyhow)?;
                fut_timeout = make_timeout_fut();
            },
            rq_frame = fut_rq_frame => match rq_frame {
                Ok(frame) => {
                    process_broker_client_peer_frame(frame, frame_sender.clone(), state.clone()).await?;
                    drop(fut_rq_frame);
                    fut_rq_frame = frame_reader.receive_frame().fuse();
                }
                Err(e) => {
                    return Err(anyhow!("Read RQ frame error: {e}"));
                }
            },
            resp_frame = fut_resp_frame => {
                    match resp_frame {
                    Some(frame) => {
                        // info!("Received response frame {:?}", frame);
                        frame_writer.send_frame(frame).await.map_err(rpc_to_anyhow)?;
                        drop(fut_resp_frame);
                        fut_resp_frame = Box::pin(frame_receiver.recv()).fuse();
                    }
                    None => {
                        return Err(anyhow!("Read RESP frame error"));
                    }
                }
            }
        }
    }
}

async fn process_broker_client_peer_frame(frame: RpcFrame, sender: Sender<RpcFrame>, state: SharedState) -> anyhow::Result<()> {
    if frame.is_request() {
        process_request(frame, sender, state).await?;
    } else if frame.is_response() {
        warn!("RPC response should not be received from client connection to parent broker: {}", &frame);
    } else {
        warn!("RPC signal should not be received from client connection to parent broker: {}", &frame);
    }
    Ok(())
}

async fn process_request(frame: RpcFrame, sender: Sender<RpcFrame>, state: SharedState) -> anyhow::Result<()> {
    debug!("Processing frame from broker: {frame:?}");
    assert!(frame.is_request());
    let request = frame.to_rpcmesage().map_err(rpc_to_anyhow)?;
    let shv_path = frame.shv_path().unwrap_or_default();
    let method = frame.method().ok_or_else(|| anyhow!("Request without method"))?;
    let result = {
        if shv_path.is_empty() {
            match method {
                "dir" => {
                    let dir = shvnode::dir(PUBLIC_DIR_LS_METHODS.iter(), request.param().into());
                    Ok(dir)
                }
                "ls" => {
                    match LsParam::from(request.param()) {
                        LsParam::List => {
                            let list: Vec<String> = state.read().await.db_pools.keys().cloned().collect();
                            Ok(list.into())
                        }
                        LsParam::Exists(dir) => {
                            Ok(state.read().await.db_pools.contains_key(&dir).into())
                        }
                    }
                }
                unknown_method => {
                    Err(anyhow!("Unknown method: {unknown_method}"))
                }
            }
        } else if let Some((db_name, _)) = shv_path.split_once('/') {
            let query = request.param().ok_or_else(|| anyhow!("Missing query parameter"))?.as_list();
            let state = state.read().await;
            if let Some(db_pool) = state.db_pools.get(db_name) {
                match method {
                    "exec" => {
                        match db_pool {
                            DbPool::Postgres(pool) => sql_exec_postgres(pool, query).await,
                            DbPool::Sqlite(pool) => sql_exec_sqlite(pool, query).await,
                        }
                    }
                    "select" => {
                        match db_pool {
                            DbPool::Postgres(pool) => sql_select_postgres(pool, query).await,
                            DbPool::Sqlite(pool) => sql_select_sqlite(pool, query).await,
                        }
                    }
                    unknown_method => {
                        Err(anyhow!("Unknown method: {unknown_method}"))
                    }
                }
            } else {
                Err(anyhow!("Unknown database: {db_name}"))
            }
        } else {
            Err(anyhow!("Invalid path: {shv_path:?}"))
        }
    };
    let resp_meta = RpcFrame::prepare_response_meta(&frame.meta).map_err(|e| anyhow!("Failed to prepare response meta: {e}"))?;
    let mut resp = RpcMessage::from_meta(resp_meta);
    match result {
        Ok(result) => {
            resp.set_result(result);
        }
        Err(err) => {
            resp.set_error(RpcError{ code: RpcErrorCode::MethodCallException, message: err.to_string()});
        }
    }
    let resp_frame = resp.to_frame().map_err(rpc_to_anyhow)?;
    sender.send(resp_frame).await?;
    Ok(())
}
async fn sql_exec_sqlite(db_pool: &Pool<Sqlite>, query: &Vec<RpcValue>) -> anyhow::Result<RpcValue> {
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
async fn sql_exec_postgres(db_pool: &Pool<Postgres>, query: &Vec<RpcValue>) -> anyhow::Result<RpcValue> {
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
async fn sql_select_sqlite(db_pool: &Pool<Sqlite>, query: &Vec<RpcValue>) -> anyhow::Result<RpcValue> {
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
async fn sql_select_postgres(db_pool: &Pool<Postgres>, query: &Vec<RpcValue>) -> anyhow::Result<RpcValue> {
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

fn rpc_value_from_sqlite_row(row: &SqliteRow, index: usize) -> anyhow::Result<RpcValue> {
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
fn rpc_value_from_postgres_row(row: &PgRow, index: usize) -> anyhow::Result<RpcValue> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use shvproto::{List, Map};
    use std::sync::Arc;
    use tokio::sync::RwLock;
    use sqlx::sqlite::SqlitePoolOptions;
    use sqlx::postgres::PgPoolOptions;

    async fn test_sql_select_with_db(db_pool: DbPool) {
        let mut db_pools = HashMap::new();
        let db_name = "test_db".to_string();
        db_pools.insert(db_name.clone(), db_pool);
        let state = State {
            db_pools,
        };
        let state = Arc::new(RwLock::new(state));

        let query = vec![
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)".into(),
        ];
        {
            let state_guard = state.read().await;
            let db_pool = state_guard.db_pools.get(&db_name).unwrap();
            let res = match db_pool {
                DbPool::Postgres(pool) => sql_exec_postgres(pool, &query).await,
                DbPool::Sqlite(pool) => sql_exec_sqlite(pool, &query).await,
            };
            res.unwrap();
        }

        let insert_query = match state.read().await.db_pools.get(&db_name).unwrap() {
            DbPool::Postgres(_) => "INSERT INTO users (id, name) VALUES ($1, $2)",
            DbPool::Sqlite(_) => "INSERT INTO users (id, name) VALUES (?, ?)",
        };
        let query = vec![
            insert_query.into(),
            1.into(),
            "Jane Doe".into(),
        ];
        {
            let state_guard = state.read().await;
            let db_pool = state_guard.db_pools.get(&db_name).unwrap();
            let res = match db_pool {
                DbPool::Postgres(pool) => sql_exec_postgres(pool, &query).await,
                DbPool::Sqlite(pool) => sql_exec_sqlite(pool, &query).await,
            };
            res.unwrap();
        }

        let query = vec![
            "SELECT * FROM users".into(),
        ];
        let result = {
            let state_guard = state.read().await;
            let db_pool = state_guard.db_pools.get(&db_name).unwrap();
            match db_pool {
                DbPool::Postgres(pool) => sql_select_postgres(pool, &query).await,
                DbPool::Sqlite(pool) => sql_select_sqlite(pool, &query).await,
            }
        };
        let expected: List = vec![
            vec![
                ("id".to_string(), 1.into()),
                ("name".to_string(), "Jane Doe".into()),
            ].into_iter().collect::<Map>().into()
        ].into();
        assert_eq!(result.unwrap(), RpcValue::from(expected));
    }

    #[tokio::test]
    async fn test_sql_select() {
        let db_pool = DbPool::Sqlite(SqlitePoolOptions::new()
            .connect("sqlite::memory:")
            .await
            .unwrap());
        test_sql_select_with_db(db_pool).await;
    }

    #[tokio::test]
    async fn test_postgres_sql_select() {
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
            warn!("Skipping postgres test, QXSQLD_POSTGRES_URL not set");
        }
    }
}
