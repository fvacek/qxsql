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
use sqlx::{Any, AnyPool};
use tokio::sync::RwLock;
use std::backtrace::Backtrace;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Sender};
use tokio::time::sleep;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use futures_util::FutureExt;
use sqlx::{Column, Row, TypeInfo, ValueRef};

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

#[derive(Clone)]
struct State {
    db_pool: sqlx::Pool<Any>,
}
type SharedState = Arc<RwLock<State>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    let mut settings = ::config::Config::builder();
    if let Some(config_path) = args.config {
        settings = settings.add_source(File::with_name(&config_path));
    }
    let settings = settings.set_default("client.url", "tcp://localhost:3755?user=test&password=password")?;
    let settings = settings.set_default("database.url", "/tmp/qxsqld.sqlite")?;

    let settings = settings.build()?;

    let mut config: config::Config = settings.try_deserialize()?;

    if let Some(url) = args.url {
        config.client.url = url;
    }
    if let Some(database) = args.database {
        config.database.url = database;
    }

    if args.print_config {
        let yaml = serde_yaml::to_string(&config)?;
        println!("{}", yaml);
        return Ok(())
    }

    info!("Connecting to database: {}", config.database.url);
    let db_pool = AnyPool::connect(&config.database.url).await?;
    info!("Database connected.");

    let state = State {
        db_pool,
    };
    let state: SharedState = Arc::new(RwLock::new(state));

    let url = url::Url::parse(&config.client.url)?;
    broker_peer_loop_from_url(url, state).await?;

    Ok(())
}

async fn broker_peer_loop_from_url(url: url::Url, state: SharedState) -> anyhow::Result<()> {
    let (host, port) = (url.host_str().unwrap_or_default(), url.port().unwrap_or(3755));
    let address = format!("{host}:{port}");
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
                            let list = vec!["sql"];
                            Ok(list.into())
                        }
                        LsParam::Exists(dir) => {
                            if dir == "sql" {
                                Ok(true.into())
                            } else {
                                Ok(false.into())
                            }
                        }
                    }
                }
                unknown_method => {
                    Err(anyhow!("Unknown method: {unknown_method}"))
                }
            }
        } else if shv_path == "sql" {
            let query = request.param().ok_or_else(|| anyhow!("Missing query parameter"))?.as_list();
            match method {
                "exec" => {
                    let result = sql_exec(&state, query).await?;
                    Ok(result)
                }
                "select" => {
                    let result = sql_select(&state, query).await?;
                    Ok(result)
                }
                unknown_method => {
                    Err(anyhow!("Unknown method: {unknown_method}"))
                }
            }
        } else {
            Err(anyhow!("Unknown path: {shv_path:?}"))
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
async fn sql_exec(state: &SharedState, query: &Vec<RpcValue>) -> anyhow::Result<RpcValue> {
    let mut sql = "".to_string();
    let mut params: Vec<RpcValue> = vec![];
    for (i, val) in query.iter().enumerate() {
        if i == 0 {
            sql = val.as_str().to_string();
        } else {
            params.push(val.clone());
        }
    }
    let db_pool = &state.read().await.db_pool;
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
async fn sql_select(state: &SharedState, query: &Vec<RpcValue>) -> anyhow::Result<RpcValue> {
    let mut sql = "".to_string();
    let mut params: Vec<RpcValue> = vec![];
    for (i, val) in query.iter().enumerate() {
        if i == 0 {
            sql = val.as_str().to_string();
        } else {
            params.push(val.clone());
        }
    }
    let db_pool = &state.read().await.db_pool;
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
            let val = rpc_value_from_row(&row, i)?;
            map.insert(col_name.to_string(), val);
        }
        result.push(map.into());
    }
    Ok(result.into())
}

fn rpc_value_from_row(row: &sqlx::any::AnyRow, index: usize) -> anyhow::Result<RpcValue> {
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




#[cfg(test)]
mod tests {
    use super::*;
    use shvproto::{List, Map};
    use sqlx::any::install_default_drivers;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    

    #[tokio::test]
    async fn test_sql_select() {
        install_default_drivers();
        let db_pool = AnyPool::connect("sqlite:file:memdb1?mode=memory&cache=shared").await.unwrap();
        sqlx::query("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL)").execute(&db_pool).await.unwrap();
        let state = State {
            db_pool,
        };
        let state = Arc::new(RwLock::new(state));

        let query = vec![
            "INSERT INTO users (id, name) VALUES (?, ?)".into(),
            1.into(),
            "Jane Doe".into(),
        ];
        sql_exec(&state, &query).await.unwrap();

        let query = vec![
            "SELECT * FROM users".into(),
        ];
        let result = sql_select(&state, &query).await.unwrap();
        let expected: List = vec![
            vec![
                ("id".to_string(), 1.into()),
                ("name".to_string(), "Jane Doe".into()),
            ].into_iter().collect::<Map>().into()
        ].into();
        assert_eq!(result, RpcValue::from(expected));
    }

    

}
