use config::Config;

use anyhow::anyhow;
use clap::Parser;
use env_logger::Env;
use futures::select;
use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use shvnode::{METH_GET, METH_SET, PUBLIC_DIR_LS_METHODS};
use shvproto::{from_rpcvalue, to_rpcvalue, RpcValue};
use shvrpc::client::{self, LoginParams};
use shvrpc::framerw::{FrameReader, FrameWriter};
use shvrpc::metamethod::{AccessLevel, Flag, MetaMethod};
use shvrpc::rpcdiscovery::LsParam;
use shvrpc::rpcframe::RpcFrame;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::streamrw::{StreamFrameReader, StreamFrameWriter};
use shvrpc::util::login_from_url;
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use sqlx::sqlite::{SqlitePoolOptions, SqliteRow};
use sqlx::Row;
use tokio::sync::RwLock;
use std::backtrace::Backtrace;
use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Sender};
use tokio::time::sleep;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use futures_util::FutureExt;
use sqlx::SqlitePool;

mod shvnode;
mod config;
mod sql;

use sql::{sql_exec, sql_select, DbPool};

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

    /// Path where event qbe files can be found
    #[arg(short, long)]
    qbe_path: Option<String>,

    /// Print effective config
    #[arg(long)]
    print_config: bool,
}

struct State {
    app_db: SqlitePool,
    event_db_pools: BTreeMap<i64, DbPool>,
}
type SharedState = Arc<RwLock<State>>;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    let args = Args::parse();

    let mut config = if let Some(config_path) = args.config {
        info!("Loading config file {config_path}");
        let content = fs::read_to_string(config_path)?;
        serde_yaml::from_str(&content)?
    } else {
        Config::default()
    };

    if let Some(url) = args.url {
        config.client.url = url;
    }
    if let Some(database) = args.database {
        config.database = config::DbConfig { url: database, };
    }

    if args.print_config {
        let yaml = serde_yaml::to_string(&config)?;
        println!("{}", yaml);
        return Ok(())
    }

    info!("==================================");
    info!("Starting application {}", env!("CARGO_PKG_NAME"));
    info!("==================================");
    info!("Connecting to app database: {}", config.database.url);
    let app_db = match SqlitePoolOptions::new().connect("sqlite::memory:").await {
        Ok(db_pool) => {
            info!("Connecting to app database: OK");
            if let Err(e) = sqlx::migrate!("./db/migrations").run(&db_pool).await {
                return Err(anyhow!("Database migration failed: {e}"));
            }
            info!("Database migration ... OK");
            db_pool
        }
        Err(e) => {
            return Err(anyhow!("Cannot connect to app database: {e}"));
        }
    };

    let state = State {
        app_db,
        event_db_pools: BTreeMap::new(),
    };
    let state: SharedState = Arc::new(RwLock::new(state));

    let url = url::Url::parse(&config.client.url)?;
    match broker_peer_loop_from_url(url, state).await {
        Ok(()) => Ok(()),
        Err(e) => Err(anyhow!("Broker peer loop error: {e}")),
    }
}

async fn broker_peer_loop_from_url(url: url::Url, state: SharedState) -> anyhow::Result<()> {
    let (host, port) = (url.host_str().unwrap_or_default(), url.port().unwrap_or(3755));
    let address = format!("{}:{}", host, port);
    // Establish a connection
    info!("Connecting to broker TCP peer: {address}");
    let stream = TcpStream::connect(&address).await?;
    let (reader, writer) = stream.into_split();
    let reader = tokio::io::BufReader::new(reader).compat();
    let writer = writer.compat_write();

    let peer_id = 1;
    let frame_reader = StreamFrameReader::new(reader).with_peer_id(peer_id);
    let frame_writer = StreamFrameWriter::new(writer).with_peer_id(peer_id);
    broker_peer_loop(url, frame_reader, frame_writer, state).await
}

fn rpc_to_anyhow(err: shvrpc::Error) -> anyhow::Error {
    error!("RPC Error: {err}\nbacktrace: {}", Backtrace::capture());
    anyhow!("RPC error: {}", err)
}
pub(crate) fn sqlx_to_anyhow(err: sqlx::Error) -> anyhow::Error {
    error!("SQL Error: {err}\nbacktrace: {}", Backtrace::capture());
    anyhow!("SQL error: {}", err)
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

fn process_dir_ls<'a, 'b>(method: &str, param: Option<&RpcValue>,
    methods: impl Iterator<Item=&'a MetaMethod>,
    mut dirs: impl Iterator<Item=&'b str>) -> anyhow::Result<RpcValue> {
    match method {
        "dir" => {
            let dir = shvnode::dir(methods, param.into());
            Ok(dir)
        }
        "ls" => {
            // let list: Vec<_> = state.read().await.event_db_pools.keys().map(|k| format!("{k}")).collect();
            match LsParam::from(param) {
                LsParam::List => {
                    Ok(dirs.collect::<Vec<_>>().into())
                }
                LsParam::Exists(dir) => {
                    Ok(dirs.any(|d| d == dir).into())
                }
            }
        }
        unknown_method => {
            Err(anyhow!("Unknown method: {unknown_method}"))
        }
    }
}

async fn process_request(frame: RpcFrame, sender: Sender<RpcFrame>, state: SharedState) -> anyhow::Result<()> {
    debug!("Processing frame from broker: {frame:?}");
    assert!(frame.is_request());
    let request = frame.to_rpcmesage().map_err(rpc_to_anyhow)?;
    let shv_path = frame.shv_path().unwrap_or_default();
    let method = frame.method().ok_or_else(|| anyhow!("Request without method"))?;
    let param = request.param();
    let result = 'result: {
        const API_DIR: &str = "api";
        const EVENT_DIR: &str = "event";
        const SQL_DIR: &str = "sql";
        let mut dirs = shv_path.split('/');
        if let Some(dir) = dirs.next() {
            if dir.is_empty() {
                break 'result process_dir_ls(method, param, PUBLIC_DIR_LS_METHODS.iter(), [API_DIR].iter().copied());
            }
            if dir == API_DIR {
                if let Some(dir) = dirs.next() && dir == EVENT_DIR {
                    if let Some(event_id) = dirs.next() {
                        if let Ok(event_id) = event_id.parse::<i64>() {
                            if let Some(dir) = dirs.next() && dir == SQL_DIR {
                                if dirs.next().is_none() {
                                    let query = request.param().unwrap_or_default().as_list();
                                    match method {
                                        "exec" => break 'result sql_exec(&state, event_id, query).await,
                                        "select" => break 'result sql_select(&state, event_id, query).await,
                                        unknown_method => {
                                            break 'result Err(anyhow!("Unknown method: {unknown_method}"))
                                        }
                                    }
                                }
                            }
                        }
                    } else {
                        match method {
                            METH_GET => {
                                let event_id = param.unwrap_or_default().as_i64();
                                let db_pool = state.read().await.app_db.clone();
                                let rec = sqlx::query_as::<_, sql::EventRecord>("SELECT * FROM events WHERE id=?")
                                    .bind(event_id)
                                    .fetch_one(&db_pool).await?;
                                break 'result to_rpcvalue(&rec).map_err(|e| anyhow!(e))
                            }
                            METH_SET => {
                                #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
                                struct SetValueParams (i64, sql::EventRecord);
                                match from_rpcvalue::<SetValueParams>(param.unwrap_or_default()) {
                                    Ok(SetValueParams(event_id, value)) => {
                                        if event_id == 0 {
                                            break 'result Err(anyhow!("Invalid event ID"));
                                        }
                                        let db_pool = state.read().await.app_db.clone();
                                        break 'result sqlx::query("UPDATE events SET (api_token, owner) VALUES (?, ?) WHERE id=?")
                                            .bind(value.api_token)
                                            .bind(value.owner)
                                            .bind(event_id)
                                            .execute(&db_pool).await
                                            .map(|_| RpcValue::null())
                                            .map_err(sqlx_to_anyhow)
                                    }
                                    Err(e) => break 'result Err(anyhow!(e))
                                };
                            }
                            _ => {
                                let db_pool = state.read().await.app_db.clone();
                                let ids = sqlx::query("SELECT id FROM events ORDER BY id")
                                    .map(|row: SqliteRow| { row.get::<i64, _>(0).to_string() })
                                    .fetch_all(&db_pool).await?;
                                pub const META_METHOD_GET: MetaMethod = MetaMethod { name: METH_GET, flags: Flag::IsGetter as u32, access: AccessLevel::Read, param: "Any", result: "Any", signals: &[("recchng", Some("i(0,)"))], description: "" };
                                pub const META_METHOD_SET: MetaMethod = MetaMethod { name: METH_SET, flags: Flag::IsSetter as u32, access: AccessLevel::Write, param: "Any", result: "n", signals: &[], description: "" };
                                break 'result process_dir_ls(method, param, PUBLIC_DIR_LS_METHODS.iter().chain([META_METHOD_GET, META_METHOD_SET].iter()), ids.iter().map(|s| s.as_str()));
                            }
                        }
                    }
                } else {
                    break 'result process_dir_ls(method, param, PUBLIC_DIR_LS_METHODS.iter(), [EVENT_DIR].iter().copied());
                }
            }
        }
        Err(anyhow!("Invalid path: {shv_path:?}"))
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
