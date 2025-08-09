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
use tokio::sync::RwLock;
use std::backtrace::Backtrace;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Sender};
use tokio::time::sleep;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use futures_util::FutureExt;
use sqlx::{postgres::PgPool, SqlitePool};

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

    /// SHV path to mount the service on
    #[arg(short, long)]
    path: Option<String>,

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
    env_logger::init();
    let args = Args::parse();

    let mut settings = ::config::Config::builder();
    if let Some(config_path) = args.config {
        settings = settings.add_source(File::with_name(&config_path));
    } else {
        settings = settings.add_source(File::with_name("config.yaml"));
    }
    let settings = settings.set_default("client.url", "tcp://localhost:3755?user=test&password=password")?;

    let settings = settings.build()?;

    let mut config: config::Config = settings.try_deserialize()?;

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

    let app_db = {
        let Ok(db_pool) = SqlitePool::connect(&config.database.url).await else {
            return Err(anyhow!("Unsupported database scheme for {}", config.database.url));
        };
        info!("Connecting to events database: {} ... OK", config.database.url);
        db_pool
    };

    // let mut event_db_pools = BTreeMap::new();
    // for (db_name, db_config) in &config.event_databases {
    //     info!("Connecting to database: {}", db_name);
    //     let db_pool = if db_config.url.starts_with("postgres") {
    //         DbPool::Postgres(PgPool::connect(&db_config.url).await?)
    //     } else if db_config.url.starts_with("sqlite") {
    //         DbPool::Sqlite(SqlitePool::connect(&db_config.url).await?)
    //     } else {
    //         return Err(anyhow!("Unsupported database scheme for {}", db_name));
    //     };
    //     db_pools.insert(db_name.clone(), db_pool);
    //     info!("Database {} connected.", db_name);
    // }

    let state = State {
        app_db,
        event_db_pools: BTreeMap::new(),
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
    let result = 'result: {
        if shv_path.is_empty() {
            match method {
                "dir" => {
                    let dir = shvnode::dir(PUBLIC_DIR_LS_METHODS.iter(), request.param().into());
                    break 'result Ok(dir)
                }
                "ls" => {
                    let list: Vec<_> = state.read().await.event_db_pools.keys().map(|k| format!("{k}")).collect();
                    match LsParam::from(request.param()) {
                        LsParam::List => {
                            break 'result Ok(list.into())
                        }
                        LsParam::Exists(dir) => {
                            break 'result Ok(list.contains(&dir).into())
                        }
                    }
                }
                unknown_method => {
                    break 'result Err(anyhow!("Unknown method: {unknown_method}"))
                }
            }
        } else if let Some((dir, shv_path)) = shv_path.split_once('/') &&    dir == "api" {
            if let Some((dir, shv_path)) = shv_path.split_once('/') && dir   == "event" {
                if let Some((event_id, shv_path)) = shv_path.split_once(     '/') {
                    if let Ok(event_id) = event_id.parse::<i64>() {
                        if let Some((dir, shv_path)) = shv_path.split_once('/') && dir == "sql" && shv_path.is_empty() {
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
