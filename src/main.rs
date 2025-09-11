use config::Config;
use anyhow::anyhow;
use clap::Parser;
use env_logger::Env;
use futures::select;
use log::{debug, error, info, warn};
use shvnode::PUBLIC_DIR_LS_METHODS;
use shvproto::RpcValue;
use shvrpc::client::{self, LoginParams};
use shvrpc::framerw::{FrameReader, FrameWriter};
use shvrpc::metamethod::MetaMethod;
use shvrpc::rpcdiscovery::LsParam;
use shvrpc::rpcframe::RpcFrame;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::streamrw::{StreamFrameReader, StreamFrameWriter};
use shvrpc::util::login_from_url;
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::postgres::PgPoolOptions;
use tokio::io::BufReader;
use tokio::sync::RwLock;
use url::Url;
use std::backtrace::Backtrace;
use std::fs;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Sender};
use tokio::time::sleep;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use futures_util::FutureExt;

mod shvnode;
mod config;
mod sql;

use sql::DbPool;

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

    /// Print effective config
    #[arg(long)]
    print_config: bool,
}

struct State {
    db: DbPool,
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
        config.client.url = Url::parse(&url)?;
    }
    if let Some(database) = args.database {
        config.db.url = Url::parse(&database)?;
    }

    if args.print_config {
        let yaml = serde_yaml::to_string(&config)?;
        println!("{}", yaml);
        return Ok(())
    }

    info!("==================================");
    info!("Starting application {}", env!("CARGO_PKG_NAME"));
    info!("==================================");
    info!("Connecting to app database: {}", config.db.url);
    let db = if config.db.url.scheme() == "sqlite" {
        DbPool::Sqlite(SqlitePoolOptions::new().connect(config.db.url.as_str()).await?)
    } else {
        DbPool::Postgres(PgPoolOptions::new().connect(config.db.url.as_str()).await?)
    };

    let state = State { db };
    let state: SharedState = Arc::new(RwLock::new(state));

    match peer_loop_from_url(&config.client.url, state).await {
        Ok(()) => Ok(()),
        Err(e) => Err(anyhow!("Broker peer loop error: {e}")),
    }
}

type BoxedFrameReader = Box<dyn FrameReader + Unpin + Send>;
type BoxedFrameWriter = Box<dyn FrameWriter + Unpin + Send>;

async fn login(url: &Url) -> anyhow::Result<(BoxedFrameReader, BoxedFrameWriter)> {
    // Establish a connection
    debug!("Connecting to: {url}");
    let mut reset_session = false;
    let (mut frame_reader, mut frame_writer) = match url.scheme() {
        "tcp" => {
            let address = format!(
                "{}:{}",
                url.host_str().unwrap_or("localhost"),
                url.port().unwrap_or(3755)
            );
            let stream = TcpStream::connect(&address).await?;
            let (reader, writer) = stream.split();
            let brd = BufReader::new(reader);
            let bwr = BufWriter::new(writer);
            let frame_reader: BoxedFrameReader = Box::new(StreamFrameReader::new(brd));
            let frame_writer: BoxedFrameWriter = Box::new(StreamFrameWriter::new(bwr));
            (frame_reader, frame_writer)
        }
        s => {
            panic!("Scheme {s} is not supported")
        }
    };

    // login
    let (user, password) = login_from_url(url);
    let login_params = LoginParams {
        user,
        password,
        ..Default::default()
    };
    client::login(&mut *frame_reader, &mut *frame_writer, &login_params, reset_session).await?;
    debug!("Connected to broker.");
    Ok((frame_reader, frame_writer))
}

async fn peer_loop_from_url(url: &url::Url, state: SharedState) -> anyhow::Result<()> {
    // Establish a connection
    info!("Connecting to broker TCP peer: {url}");
    let (reader, writer) = login(url).await?;

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

async fn broker_peer_loop(url: &url::Url, mut frame_reader: impl FrameReader + Send, mut frame_writer: impl FrameWriter + Send, state: SharedState) -> anyhow::Result<()> {
    // login
    let (user, password) = login_from_url(url);
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
                break 'result process_dir_ls(method, param, PUBLIC_DIR_LS_METHODS.iter(), [EVENT_DIR].iter().copied());
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
