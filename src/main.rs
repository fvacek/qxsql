use ::config::File;
use anyhow::anyhow;
use clap::Parser;
use futures::select;
use log::{debug, error, info, warn};
use shvnode::PUBLIC_DIR_LS_METHODS;
use shvproto::List;
use shvrpc::client::{self, LoginParams};
use shvrpc::framerw::{FrameReader, FrameWriter};
use shvrpc::rpcdiscovery::LsParam;
use shvrpc::rpcframe::RpcFrame;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use shvrpc::streamrw::{StreamFrameReader, StreamFrameWriter};
use shvrpc::util::login_from_url;
use shvrpc::{RpcMessage, RpcMessageMetaTags};
use std::backtrace::Backtrace;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Sender};
use tokio::time::sleep;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use futures_util::FutureExt;

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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    let mut settings = ::config::Config::builder();
    if let Some(config_path) = args.config {
        settings = settings.add_source(File::with_name(&config_path));
    }
    let settings = settings.set_default("client.url", "tcp://localhost:3755?user=test&password=password")?;

    let settings = settings.build()?;

    let config: config::Config = settings.try_deserialize()?;

    let url = match args.url {
        Some(url) => url,
        None => config.client.url,
    };

    info!("Connecting to database: {}", args.database.unwrap_or(config.database));
    // let db_pool = AnyPool::connect(&args.database).await?;
    info!("Database connected.");

    let url = url::Url::parse(&url)?;
    broker_peer_loop_from_url(url).await?;

    Ok(())
}

async fn broker_peer_loop_from_url(url: url::Url) -> anyhow::Result<()> {
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
    return broker_peer_loop(url, frame_reader, frame_writer).await
}

fn rpc_to_anyhow(err: shvrpc::Error) -> anyhow::Error {
    error!("RPC Error: {err}\nbacktrace: {}", Backtrace::capture());
    anyhow!("RPC error: {}", err)
}
async fn broker_peer_loop(url: url::Url, mut frame_reader: impl FrameReader + Send, mut frame_writer: impl FrameWriter + Send) -> anyhow::Result<()> {
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
                    process_broker_client_peer_frame(frame, frame_sender.clone()).await?;
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

async fn process_broker_client_peer_frame(frame: RpcFrame, sender: Sender<RpcFrame>) -> anyhow::Result<()> {
    if frame.is_request() {
        process_request(frame, sender).await?;
    } else if frame.is_response() {
        warn!("RPC response should not be received from client connection to parent broker: {}", &frame);
    } else {
        warn!("RPC signal should not be received from client connection to parent broker: {}", &frame);
    }
    Ok(())
}

async fn process_request(frame: RpcFrame, sender: Sender<RpcFrame>) -> anyhow::Result<()> {
    debug!("Processing frame from broker: {frame:?}");
    assert!(frame.is_request());
    let rpcmsg = frame.to_rpcmesage().map_err(rpc_to_anyhow)?;
    let method = frame.method().ok_or_else(|| anyhow!("Request without method"))?;
    let result = match method {
        "dir" => {
            let dir = shvnode::dir(PUBLIC_DIR_LS_METHODS.iter(), rpcmsg.param().into());
            Ok(dir)
        }
        "ls" => {
            match LsParam::from(rpcmsg.param()) {
                LsParam::List => {
                    let list = List::new();
                    Ok(list.into())
                }
                LsParam::Exists(_) => {
                    Ok(false.into())
                }
            }
        }
        unknown_method => {
            Err(anyhow!("Unknown method: {unknown_method}"))
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
