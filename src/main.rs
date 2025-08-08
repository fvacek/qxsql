use clap::Parser;
use futures::select;
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
use log::{debug, error, info, warn};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, Sender};
use tokio::time::sleep;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use std::backtrace::Backtrace;
use futures_util::FutureExt;
use anyhow::anyhow;

mod shvnode;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// SHV broker URL
    #[arg(short, long)]
    url: String,

    /// Database connection string
    #[arg(short, long)]
    database: String,

    /// SHV path to mount the service on
    #[arg(short, long, default_value = "db")]
    path: String,
}

// async fn rpc_task(
//     mut rpc_connection: shvrpc::transport::TcpRpcConnection,
//     db_pool: AnyPool,
//     mount_path: &str,
// ) -> anyhow::Result<()> {
//     rpc_connection.login("admin", "admin").await?;
//     info!("Logged in to shv broker.");

//     let (mut rpc_writer, mut rpc_reader) = rpc_connection.split();
//     let pending_calls = Arc::new(Mutex::new(BTreeMap::new()));

//     let mount_point = format!("{}/{}", mount_path, "master");
//     let mut frame = RpcFrame::new_request(mount_point.clone(), "mount", None);
//     frame.set_access_level("su");
//     rpc_writer.send_frame(frame).await?;

//     let db_pool = Arc::new(db_pool);

//     loop {
//         tokio::select! {
//             Some(frame) = rpc_reader.next() => {
//                 if let Some(req) = frame.as_request() {
//                     let shv_path = req.shv_path().unwrap_or_default();
//                     let method = req.method().unwrap_or_default();
//                     info!("SHV call received: {}->{}", shv_path, method);

//                     let response = RpcMessage::new_response(req.request_id().unwrap_or_default());
//                     let db_pool = db_pool.clone();
//                     let pending_calls = pending_calls.clone();

//                     let fut: Pin<Box<dyn Future<Output = RpcMessage> + Send>> = match method {
//                         "exec" => {
//                             Box::pin(async move {
//                                 let params = req.params().unwrap_or_default();
//                                 let sql = params.as_str();
//                                 match db_pool.execute(sql).await {
//                                     Ok(result) => response.with_result(result.rows_affected()),
//                                     Err(e) => response.with_error(e.to_string()),
//                                 }
//                             })
//                         }
//                         "select" => {
//                             Box::pin(async move {
//                                 let params = req.params().unwrap_or_default();
//                                 let sql = params.as_str();
//                                 match db_pool.fetch_all(sql).await {
//                                     Ok(rows) => {
//                                         let mut result_rows = vec![];
//                                         for row in rows {
//                                             let mut result_row = vec![];
//                                             for (i, col) in row.columns().iter().enumerate() {
//                                                 let value: RpcValue = match col.type_info().name() {
//                                                     "TEXT" | "VARCHAR" => row.get::<String, _>(i).into(),
//                                                     "INTEGER" | "INT" => row.get::<i64, _>(i).into(),
//                                                     "REAL" | "FLOAT" | "DOUBLE" => row.get::<f64, _>(i).into(),
//                                                     "BOOLEAN" | "BOOL" => row.get::<bool, _>(i).into(),
//                                                     "NULL" => RpcValue::from(()),
//                                                     _ => RpcValue::from(row.get::<String, _>(i)),
//                                                 };
//                                                 result_row.push(value);
//                                             }
//                                             result_rows.push(RpcValue::from(result_row));
//                                         }
//                                         response.with_result(RpcValue::from(result_rows))
//                                     }
//                                     Err(e) => response.with_error(e.to_string()),
//                                 }
//                             })
//                         }
//                         _ => {
//                             Box::pin(async move {
//                                 response.with_error(format!("Unknown method: {}", method))
//                             })
//                         }
//                     };
//                     pending_calls.lock().unwrap().insert(req.request_id().unwrap(), fut);
//                 } else if let Some(resp) = frame.as_response() {
//                     info!("SHV response received: {:?}", resp);
//                 } else if let Some(signal) = frame.as_signal() {
//                     info!("SHV signal received: {:?}", signal);
//                 }
//             },
//         }
//         let mut finished_calls = vec![];
//         for (req_id, fut) in pending_calls.lock().unwrap().iter_mut() {
//             if let Some(response) = fut.now_or_never() {
//                 rpc_writer.send_message(response).await?;
//                 finished_calls.push(*req_id);
//             }
//         }
//         for req_id in finished_calls {
//             pending_calls.lock().unwrap().remove(&req_id);
//         }
//     }
// }

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    info!("Connecting to database: {}", args.database);
    // let db_pool = AnyPool::connect(&args.database).await?;
    info!("Database connected.");

    let url = url::Url::parse(&args.url)?;
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
            resp_frame = fut_resp_frame => match resp_frame {
                Some(frame) => {
                    process_broker_client_peer_frame(frame, frame_sender.clone()).await?;
                    drop(fut_resp_frame);
                    fut_resp_frame = Box::pin(frame_receiver.recv()).fuse();
                }
                None => {
                    return Err(anyhow!("Read RESP frame error"));
                }
            }
        }
    };
    Ok(())
}

async fn process_broker_client_peer_frame(frame: RpcFrame, sender: Sender<RpcFrame>) -> anyhow::Result<()> {
    if frame.is_request() {
        process_request(frame, sender).await?;
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
