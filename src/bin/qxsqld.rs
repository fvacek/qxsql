use qxsqld::sql::{sql_select, QueryAndParams};
use shvclient::appnodes::DotAppNode;
use shvrpc::rpcmessage::{RpcError, RpcErrorCode};
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use tokio::sync::RwLock;

use clap::Parser;
use futures::{select, FutureExt, StreamExt};
use log::*;
use shvrpc::{client::ClientConfig, util::parse_log_verbosity};
use shvrpc::RpcMessage;
use shvclient::clientnode::SIG_CHNG;
use shvclient::{ClientCommandSender, ClientEvent, ClientEventsReceiver, AppState};
use simple_logger::SimpleLogger;
use shvproto::{to_rpcvalue, FromRpcValue, RpcValue, ToRpcValue};
use url::Url;


#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Opts {
    /// Path to config file
    #[arg(short, long)]
    config: Option<String>,

    /// SHV broker URL
    #[arg(short = 's', long)]
    url: Option<String>,

    /// Mount point on broker connected to, note that broker might not accept any path.
    #[arg(short, long)]
    mount: Option<String>,

    /// Database connection string
    #[arg(short, long)]
    database: Option<String>,

    /// Print effective config
    #[arg(long)]
    print_config: bool,

    /// Verbose mode (module, .)
    #[arg(short, long)]
    verbose: Option<String>,
}

// struct State {
//     db: crate::sql::DbPool,
// }
type State = RwLock<qxsqld::sql::DbPool>;

fn init_logger(cli_opts: &Opts) {
    let mut logger = SimpleLogger::new();
    logger = logger.with_level(LevelFilter::Info);
    if let Some(module_names) = &cli_opts.verbose {
        for (module, level) in parse_log_verbosity(module_names, module_path!()) {
            if let Some(module) = module {
                logger = logger.with_module_level(module, level);
            } else {
                logger = logger.with_level(level);
            }
        }
    }
    logger.init().unwrap();
}

async fn emit_chng_task(
    client_cmd_tx: ClientCommandSender<State>,
    client_evt_rx: ClientEventsReceiver,
    _app_state: AppState<State>,
) -> shvrpc::Result<()> {
    info!("signal task started");
    let mut client_evt_rx = client_evt_rx.fuse();
    let mut cnt = 0;
    let mut emit_signal = true;
    loop {
        select! {
            rx_event = client_evt_rx.next() => match rx_event {
                Some(ClientEvent::ConnectionFailed(_)) => {
                    info!("Connection failed");
                }
                Some(ClientEvent::Connected(_)) => {
                    emit_signal = true;
                    info!("Device connected");

                    client_cmd_tx.mount_node("onfly", shvclient::fixed_node! {
                        device_handler<State>(request, _tx ) {
                            "echo" [IsGetter, Browse, "", ""] (param: RpcValue) => {
                                println!("echo: {param}");
                                Some(Ok(param))
                            }
                        }
                    });
                },
                Some(ClientEvent::Disconnected) => {
                    emit_signal = false;
                    info!("Device disconnected");
                },
                None => break,
            },
            _ = tokio::time::sleep(std::time::Duration::from_secs(3)).fuse() => { }

        }
        if emit_signal {
            let sig = RpcMessage::new_signal("status/delayed", SIG_CHNG, Some(cnt.into()));
            client_cmd_tx.send_message(sig)?;
            info!("signal task emits a value: {cnt}");
            cnt += 1;
        }
        // let state = app_state.read().await;
        // info!("state: {state}");
        if cnt == 10 {
            client_cmd_tx.unmount_node("onfly");
        }
        if cnt == 20 {
            client_cmd_tx.terminate_client();
        }
    }
    info!("signal task finished");
    Ok(())
}

#[derive(Default, Clone, FromRpcValue, ToRpcValue)]
struct CustomParam {
    data: Vec<String>,
    data2: Vec<RpcValue>,
}

#[tokio::main]
pub(crate) async fn main() -> shvrpc::Result<()> {
    let cli_opts = Opts::parse();
    init_logger(&cli_opts);

    let mut config = if let Some(config_path) = cli_opts.config {
        info!("Loading config file {config_path}");
        let f = std::fs::File::open(config_path)?;
        serde_yaml::from_reader(f)?
    } else {
        qxsqld::config::Config::default()
    };

    if let Some(url) = cli_opts.url {
        config.client.url = Url::parse(&url)?;
    }
    if let Some(database) = cli_opts.database {
        config.db.url = Url::parse(&database)?;
    }

    if cli_opts.print_config {
        let yaml = serde_yaml::to_string(&config)?;
        println!("{}", yaml);
        return Ok(())
    }

    let client_config = ClientConfig {
        url: config.client.url.clone(),
        mount: config.client.mount.clone(),
        ..Default::default()
    };

    log::info!("=====================================================");
    log::info!("{} starting", std::module_path!());
    log::info!("=====================================================");

    info!("Connecting to app database: {}", config.db.url);
    let db = if config.db.url.scheme() == "sqlite" {
        qxsqld::sql::DbPool::Sqlite(SqlitePoolOptions::new().connect(config.db.url.as_str()).await?)
    } else {
        qxsqld::sql::DbPool::Postgres(PgPoolOptions::new().connect(config.db.url.as_str()).await?)
    };

    let counter = AppState::new(RwLock::new(db));
    let cnt = counter.clone();

    let app_tasks = move |client_cmd_tx, client_evt_rx| {
        tokio::task::spawn(emit_chng_task(client_cmd_tx, client_evt_rx, counter));
    };

    let sql_node = shvclient::fixed_node!(
        sql_select_handler<State>(request, client_cmd_tx, app_state) {
            "select" [None, Read, "[s:query,{s|i|b|t|n}:params]", "{{s:name}:fields,[[s|i|b|t|n]]:rows}"] (query: QueryAndParams) => {
                let mut resp = request.prepare_response().unwrap_or_default();
                tokio::task::spawn(async move {
                    let state = app_state.read().await;
                    let result = sql_select(&state, &query).await;
                    match result {
                        Ok(result) => resp.set_result(to_rpcvalue(&result).expect("serde should work")),
                        Err(e) => resp.set_error(RpcError::new(RpcErrorCode::MethodCallException, format!("SQL error: {}", e))),
                    };
                    if let Err(e) = client_cmd_tx.send_message(resp) {
                        error!("sql_select_handler: Cannot send response ({e})");
                    }
                });
                None
            }
        }
    );

    shvclient::Client::new()
        .app(DotAppNode::new("simple_device_tokio"))
        .mount("status/delayed", sql_node)
        .with_app_state(cnt)
        .run_with_init(&client_config, app_tasks)
        // .run(&client_config)
        .await
}
