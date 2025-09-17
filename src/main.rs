use crate::{sql::{sql_exec, sql_exec_transaction, sql_select, DbValue, QueryAndParams, QueryAndParamsList, RecChng}, sql_utils::SqlOperation};
use shvclient::appnodes::DotAppNode;
use shvrpc::{rpcmessage::{RpcError, RpcErrorCode}, RpcMessage};
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use tokio::sync::RwLock;

use clap::Parser;
use log::*;
use shvrpc::util::parse_log_verbosity;
use shvclient::AppState;
use simple_logger::SimpleLogger;
use shvproto::{to_rpcvalue, FromRpcValue, RpcValue, ToRpcValue};
use url::Url;

mod sql_utils;
mod config;
mod sql;

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

    /// Database connection string
    #[arg(short, long)]
    write_database_token: Option<String>,

    /// Print effective config
    #[arg(long)]
    print_config: bool,

    /// Verbose mode (module, .)
    #[arg(short, long)]
    verbose: Option<String>,
}

type State = RwLock<crate::sql::DbPool>;

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
        crate::config::Config::default()
    };

    if let Some(url) = cli_opts.url {
        config.client.url = Url::parse(&url)?;
    }
    if let Some(database) = cli_opts.database {
        config.db.url = Url::parse(&database)?;
    }
    if let Some(mount) = cli_opts.mount {
        config.client.mount = Some(mount);
    }

    if cli_opts.print_config {
        let yaml = serde_yaml::to_string(&config)?;
        println!("{}", yaml);
        return Ok(())
    }

    log::info!("=====================================================");
    log::info!("{} starting", std::module_path!());
    log::info!("=====================================================");

    // info!("Heart beat interval: {:?}", client_config.heartbeat_interval);
    info!("Connecting to app database: {}", config.db.url);
    let db = if config.db.url.scheme() == "sqlite" {
        crate::sql::DbPool::Sqlite(SqlitePoolOptions::new().connect(config.db.url.as_str()).await?)
    } else {
        crate::sql::DbPool::Postgres(PgPoolOptions::new().connect(config.db.url.as_str()).await?)
    };

    let app_state = AppState::new(RwLock::new(db));
    let cnt = app_state.clone();

    // let app_tasks = move |_client_cmd_tx, _client_evt_rx| {
    //     tokio::task::spawn(emit_chng_task(client_cmd_tx, client_evt_rx, app_state));
    // };

    let write_database_token = cli_opts.write_database_token.clone();
    let sql_node = shvclient::fixed_node!(
        sql_handler<State>(request, client_cmd_tx, app_state) {
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
                        error!("sql_select: Cannot send response ({e})");
                    }
                });
                None
            }
            "exec" [None, Read, "[s:query,{s|i|b|t|n}:params,s|n:issuer]", "{{s:name}:fields,[[s|i|b|t|n]]:rows}"] (query: QueryAndParams) => {
                let mut resp = request.prepare_response().unwrap_or_default();
                let user_id = request.user_id().map(|id| id.to_string());
                let is_authorized = write_database_token.isNone();
                if !is_authorized {
                    return Some(Err(RpcError::new(RpcErrorCode::PermissionDenied, "Unauthorized")));
                }
                tokio::task::spawn(async move {
                    let state = app_state.read().await;
                    let result = sql_exec(&state, &query).await;
                    let recchng = match result {
                        Ok(result) => {
                            resp.set_result(to_rpcvalue(&result).expect("serde should work"));
                            if query.issuer().is_some() {
                                match result.info.operation {
                                    SqlOperation::Insert => {
                                        let record = query.params().clone();
                                        Some(RecChng {
                                            table: result.info.table_name,
                                            id: result.insert_id,
                                            record: Some(record),
                                            issuer: query.issuer().unwrap_or_default().to_string(),
                                        })
                                    }
                                    SqlOperation::Update => {
                                        let id = if let Some(DbValue::Int(id)) = query.params().get("id") { *id } else { 0 };
                                        let mut record = query.params().clone();
                                        record.remove("id");
                                        Some(RecChng {
                                            table: result.info.table_name,
                                            id,
                                            record: Some(record),
                                            issuer: query.issuer().unwrap_or_default().to_string(),
                                        })
                                    }
                                    SqlOperation::Delete => {
                                        let id = if let Some(DbValue::Int(id)) = query.params().get("id") { *id } else { 0 };
                                        Some(RecChng {
                                            table: result.info.table_name,
                                            id,
                                            record: None,
                                            issuer: query.issuer().unwrap_or_default().to_string(),
                                        })
                                    }
                                }
                            } else {
                                None
                            }
                        }
                        Err(e) => {
                            resp.set_error(RpcError::new(RpcErrorCode::MethodCallException, format!("SQL error: {}", e)));
                            None
                        },
                    };
                    if let Err(e) = client_cmd_tx.send_message(resp) {
                        error!("sql_exec: Cannot send response ({e})");
                    }
                    if let Some(recchng) = recchng {
                        match to_rpcvalue(&recchng) {
                            Ok(rv) => {
                                let msg = RpcMessage::new_signal("sql", "recchng", Some(rv));
                                if let Err(e) = client_cmd_tx.send_message(msg) {
                                    error!("sql_exec: Cannot send signal ({e})");
                                }
                            },
                            Err(e) => {
                                error!("sql_exec: Cannot convert RecChng to RPC value ({e})");
                            }
                        }
                    }
                });
                None
            }
            "transaction" [None, Read, "[s:query,[[s|i|b|t|n]]:params]", "{{s:name}:fields,[[s|i|b|t|n]]:rows}"] (query: QueryAndParamsList) => {
                let mut resp = request.prepare_response().unwrap_or_default();
                tokio::task::spawn(async move {
                    let state = app_state.read().await;
                    let result = sql_exec_transaction(&state, &query).await;
                    match result {
                        Ok(result) => resp.set_result(to_rpcvalue(&result).expect("serde should work")),
                        Err(e) => resp.set_error(RpcError::new(RpcErrorCode::MethodCallException, format!("SQL error: {}", e))),
                    };
                    if let Err(e) = client_cmd_tx.send_message(resp) {
                        error!("sql_transaction: Cannot send response ({e})");
                    }
                });
                None
            }
        }
    );

    shvclient::Client::new()
        .app(DotAppNode::new("qxsql"))
        .mount("sql", sql_node)
        .with_app_state(cnt)
        // .run_with_init(&client_config, app_tasks)
        .run(&config.client)
        .await
}
