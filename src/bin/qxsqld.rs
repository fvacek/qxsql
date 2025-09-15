use qxsqld::sql::{sql_exec, sql_exec_transaction, sql_select, DbValue, QueryAndParams, QueryAndParamsList, RecChng};
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
        qxsqld::sql::DbPool::Sqlite(SqlitePoolOptions::new().connect(config.db.url.as_str()).await?)
    } else {
        qxsqld::sql::DbPool::Postgres(PgPoolOptions::new().connect(config.db.url.as_str()).await?)
    };

    let app_state = AppState::new(RwLock::new(db));
    let cnt = app_state.clone();

    // let app_tasks = move |_client_cmd_tx, _client_evt_rx| {
    //     tokio::task::spawn(emit_chng_task(client_cmd_tx, client_evt_rx, app_state));
    // };

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
                tokio::task::spawn(async move {
                    let state = app_state.read().await;
                    let result = sql_exec(&state, &query).await;
                    let signal = match result {
                        Ok(result) => {
                            resp.set_result(to_rpcvalue(&result).expect("serde should work"));
                            if let Some(sql_info) = result.info {
                                if sql_info.is_insert() {
                                    let id = result.insert_id;
                                    let rec = query.params();
                                    Some((sql_info, id, rec))
                                } else {
                                    let id = if let Some(DbValue::Int(id)) = query.params().get("id") { *id } else { 0 };
                                    let rec = query.params();
                                    Some((sql_info, id, rec))
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
                    if let Some((info, id, rec)) = signal && let Some(issuer) = query.issuer() {
                        let recchng = RecChng {table:info.table_name,id,record:rec.clone(), issuer: issuer.to_string() };
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
