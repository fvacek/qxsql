use std::sync::OnceLock;

use qxsqld::{
    sql::SqlProvider, QueryAndParams, QueryAndParamsList, RecChng, RecDeleteParam, RecInsertParam, RecOp, RecReadParam, RecUpdateParam
};
use appstate::{QxLockedAppState, QxSharedAppState};
use sql_impl::{QxSql, DbPool, sql_exec_transaction};

use shvclient::appnodes::DotAppNode;

use shvrpc::{
    rpcmessage::{RpcError, RpcErrorCode},
};
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use tokio::sync::RwLock;

use clap::Parser;
use log::*;
use shvclient::AppState;
use shvproto::{FromRpcValue, RpcValue, ToRpcValue, to_rpcvalue};
use shvrpc::util::parse_log_verbosity;
use simple_logger::SimpleLogger;
use url::Url;


mod appstate;
mod config;
mod sql_impl;

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
    #[arg(
        short,
        long,
        help = "Database connection string, examle: postgres://myuser:mypassword@localhost/mydb?options=--search_path%3Dmyschema"
    )]
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
async fn main() -> shvrpc::Result<()> {
    let cli_opts = Opts::parse();
    init_logger(&cli_opts);

    let mut config = if let Some(config_path) = cli_opts.config {
        info!("Loading config file {config_path}");
        let f = std::fs::File::open(config_path)?;
        serde_yaml::from_reader(f)?
    } else {
        config::Config::default()
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
        return Ok(());
    }

    log::info!("=====================================================");
    log::info!("{} starting", std::module_path!());
    log::info!("=====================================================");

    // info!("Heart beat interval: {:?}", client_config.heartbeat_interval);
    info!("Connecting to app database: {}", config.db.url);
    let db = if config.db.url.scheme() == "sqlite" {
        DbPool::Sqlite(
            SqlitePoolOptions::new()
                .connect(config.db.url.as_str())
                .await?,
        )
    } else {
        DbPool::Postgres(PgPoolOptions::new().connect(config.db.url.as_str()).await?)
    };

    let app_state: QxSharedAppState = AppState::new(RwLock::new(db));
    let app_state2 = app_state.clone();

    // let app_tasks = move |_client_cmd_tx, _client_evt_rx| {
    //     tokio::task::spawn(emit_chng_task(client_cmd_tx, client_evt_rx, app_state));
    // };

    // Initialize global configuration
    init_global_config(cli_opts.write_database_token.clone())
        .expect("Global config should only be set once");

    let sql_node = shvclient::fixed_node!(
        sql_handler<QxLockedAppState>(request, client_cmd_tx, app_state) {
            "select" [None, Read, "[s:query,{s|i|b|t|n}:params]", "{{s:name}:fields,[[s|i|b|t|n]]:rows}"] (query: QueryAndParams) => {
                let mut resp = request.prepare_response().unwrap_or_default();
                tokio::task::spawn(async move {
                    let qxsql = QxSql(app_state);
                    let result = qxsql.query(query.query(), query.params()).await;
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
                if let Err(auth_error) = check_write_authorization(&request) {
                    return Some(Err(auth_error));
                }
                tokio::task::spawn(async move {
                    let qxsql = QxSql(app_state);
                    let result = qxsql.exec(query.query(), query.params()).await;
                    match result {
                        Ok(result) => resp.set_result(to_rpcvalue(&result).expect("serde should work")),
                        Err(e) => resp.set_error(RpcError::new(RpcErrorCode::MethodCallException, format!("SQL error: {}", e))),
                    };
                    if let Err(e) = client_cmd_tx.send_message(resp) {
                        error!("sql_exec: Cannot send response ({e})");
                    }
                });
                None
            }
            "transaction" [None, Read, "[s:query,[[s|i|b|t|n]]:params]", "{{s:name}:fields,[[s|i|b|t|n]]:rows}"] (query: QueryAndParamsList) => {
                let mut resp = request.prepare_response().unwrap_or_default();
                if let Err(auth_error) = check_write_authorization(&request) {
                    return Some(Err(auth_error));
                }
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
            "create" [None, Write, "{s:table,{s|i|b|t|n}:record,s:issuer}", "i"] (param: RecInsertParam) => {
                let mut resp = request.prepare_response().unwrap_or_default();
                tokio::task::spawn(async move {
                    let qxsql = QxSql(app_state);
                    let result = qxsql.create_record(&param.table, &param.record).await;
                    let insert_id = match result {
                        Ok(result) => {
                            resp.set_result(to_rpcvalue(&result).expect("serde should work"));
                            Some(result)
                        },
                        Err(e) => {
                            resp.set_error(RpcError::new(RpcErrorCode::MethodCallException, format!("Update record error: {}", e)));
                            None
                        }
                    };
                    client_cmd_tx.send_message(resp).unwrap_or_else(|err| log::error!("sql_select: Cannot send response ({err})"));
                    if let Some(insert_id) = insert_id {
                        let recchng = RecChng {table:param.table, id:insert_id, record:Some(param.record), op: RecOp::Insert, issuer:param.issuer };
                        let rec = to_rpcvalue(&recchng).expect("serde should work");
                        client_cmd_tx.send_message(shvrpc::RpcMessage::new_signal("sql", "recchng", Some(rec)))
                                        .unwrap_or_else(|err| log::error!("Cannot send signal ({err})"));
                    }
                });
                None
            }
            "read" [None, Read, "{s:table,{i}:id}", "{s|i|b|t|n}"] (param: RecReadParam) => {
                let mut resp = request.prepare_response().unwrap_or_default();
                tokio::task::spawn(async move {
                    let qxsql = QxSql(app_state);
                    let result = qxsql.read_record(&param.table, param.id).await;
                    match result {
                        Ok(record) => {
                            let record = to_rpcvalue(&record).expect("serde should work");
                            resp.set_result(record);
                        },
                        Err(e) => {
                            resp.set_error(RpcError::new(RpcErrorCode::MethodCallException, format!("Read record error: {}", e)));
                        }
                    };
                    client_cmd_tx.send_message(resp).unwrap_or_else(|err| log::error!("sql_select: Cannot send response ({err})"));
                });
                None
            }
            "update" [None, Write, "{s:table,i:id,{s|i|b|t|n}:record,s:issuer}", "b"] (param: RecUpdateParam) => {
                let mut resp = request.prepare_response().unwrap_or_default();
                tokio::task::spawn(async move {
                    let qxsql = QxSql(app_state);
                    let result = qxsql.update_record(&param.table, param.id, &param.record).await;
                    let mut send_signal = false;
                    match result {
                        Ok(result) => {
                            send_signal = result;
                            resp.set_result(to_rpcvalue(&result).expect("serde should work"));
                        },
                        Err(e) => {
                            resp.set_error(RpcError::new(RpcErrorCode::MethodCallException, format!("Update record error: {}", e)));
                        }
                    };
                    client_cmd_tx.send_message(resp).unwrap_or_else(|err| log::error!("sql_select: Cannot send response ({err})"));
                    if send_signal {
                        let recchng = RecChng {table:param.table, id:param.id, record:Some(param.record), op: RecOp::Update, issuer:param.issuer };
                        let rec = to_rpcvalue(&recchng).expect("serde should work");
                        client_cmd_tx.send_message(shvrpc::RpcMessage::new_signal("sql", "recchng", Some(rec)))
                                        .unwrap_or_else(|err| log::error!("Cannot send signal ({err})"));
                    }
                });
                None
            }
            "delete" [None, Write, "{s:table,i:id,s:issuer}", "b"] (param: RecDeleteParam) => {
                let mut resp = request.prepare_response().unwrap_or_default();
                tokio::task::spawn(async move {
                    let qxsql = QxSql(app_state);
                    let result = qxsql.delete_record(&param.table, param.id).await;
                    let was_deleted = match result {
                        Ok(result) => {
                            resp.set_result(to_rpcvalue(&result).expect("serde should work"));
                            Some(result)
                        },
                        Err(e) => {
                            resp.set_error(RpcError::new(RpcErrorCode::MethodCallException, format!("Update record error: {}", e)));
                            None
                        }
                    };
                    client_cmd_tx.send_message(resp).unwrap_or_else(|err| log::error!("sql_select: Cannot send response ({err})"));
                    if let Some(was_deleted) = was_deleted && was_deleted {
                        let recchng = RecChng {table:param.table, id:param.id, record:None, op: RecOp::Delete, issuer:param.issuer };
                        let rec = to_rpcvalue(&recchng).expect("serde should work");
                        client_cmd_tx.send_message(shvrpc::RpcMessage::new_signal("sql", "recchng", Some(rec)))
                                        .unwrap_or_else(|err| log::error!("Cannot send signal ({err})"));
                    }
                });
                None
            }
        }
    );

    let dot_app_node = shvclient::fixed_node!(
        sql_handler<QxLockedAppState>(request, _client_cmd_tx, _app_state) {
            "name" [IsGetter, Browse, "n", "s"] => {
                Some(Ok(env!("CARGO_PKG_NAME").into()))
            }
            "version" [IsGetter, Browse, "n", "s"] => {
                Some(Ok(env!("CARGO_PKG_VERSION").into()))
            }
            "ping" [None, Browse, "n", "n"] => {
                Some(Ok(().into()))
            }
        }
    );

    shvclient::Client::new()
        .app(DotAppNode::new(env!("CARGO_PKG_NAME")))
        .mount(".app", dot_app_node)
        .mount("sql", sql_node)
        .with_app_state(app_state2)
        // .run_with_init(&client_config, app_tasks)
        .run(&config.client)
        .await
}

// Global configuration for database access control
#[derive(Debug)]
pub struct GlobalConfig {
    pub write_database_token: Option<String>,
}

static GLOBAL_CONFIG: OnceLock<GlobalConfig> = OnceLock::new();

/// Initialize the global configuration
pub fn init_global_config(write_database_token: Option<String>) -> Result<(), &'static str> {
    GLOBAL_CONFIG
        .set(GlobalConfig {
            write_database_token,
        })
        .map_err(|_| "Global config should only be set once")
}

/// Check write authorization for database operations
fn check_write_authorization(request: &shvrpc::RpcMessage) -> Result<(), shvrpc::rpcmessage::RpcError> {
    use shvrpc::{rpcmessage::{RpcError, RpcErrorCode}, RpcMessageMetaTags};

    let config = GLOBAL_CONFIG
        .get()
        .expect("Global config should be initialized");
    if let Some(write_token) = &config.write_database_token {
        if let Some(user_id) = request.user_id()
            && let Some(user_token) = user_id.split(';').next()
            && user_token != write_token
        {
            return Ok(());
        }

        return Err(RpcError::new(
            RpcErrorCode::PermissionDenied,
            "Unauthorized",
        ));
    }
    Ok(())
}
