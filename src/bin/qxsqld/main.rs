
use qxsql::{
    sql::{QxSqlApi, RecListParam, CREATE_PARAMS, CREATE_RESULT, DELETE_PARAMS, DELETE_RESULT, EXEC_PARAMS, EXEC_RESULT, LIST_PARAMS, LIST_RESULT, QUERY_PARAMS, QUERY_RESULT, READ_PARAMS, READ_RESULT, TRANSACTION_PARAMS, TRANSACTION_RESULT, UPDATE_PARAMS, UPDATE_RESULT}, string_list_to_ref_vec, QueryAndParams, QueryAndParamsList, RecChng, RecDeleteParam, RecInsertParam, RecOp, RecReadParam, RecUpdateParam
};
use sql_impl::{QxSql, DbPool, sql_exec_transaction};
use shvclient::appnodes::{DotAppNode, DotDeviceNode};

use shvrpc::{
    metamethod::AccessLevel, rpcmessage::{RpcError, RpcErrorCode}
};
use sqlx::postgres::PgPoolOptions;
use sqlx::sqlite::SqlitePoolOptions;
use tokio::sync::RwLock;

use clap::Parser;
use log::*;
use shvproto::to_rpcvalue;
use shvrpc::util::parse_log_verbosity;
use simple_logger::SimpleLogger;
use url::Url;

use crate::{appstate::{AppState, SharedAppState}, config::AccessOp, confignode::ConfigNode};


mod appstate;
mod config;
mod sql_impl;
mod confignode;

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

struct SqlNode {
    app_state: SharedAppState,
}

shvclient::impl_static_node! {
    SqlNode(&self, request, client_cmd_tx) {
        "query" [None, Read, QUERY_PARAMS, QUERY_RESULT] (query: QueryAndParams) => {
            if let Err(auth_error) = check_write_authorization(&request, AccessLevel::Read, self.app_state.clone(), "", AccessOp::Read).await {
                return Some(Err(auth_error));
            }
            let mut resp = request.prepare_response().unwrap_or_default();
            let app_state = self.app_state.clone();
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
        "exec" [None, Write, EXEC_PARAMS, EXEC_RESULT] (query: QueryAndParams) => {
            let mut resp = request.prepare_response().unwrap_or_default();
            if let Err(auth_error) = check_write_authorization(&request, AccessLevel::Write, self.app_state.clone(), "", AccessOp::Exec).await {
                return Some(Err(auth_error));
            }
            let app_state = self.app_state.clone();
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
        "transaction" [None, Read, TRANSACTION_PARAMS, TRANSACTION_RESULT] (query: QueryAndParamsList) => {
            if let Err(auth_error) = check_write_authorization(&request, AccessLevel::Write, self.app_state.clone(), "", AccessOp::Exec).await {
                return Some(Err(auth_error));
            }
            let mut resp = request.prepare_response().unwrap_or_default();
            let app_state = self.app_state.clone();
            tokio::task::spawn(async move {
                let db = app_state.read().await.db.clone();
                let result = sql_exec_transaction(db, &query).await;
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
        "list" [None, Read, LIST_PARAMS, LIST_RESULT] (param: RecListParam) => {
            if let Err(auth_error) = check_write_authorization(&request, AccessLevel::Read, self.app_state.clone(), "", AccessOp::Read).await {
                return Some(Err(auth_error));
            }
            let mut resp = request.prepare_response().unwrap_or_default();
            let app_state = self.app_state.clone();
            tokio::task::spawn(async move {
                let qxsql = QxSql(app_state);
                let fields = string_list_to_ref_vec(&param.fields);
                let result = qxsql.list_records(&param.table, fields, param.ids_above, param.limit).await;
                match result {
                    Ok(record) => {
                        let record = to_rpcvalue(&record).expect("serde should work");
                        resp.set_result(record);
                    },
                    Err(e) => {
                        resp.set_error(RpcError::new(RpcErrorCode::MethodCallException, format!("List records error: {}", e)));
                    }
                };
                client_cmd_tx.send_message(resp).unwrap_or_else(|err| log::error!("sql_select: Cannot send response ({err})"));
            });
            None
        }
        "create" [None, Write, CREATE_PARAMS, CREATE_RESULT] (param: RecInsertParam) => {
            if let Err(auth_error) = check_write_authorization(&request, AccessLevel::Write, self.app_state.clone(), &param.table, AccessOp::Create).await {
                return Some(Err(auth_error));
            }
            let mut resp = request.prepare_response().unwrap_or_default();
            let app_state = self.app_state.clone();
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
        "read" [None, Read, READ_PARAMS, READ_RESULT] (param: RecReadParam) => {
            if let Err(auth_error) = check_write_authorization(&request, AccessLevel::Read, self.app_state.clone(), &param.table, AccessOp::Read).await {
                return Some(Err(auth_error));
            }
            let mut resp = request.prepare_response().unwrap_or_default();
            let app_state = self.app_state.clone();
            tokio::task::spawn(async move {
                let qxsql = QxSql(app_state);
                let fields = string_list_to_ref_vec(&param.fields);
                let result = qxsql.read_record(&param.table, param.id, fields).await;
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
        "update" [None, Write, UPDATE_PARAMS, UPDATE_RESULT] (param: RecUpdateParam) => {
            if let Err(auth_error) = check_write_authorization(&request, AccessLevel::Write, self.app_state.clone(), &param.table, AccessOp::Update).await {
                return Some(Err(auth_error));
            }
            let mut resp = request.prepare_response().unwrap_or_default();
            let app_state = self.app_state.clone();
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
        "delete" [None, Write, DELETE_PARAMS, DELETE_RESULT] (param: RecDeleteParam) => {
            if let Err(auth_error) = check_write_authorization(&request, AccessLevel::Write, self.app_state.clone(), &param.table, AccessOp::Delete).await {
                return Some(Err(auth_error));
            }
            let mut resp = request.prepare_response().unwrap_or_default();
            let app_state = self.app_state.clone();
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
        DbPool::Postgres(PgPoolOptions::new()
            .connect(config.db.url.as_str())
            .await?)
    };

    let app_state = SharedAppState::new(RwLock::new(AppState {
        db,
        db_access: config.access.clone(),
    }));

    // let app_tasks = move |_client_cmd_tx, _client_evt_rx| {
    //     tokio::task::spawn(emit_chng_task(client_cmd_tx, client_evt_rx, app_state));
    // };

    shvclient::Client::new()
        .app(DotAppNode::new(env!("CARGO_PKG_NAME")))
        .device(DotDeviceNode::new(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"), Some("00000".into())))
        .mount_static("sql", SqlNode { app_state: app_state.clone() })
        .mount_static("config", ConfigNode { app_state: app_state.clone() })
        .run(&config.client)
        .await
}

/// Check write authorization for database operations
async fn check_write_authorization(
    request: &shvrpc::RpcMessage,
    access_level: AccessLevel,
    app_state: SharedAppState,
    table: &str,
    op: AccessOp,
) -> Result<(), shvrpc::rpcmessage::RpcError> {
    use shvrpc::{rpcmessage::{RpcError, RpcErrorCode}, RpcMessageMetaTags};
    if request.access_level().unwrap_or(0) >= access_level as i32 {
        // access level is set to read to enable user-id driven access control
        // skip user-id check, if request access_level is high enough
        return Ok(());
    }
    let user_id = request.user_id().unwrap_or_default();
    let access_token = split_first_fragment(user_id, ';').0;
    let ok = app_state.read().await.db_access.as_ref().map(|db_access| {
        db_access.is_authorized(access_token, table, op)
    }).unwrap_or(false);
    if !ok {
        return Err(RpcError::new(
            RpcErrorCode::PermissionDenied,
            "Unauthorized",
        ));
    }
    Ok(())
}

fn split_first_fragment(path: &str, sep: char) -> (&str, &str) {
    if let Some(ix) = path.find(sep) {
        let dir = &path[0 .. ix];
        let rest = &path[ix + 1..];
        (dir, rest)
    } else {
        (path, "")
    }
}

// fn split_first_path_fragment(path: &str) -> (&str, &str) {
//     split_first_fragment(path, '/')
// }
