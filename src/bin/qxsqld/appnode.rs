
use std::sync::OnceLock;

use async_trait::async_trait;
use shvclient::ClientCommandSender;
use shvclient::appnodes::{DOT_APP_METHODS, DotAppNode};
use shvclient::clientnode::StaticNode;
use shvrpc::metamethod::{AccessLevel, Flag, MetaMethod};
use shvrpc::rpcmessage::RpcErrorCode;
use shvrpc::{RpcMessageMetaTags, RpcMessage, rpcmessage::RpcError};
use shvproto::RpcValue;

use crate::appstate::SharedAppState;
use crate::config::DbAccess;

pub struct AppNode {
    app_state: SharedAppState,
    dot_app_node: DotAppNode,
}

impl AppNode {
    pub fn new(app_name: impl Into<String>, app_state: SharedAppState) -> Self {
        Self {
            app_state,
            dot_app_node: DotAppNode::new(app_name),
        }
    }
}

static METHODS: OnceLock<Vec<MetaMethod>> = OnceLock::new();

fn get_methods() -> &'static [MetaMethod] {
    METHODS.get_or_init(|| {
        let mut mm = Vec::from_iter(DOT_APP_METHODS.iter().cloned());
        mm.extend_from_slice(APP_METHODS);
        mm
    })
}

const METH_DB_ACCESS: &str = "dbAccess";
const METH_SET_DB_ACCESS: &str = "setDbAccess";
const METH_QUIT: &str = "quit";

pub const APP_METHODS: &[MetaMethod] = &[
    MetaMethod::new_static(
        METH_DB_ACCESS, Flag::None as u32, AccessLevel::Read, "", "!DbAccess", &[], "",
    ),
    MetaMethod::new_static(
        METH_SET_DB_ACCESS, Flag::None as u32, AccessLevel::Config, "!DbAccess", "", &[], "",
    ),
    MetaMethod::new_static(
        METH_QUIT, Flag::None as u32, AccessLevel::Write, "", "", &[], "",
    ),
];

#[async_trait]
impl StaticNode for AppNode {
    fn methods(&self) -> &'static [MetaMethod] {
        get_methods()
    }

    async fn process_request(&self, request: RpcMessage, client_command_sender: ClientCommandSender) -> Option<Result<RpcValue, RpcError>> {
        match request.method() {
            Some(METH_DB_ACCESS) => {
                let db_access = &self.app_state.read().await.db_access;
                let rv = shvproto::to_rpcvalue(db_access)
                    .map_err(|e| RpcError::new(RpcErrorCode::MethodCallException, format!("Failed to serialize DbAccess: {}", e)));
                Some(rv)
            }
            Some(METH_SET_DB_ACCESS) => {
                match shvproto::from_rpcvalue::<DbAccess>(request.param().unwrap_or_default())
                    .map_err(|e| RpcError::new(RpcErrorCode::MethodCallException, format!("Failed to deserialize DbAccess: {}", e))) {
                    Ok(db_access) => {
                        self.app_state.write().await.db_access = Some(db_access);
                        Some(Ok(().into()))
                    }
                    Err(err) => Some(Err(err)),
                }
            }
            Some(METH_QUIT) => {
                log::info!("Exit method called, initiating graceful shutdown");

                // Take the shutdown sender from the app state and trigger shutdown
                if let Some(shutdown_tx) = self.app_state.write().await.shutdown_tx.take() {
                    let _ = shutdown_tx.send(());
                    log::info!("Shutdown signal sent successfully");
                    Some(Ok(true.into()))
                } else {
                    log::warn!("Shutdown already initiated or sender not available");
                    Some(Ok(false.into()))
                }
            }
            _ => self.dot_app_node.process_request(request, client_command_sender).await,
        }
    }
}
