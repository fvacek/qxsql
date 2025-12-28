use shvrpc::rpcmessage::{RpcError, RpcErrorCode};

use crate::{appstate::SharedAppState, config::DbAccess};

pub struct ConfigNode {
    pub(crate) app_state: SharedAppState,
}

shvclient::impl_static_node! {
    ConfigNode(&self, request, _client_cmd_tx) {
        "dbAccess" [None, Read, "", "!DbAccess"] => {
            let db_access = &self.app_state.read().await.db_access;
            let rv = shvproto::to_rpcvalue(db_access)
                .map_err(|e| RpcError::new(RpcErrorCode::MethodCallException, format!("Failed to serialize DbAccess: {}", e)));
            Some(rv)
        }
        "setDbAccess" [None, Read, "!DbAccess", ""] (db_access: DbAccess) => {
            self.app_state.write().await.db_access = Some(db_access);
            Some(Ok(().into()))
        }
    }
}
