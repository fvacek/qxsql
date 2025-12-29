use std::sync::Arc;
use tokio::sync::{RwLock, oneshot};
use crate::config::DbAccess;

pub(crate) type SharedAppState = Arc<RwLock<AppState>>;

pub(crate) struct AppState {
    pub(crate) db: super::sql_impl::DbPool,
    pub(crate) db_access: Option<DbAccess>,
    pub(crate) shutdown_tx: Option<oneshot::Sender<()>>,
}
