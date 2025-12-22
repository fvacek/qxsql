use std::sync::Arc;
use tokio::sync::RwLock;

pub(crate) type State = super::sql_impl::DbPool;
pub(crate) type AppState = Arc<RwLock<State>>;
