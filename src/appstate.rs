use shvclient::AppState;
use tokio::sync::RwLock;

pub type QxAppState = crate::sql_impl::DbPool;
pub type QxLockedAppState = RwLock<QxAppState>;
pub type QxSharedAppState = AppState<QxLockedAppState>;
