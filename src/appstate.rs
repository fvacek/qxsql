use shvclient::AppState;
use tokio::sync::RwLock;

pub(crate) type QxAppState = crate::sql::DbPool;
pub(crate) type QxLockedAppState = RwLock<QxAppState>;
pub(crate) type QxSharedAppState = AppState<QxLockedAppState>;
