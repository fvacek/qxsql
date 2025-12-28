use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use shvproto::RpcValue;
use shvrpc::client::ClientConfig;
use url::Url;


#[derive(Debug, Serialize, Deserialize)]
#[derive(Default)]
pub struct Config {
    pub client: ClientConfig,
    pub db: DbConfig,
    pub access: Option<DbAccess>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DbConfig {
    pub url: Url,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            url: Url::parse("sqlite::memory:").unwrap(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct DbAccess {
    pub tokens: BTreeMap<String, Token>,
    pub roles: BTreeMap<String, Role>,
}
impl DbAccess {
    pub fn is_authorized(&self, access_token: &str, table_name: &str, op: AccessOp) -> bool {
        if let Some(token) = self.tokens.get(access_token) {
            let op_c: char = op.into();

            for role in token.roles.iter() {
                if let Some(role) = self.roles.get(role) {
                    let rules = role.access.tables.get(table_name)
                        .unwrap_or(&role.access.fallback);

                    if rules.contains(op_c) {
                        return true;
                    }
                }
            }
        }
        false
    }
}

impl TryFrom<&RpcValue> for DbAccess {
    type Error = String;

    fn try_from(value: &RpcValue) -> Result<Self, Self::Error> {
        let dbaccess: DbAccess = shvproto::from_rpcvalue(value)
            .map_err(|e| e.to_string())?;
        Ok(dbaccess)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Token {
    pub roles: Vec<String>,
}
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct Role {
    pub access: TableAccess,
}
#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct TableAccess {
    pub tables: BTreeMap<String, String>,
    pub fallback: String,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq)]
pub enum AccessOp {
    Create,
    Read,
    Update,
    Delete,
    Exec,
    Query,
}
impl From<AccessOp> for char {
    fn from(op: AccessOp) -> Self {
        match op {
            AccessOp::Create => 'C',
            AccessOp::Read => 'R',
            AccessOp::Update => 'U',
            AccessOp::Delete => 'D',
            AccessOp::Exec => 'E',
            AccessOp::Query => 'Q',
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn create_test_db_access() -> DbAccess {
        let mut tokens = BTreeMap::new();
        let mut roles = BTreeMap::new();

        // Create a role with table-specific permissions
        let mut admin_tables = BTreeMap::new();
        admin_tables.insert("users".to_string(), "CRUD".to_string());
        admin_tables.insert("orders".to_string(), "RU".to_string());
        
        let admin_role = Role {
            access: TableAccess {
                tables: admin_tables,
                fallback: "R".to_string(),
            },
        };
        roles.insert("admin".to_string(), admin_role);

        // Create a read-only role
        let readonly_role = Role {
            access: TableAccess {
                tables: BTreeMap::new(),
                fallback: "R".to_string(),
            },
        };
        roles.insert("readonly".to_string(), readonly_role);

        // Create a role with no permissions
        let no_access_role = Role {
            access: TableAccess {
                tables: BTreeMap::new(),
                fallback: "".to_string(),
            },
        };
        roles.insert("no_access".to_string(), no_access_role);

        // Create tokens
        let admin_token = Token {
            roles: vec!["admin".to_string()],
        };
        tokens.insert("admin_token".to_string(), admin_token);

        let readonly_token = Token {
            roles: vec!["readonly".to_string()],
        };
        tokens.insert("readonly_token".to_string(), readonly_token);

        let multi_role_token = Token {
            roles: vec!["readonly".to_string(), "admin".to_string()],
        };
        tokens.insert("multi_role_token".to_string(), multi_role_token);

        let no_access_token = Token {
            roles: vec!["no_access".to_string()],
        };
        tokens.insert("no_access_token".to_string(), no_access_token);

        DbAccess { tokens, roles }
    }

    #[test]
    fn test_is_authorized_valid_token_table_specific_permission() {
        let db_access = create_test_db_access();
        
        // Admin token should have CRUD access to users table
        assert!(db_access.is_authorized("admin_token", "users", AccessOp::Create));
        assert!(db_access.is_authorized("admin_token", "users", AccessOp::Read));
        assert!(db_access.is_authorized("admin_token", "users", AccessOp::Update));
        assert!(db_access.is_authorized("admin_token", "users", AccessOp::Delete));
        
        // Admin token should have limited access to orders table
        assert!(!db_access.is_authorized("admin_token", "orders", AccessOp::Create));
        assert!(db_access.is_authorized("admin_token", "orders", AccessOp::Read));
        assert!(db_access.is_authorized("admin_token", "orders", AccessOp::Update));
        assert!(!db_access.is_authorized("admin_token", "orders", AccessOp::Delete));
    }

    #[test]
    fn test_is_authorized_fallback_permission() {
        let db_access = create_test_db_access();
        
        // Admin token should fall back to Read permission for unknown tables
        assert!(!db_access.is_authorized("admin_token", "unknown_table", AccessOp::Create));
        assert!(db_access.is_authorized("admin_token", "unknown_table", AccessOp::Read));
        assert!(!db_access.is_authorized("admin_token", "unknown_table", AccessOp::Update));
        assert!(!db_access.is_authorized("admin_token", "unknown_table", AccessOp::Delete));
    }

    #[test]
    fn test_is_authorized_readonly_role() {
        let db_access = create_test_db_access();
        
        // Readonly token should only have read access
        assert!(!db_access.is_authorized("readonly_token", "users", AccessOp::Create));
        assert!(db_access.is_authorized("readonly_token", "users", AccessOp::Read));
        assert!(!db_access.is_authorized("readonly_token", "users", AccessOp::Update));
        assert!(!db_access.is_authorized("readonly_token", "users", AccessOp::Delete));
        assert!(!db_access.is_authorized("readonly_token", "users", AccessOp::Exec));
        assert!(!db_access.is_authorized("readonly_token", "users", AccessOp::Query));
    }

    #[test]
    fn test_is_authorized_multiple_roles() {
        let db_access = create_test_db_access();
        
        // Multi-role token should have admin permissions (most permissive role wins)
        assert!(db_access.is_authorized("multi_role_token", "users", AccessOp::Create));
        assert!(db_access.is_authorized("multi_role_token", "users", AccessOp::Read));
        assert!(db_access.is_authorized("multi_role_token", "users", AccessOp::Update));
        assert!(db_access.is_authorized("multi_role_token", "users", AccessOp::Delete));
    }

    #[test]
    fn test_is_authorized_invalid_token() {
        let db_access = create_test_db_access();
        
        // Invalid token should have no access
        assert!(!db_access.is_authorized("invalid_token", "users", AccessOp::Read));
        assert!(!db_access.is_authorized("invalid_token", "users", AccessOp::Create));
        assert!(!db_access.is_authorized("invalid_token", "users", AccessOp::Update));
        assert!(!db_access.is_authorized("invalid_token", "users", AccessOp::Delete));
    }

    #[test]
    fn test_is_authorized_no_access_role() {
        let db_access = create_test_db_access();
        
        // No access token should have no permissions
        assert!(!db_access.is_authorized("no_access_token", "users", AccessOp::Read));
        assert!(!db_access.is_authorized("no_access_token", "users", AccessOp::Create));
        assert!(!db_access.is_authorized("no_access_token", "users", AccessOp::Update));
        assert!(!db_access.is_authorized("no_access_token", "users", AccessOp::Delete));
    }

    #[test]
    fn test_is_authorized_empty_db_access() {
        let db_access = DbAccess::default();
        
        // Empty DbAccess should deny all requests
        assert!(!db_access.is_authorized("any_token", "any_table", AccessOp::Read));
        assert!(!db_access.is_authorized("any_token", "any_table", AccessOp::Create));
        assert!(!db_access.is_authorized("any_token", "any_table", AccessOp::Update));
        assert!(!db_access.is_authorized("any_token", "any_table", AccessOp::Delete));
    }

    #[test]
    fn test_access_op_to_char_conversion() {
        assert_eq!(char::from(AccessOp::Create), 'C');
        assert_eq!(char::from(AccessOp::Read), 'R');
        assert_eq!(char::from(AccessOp::Update), 'U');
        assert_eq!(char::from(AccessOp::Delete), 'D');
        assert_eq!(char::from(AccessOp::Exec), 'E');
        assert_eq!(char::from(AccessOp::Query), 'Q');
    }

    #[test]
    fn test_is_authorized_exec_and_query_operations() {
        let mut tokens = BTreeMap::new();
        let mut roles = BTreeMap::new();

        let mut exec_tables = BTreeMap::new();
        exec_tables.insert("procedures".to_string(), "EQ".to_string());
        
        let exec_role = Role {
            access: TableAccess {
                tables: exec_tables,
                fallback: "".to_string(),
            },
        };
        roles.insert("exec_role".to_string(), exec_role);

        let exec_token = Token {
            roles: vec!["exec_role".to_string()],
        };
        tokens.insert("exec_token".to_string(), exec_token);

        let db_access = DbAccess { tokens, roles };

        // Test Exec and Query operations
        assert!(db_access.is_authorized("exec_token", "procedures", AccessOp::Exec));
        assert!(db_access.is_authorized("exec_token", "procedures", AccessOp::Query));
        assert!(!db_access.is_authorized("exec_token", "procedures", AccessOp::Read));
        assert!(!db_access.is_authorized("exec_token", "procedures", AccessOp::Create));
    }
}
