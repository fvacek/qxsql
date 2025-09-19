use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub(crate) fn parse_rfc3339_datetime(s: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SqlOperation {
    Insert,
    Update,
    Delete,
    Other(String)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SqlInfo {
    pub operation: SqlOperation,
    pub table_name: String,
    pub is_returning_id: bool,
}

pub fn parse_sql_info(sql: &str) -> Result<SqlInfo, String> {
    let sql = sql.trim();
    if sql.is_empty() {
        return Err("Empty SQL statement".to_string());
    }

    // Split by whitespace and filter out empty parts
    let parts: Vec<&str> = sql.split_whitespace().collect();
    if parts.len() < 3 {
        return Err("Invalid SQL statement: too few parts".to_string());
    }

    let operation_str = parts[0].to_uppercase();
    let operation = match operation_str.as_str() {
        "INSERT" => SqlOperation::Insert,
        "UPDATE" => SqlOperation::Update,
        "DELETE" => SqlOperation::Delete,
        o => SqlOperation::Other(o.to_string()),
    };

    let table_name = match operation {
        SqlOperation::Insert => {
            // For INSERT: "INSERT INTO table_name ..."
            if parts.len() < 3 {
                return Err("Invalid INSERT statement: missing table name".to_string());
            }
            if parts[1].to_uppercase() != "INTO" {
                return Err("Invalid INSERT statement: expected 'INTO' keyword".to_string());
            }
            parts[2].to_string()
        },
        SqlOperation::Delete => {
            // For DELETE: "DELETE FROM table_name ..."
            if parts.len() < 3 {
                return Err("Invalid DELETE statement: missing table name".to_string());
            }
            if parts[1].to_uppercase() != "FROM" {
                return Err("Invalid DELETE statement: expected 'FROM' keyword".to_string());
            }
            parts[2].to_string()
        },
        SqlOperation::Update => {
            // For UPDATE: "UPDATE table_name ..."
            if parts.len() < 2 {
                return Err("Invalid UPDATE statement: missing table name".to_string());
            }
            // Check if the second part is "SET" which would mean no table name
            if parts[1].to_uppercase() == "SET" {
                return Err("Invalid UPDATE statement: missing table name".to_string());
            }
            parts[1].to_string()
        },
        SqlOperation::Other(_) => {
            // For other operations, do nothing
            "".to_string()
        }
    };

    // Check for RETURNING id clause (only relevant for INSERT)
    let returning_id = match operation {
        SqlOperation::Insert => {
                // Check if the statement ends with "RETURNING id" (case insensitive, ignoring whitespace)
                let sql_upper = sql.to_uppercase();
                let trimmed = sql_upper.trim_end();

                // Split by whitespace and check if the last two tokens are "RETURNING" and "ID"
                let tokens: Vec<&str> = trimmed.split_whitespace().collect();
                tokens.len() >= 2
                    && tokens[tokens.len() - 2] == "RETURNING"
                    && tokens[tokens.len() - 1] == "ID"
            },
        SqlOperation::Delete => false,
        SqlOperation::Update => false,
        SqlOperation::Other(_) => false,
    };

    Ok(SqlInfo {
        operation,
        table_name,
        is_returning_id: returning_id,
    })
}

/// Replaces named parameters (:key) in an SQL query with positional parameters (?n)
/// where n is the 1-based index of the key in the provided list.
/// Ignores occurrences of :key that are inside single quotes.
///
/// # Arguments
/// * `keys` - A slice of string keys to search for in the query
/// * `sql` - The SQL query string containing named parameters
///
/// # Returns
/// A new String with named parameters replaced by positional parameters
///
/// # Examples
/// ```
/// use qxsqld::replace_named_with_positional_params;
/// let keys = vec!["name", "age"];
/// let sql = "SELECT * FROM users WHERE name = :name AND age > :age";
/// let result = replace_named_with_positional_params(&keys, sql);
/// assert_eq!(result, "SELECT * FROM users WHERE name = ?1 AND age > ?2");
/// ```
pub(crate) fn replace_named_with_positional_params(sql: &str, keys: &[&str], repl_char: char) -> String {
    let mut result = String::with_capacity(sql.len());
    let mut chars = sql.chars().peekable();
    let mut in_single_quotes = false;

    while let Some(ch) = chars.next() {
        match ch {
            '\'' => {
                result.push(ch);
                // Handle escaped single quotes ('')
                if chars.peek() == Some(&'\'') {
                    result.push(chars.next().unwrap()); // consume the second quote
                } else {
                    in_single_quotes = !in_single_quotes;
                }
            }
            ':' if !in_single_quotes => {
                // Check if this colon is followed by one of our keys
                let remaining: String = chars.clone().collect();
                let mut matched_key = None;
                let mut matched_length = 0;

                // Find the longest matching key to handle cases where one key is a prefix of another
                for (index, &key) in keys.iter().enumerate() {
                    if remaining.starts_with(key) {
                        // Check that the key is followed by a non-alphanumeric character or end of string
                        let next_char_pos = key.len();
                        let is_word_boundary = match remaining.chars().nth(next_char_pos) {
                            Some(c) => !c.is_alphanumeric() && c != '_',
                            None => true,
                        };

                        if is_word_boundary && key.len() > matched_length {
                            matched_key = Some((index + 1, key)); // 1-based index
                            matched_length = key.len();
                        }
                    }
                }

                if let Some((param_number, key)) = matched_key {
                    // Replace :key with ?n
                    result.push_str(&format!("{repl_char}{}", param_number));
                    // Skip the key characters
                    for _ in 0..key.len() {
                        chars.next();
                    }
                } else {
                    // Not a matching key, just add the colon
                    result.push(ch);
                }
            }
            _ => {
                result.push(ch);
            }
        }
    }

    result
}

pub(crate) fn postgres_query_positional_args_from_sqlite(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut param_counter = 1;
    let mut in_single_quote = false;

    for c in input.chars() {
        match c {
            '\'' => {
                output.push(c);
                in_single_quote = !in_single_quote;
            }
            '?' if !in_single_quote => {
                output.push('$');
                output.push_str(&param_counter.to_string());
                param_counter += 1;
            }
            _ => output.push(c),
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_named_to_positional_basic_replacement() {
        let keys = vec!["name", "age"];
        let sql = "SELECT * FROM users WHERE name = :name AND age > :age";
        let result = replace_named_with_positional_params(sql, &keys, '?');
        assert_eq!(result, "SELECT * FROM users WHERE name = ?1 AND age > ?2");
    }

    #[test]
    fn test_named_to_positional_ignore_in_quotes() {
        let keys = vec!["name"];
        let sql = "SELECT * FROM users WHERE name = :name AND description = 'User :name here'";
        let result = replace_named_with_positional_params(sql, &keys, '?');
        assert_eq!(result, "SELECT * FROM users WHERE name = ?1 AND description = 'User :name here'");
    }

    #[test]
    fn test_named_to_positional_escaped_quotes() {
        let keys = vec!["name"];
        let sql = "SELECT * FROM users WHERE name = :name AND description = 'User''s :name here' AND other = :name";
        let result = replace_named_with_positional_params(sql, &keys, '?');
        assert_eq!(result, "SELECT * FROM users WHERE name = ?1 AND description = 'User''s :name here' AND other = ?1");
    }

    #[test]
    fn test_named_to_positional_key_as_substring() {
        let keys = vec!["name", "username"];
        let sql = "SELECT * FROM users WHERE username = :username AND name = :name";
        let result = replace_named_with_positional_params(sql, &keys, '?');
        assert_eq!(result, "SELECT * FROM users WHERE username = ?2 AND name = ?1");
    }

    #[test]
    fn test_named_to_positional_no_matching_keys() {
        let keys = vec!["name"];
        let sql = "SELECT * FROM users WHERE id = :id";
        let result = replace_named_with_positional_params(sql, &keys, '?');
        assert_eq!(result, "SELECT * FROM users WHERE id = :id");
    }

    #[test]
    fn test_named_to_positional_multiple_occurrences() {
        let keys = vec!["name"];
        let sql = "SELECT * FROM users WHERE first_name = :name OR last_name = :name";
        let result = replace_named_with_positional_params(sql, &keys, '?');
        assert_eq!(result, "SELECT * FROM users WHERE first_name = ?1 OR last_name = ?1");
    }

    #[test]
    fn test_named_to_positional_word_boundary() {
        let keys = vec!["name"];
        let sql = "SELECT * FROM users WHERE name = :name AND filename = :filename";
        let result = replace_named_with_positional_params(sql, &keys, '?');
        assert_eq!(result, "SELECT * FROM users WHERE name = ?1 AND filename = :filename");
    }

    #[test]
    fn test_named_to_positional_empty_keys() {
        let keys: Vec<&str> = vec![];
        let sql = "SELECT * FROM users WHERE name = :name";
        let result = replace_named_with_positional_params(sql, &keys, '?');
        assert_eq!(result, "SELECT * FROM users WHERE name = :name");
    }
}
