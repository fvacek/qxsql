

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
/// use qxsqld::sql_utils::replace_named_with_positional_params;
/// let keys = vec!["name", "age"];
/// let sql = "SELECT * FROM users WHERE name = :name AND age > :age";
/// let result = replace_named_with_positional_params(sql, &keys, '?');
/// assert_eq!(result, "SELECT * FROM users WHERE name = ?1 AND age > ?2");
/// ```
pub fn replace_named_with_positional_params(sql: &str, keys: &[&str], repl_char: char) -> String {
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

pub fn postgres_query_positional_args_from_sqlite(input: &str) -> String {
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
