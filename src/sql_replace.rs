use std::collections::HashMap;

use crate::sql::DbValue;

/// Trait for types that can provide parameter lookups
pub(crate) trait ParamLookup {
    fn get_param(&self, key: &str) -> Option<&DbValue>;
}

impl ParamLookup for HashMap<String, DbValue> {
    fn get_param(&self, key: &str) -> Option<&DbValue> {
        self.get(key)
    }
}

impl ParamLookup for &HashMap<String, DbValue> {
    fn get_param(&self, key: &str) -> Option<&DbValue> {
        self.get(key)
    }
}

impl ParamLookup for &[(&str, DbValue)] {
    fn get_param(&self, key: &str) -> Option<&DbValue> {
        self.iter().find(|(k, _)| *k == key).map(|(_, v)| v)
    }
}

/// Replaces `:key` patterns in the input string with values from the lookup,
/// ignoring any `:key` inside single quotes.
pub(crate) fn replace_params<P>(input: &str, values: P) -> String
where
    P: ParamLookup,
{
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    let mut in_single_quote = false;

    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                in_single_quote = !in_single_quote;
                output.push(c);
            }
            ':' if !in_single_quote => {
                let mut key = String::new();
                while let Some(&next) = chars.peek() {
                    if next.is_alphanumeric() || next == '_' {
                        key.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }

                if !key.is_empty() {
                    if let Some(val) = values.get_param(&key) {
                        match val {
                            DbValue::String(s) => {
                                output.push('\'');
                                output.push_str(s);
                                output.push('\'');
                            }
                            DbValue::Int(i) => output.push_str(&i.to_string()),
                            DbValue::DateTime(dt) => {
                                output.push('\'');
                                output.push_str(&dt.to_rfc3339());
                                output.push('\'');
                            }
                            DbValue::Bool(b) => output.push_str(if *b {"true"} else {"false"}),
                            DbValue::Null => output.push_str("NULL"),
                        }
                    } else {
                        output.push(':');
                        output.push_str(&key);
                    }
                } else {
                    output.push(':');
                }
            }
            _ => output.push(c),
        }
    }

    output
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

    fn make_params(pairs: &[(&str, DbValue)]) -> HashMap<String, DbValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn test_basic_types() {
        let values = make_params(&[
            ("name", "Alice".into()),
            ("age", DbValue::Int(30)),
            ("married", true.into()),
            ("tags", ().into()),
        ]);
        let input = "User: :name, Age: :age, Married: :married, Tags: :tags in age: :age";
        let output = replace_params(input, &values);
        assert_eq!(output, "User: 'Alice', Age: 30, Married: true, Tags: NULL in age: 30");
    }

    #[test]
    fn test_slice_params() {
        let values = &[
            ("name", "Bob".into()),
            ("score", DbValue::Int(95)),
        ];
        let input = "Player :name scored :score points";
        let output = replace_params(input, values as &[(&str, DbValue)]);
        assert_eq!(output, "Player 'Bob' scored 95 points");
    }

    #[test]
    fn test_basic_replacement() {
        let values = make_params(&[("name", "Alice".into()), ("age", DbValue::Int(30))]);
        let input = "User: :name, Age: :age";
        let output = replace_params(input, &values);
        assert_eq!(output, "User: 'Alice', Age: 30");
    }

    #[test]
    fn test_quoted_literal_is_ignored() {
        let values = make_params(&[("key", "VAL".into())]);
        let input = "In SQL: ':key' should not be replaced";
        let output = replace_params(input, &values);
        assert_eq!(output, "In SQL: ':key' should not be replaced");
    }

    #[test]
    fn test_missing_key_is_preserved() {
        let values = make_params(&[("name", "Alice".into())]);
        let input = "Hello :name and :missing";
        let output = replace_params(input, &values);
        assert_eq!(output, "Hello 'Alice' and :missing");
    }

    #[test]
    fn test_mixed_content() {
        let values = make_params(&[("a", "X".into()), ("b", "Y".into())]);
        let input = "a=:a, b=':b', :c=:c";
        let output = replace_params(input, &values);
        assert_eq!(output, "a='X', b=':b', :c=:c");
    }

    #[test]
    fn test_lone_colon() {
        let values = make_params(&[]);
        let input = "Time: 12:30";
        let output = replace_params(input, &values);
        assert_eq!(output, "Time: 12:30");
    }

    #[test]
    fn test_multiple_quotes() {
        let values = make_params(&[("x", "XVAL".into())]);
        let input = "'literal1 :x' 'literal2' :x";
        let output = replace_params(input, &values);
        assert_eq!(output, "'literal1 :x' 'literal2' 'XVAL'");
    }
}
