use qxsql::{DbValue, ToRecord, TryFromRecord};

#[derive(ToRecord)]
struct User {
    id: i64,
    name: String,
    active: bool,
}

#[derive(ToRecord)]
#[allow(dead_code)]
struct Event {
    id: i64,
    #[to_record(rename = "event_type")]
    kind: String,
    #[to_record(skip)]
    internal_flag: bool,
    #[to_record(skip_if_none)]
    description: Option<String>,
}

#[derive(ToRecord)]
struct NullableFields {
    id: i64,
    comment: Option<String>,
}

#[derive(ToRecord)]
struct SkipIfNoneFields {
    id: i64,
    #[to_record(skip_if_none)]
    comment: Option<String>,
}

#[test]
fn test_basic_fields() {
    let u = User { id: 1, name: "Alice".to_string(), active: true };
    let r = u.to_record();
    assert_eq!(r.get("id"), Some(&DbValue::Int(1)));
    assert_eq!(r.get("name"), Some(&DbValue::String("Alice".to_string())));
    assert_eq!(r.get("active"), Some(&DbValue::Bool(true)));
}

#[test]
fn test_rename() {
    let e = Event {
        id: 7,
        kind: "login".to_string(),
        internal_flag: true,
        description: Some("desc".to_string()),
    };
    let r = e.to_record();
    assert_eq!(r.get("event_type"), Some(&DbValue::String("login".to_string())));
    assert!(!r.contains_key("kind"), "original field name should not appear");
}

#[test]
fn test_skip() {
    let e = Event {
        id: 7,
        kind: "logout".to_string(),
        internal_flag: true,
        description: None,
    };
    let r = e.to_record();
    assert!(!r.contains_key("internal_flag"));
}

#[test]
fn test_option_some_is_inserted() {
    let e = Event {
        id: 1,
        kind: "x".to_string(),
        internal_flag: false,
        description: Some("hello".to_string()),
    };
    let r = e.to_record();
    assert_eq!(r.get("description"), Some(&DbValue::String("hello".to_string())));
}

#[test]
fn test_option_none_is_omitted() {
    let e = Event {
        id: 1,
        kind: "x".to_string(),
        internal_flag: false,
        description: None,
    };
    let r = e.to_record();
    assert!(!r.contains_key("description"), "None Option should be absent");
}

#[test]
fn test_option_none_defaults_to_null_some() {
    let n = NullableFields { id: 3, comment: Some("hi".to_string()) };
    let r = n.to_record();
    assert_eq!(r.get("comment"), Some(&DbValue::String("hi".to_string())));
}

#[test]
fn test_option_none_defaults_to_null_none() {
    let n = NullableFields { id: 3, comment: None };
    let r = n.to_record();
    assert_eq!(r.get("comment"), Some(&DbValue::Null));
}

#[test]
fn test_skip_if_none_some() {
    let n = SkipIfNoneFields { id: 3, comment: Some("hi".to_string()) };
    let r = n.to_record();
    assert_eq!(r.get("comment"), Some(&DbValue::String("hi".to_string())));
}

#[test]
fn test_skip_if_none_none() {
    let n = SkipIfNoneFields { id: 3, comment: None };
    let r = n.to_record();
    assert!(!r.contains_key("comment"));
}

// ---------------------------------------------------------------------------
// TryFromRecord
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, TryFromRecord)]
struct UserRow {
    id: i64,
    name: String,
    active: bool,
}

#[derive(Debug, PartialEq, TryFromRecord)]
struct EventRow {
    id: i64,
    #[to_record(rename = "event_type")]
    kind: String,
    #[to_record(skip)]
    internal_flag: bool, // always Default::default() = false
    description: Option<String>,
}

#[derive(Debug, PartialEq, TryFromRecord)]
struct NullableRow {
    id: i64,
    #[to_record(skip_if_none)]
    comment: Option<String>,
}

fn make_record(pairs: &[(&str, DbValue)]) -> qxsql::Record {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

#[test]
fn try_from_basic_fields() {
    let rec = make_record(&[
        ("id",     DbValue::Int(42)),
        ("name",   DbValue::String("Bob".into())),
        ("active", DbValue::Bool(true)),
    ]);
    let u = UserRow::try_from_record(rec).unwrap();
    assert_eq!(u, UserRow { id: 42, name: "Bob".into(), active: true });
}

#[test]
fn try_from_rename() {
    let rec = make_record(&[
        ("id",         DbValue::Int(1)),
        ("event_type", DbValue::String("click".into())),
        ("description",DbValue::String("pressed".into())),
    ]);
    let e = EventRow::try_from_record(rec).unwrap();
    assert_eq!(e.kind, "click");
    assert_eq!(e.description, Some("pressed".into()));
}

#[test]
fn try_from_skip_uses_default() {
    let rec = make_record(&[
        ("id",         DbValue::Int(5)),
        ("event_type", DbValue::String("focus".into())),
    ]);
    let e = EventRow::try_from_record(rec).unwrap();
    // `internal_flag` is skipped, so it must be false (bool::default)
    assert!(!e.internal_flag);
}

#[test]
fn try_from_option_some() {
    let rec = make_record(&[
        ("id",      DbValue::Int(7)),
        ("comment", DbValue::String("hello".into())),
    ]);
    let n = NullableRow::try_from_record(rec).unwrap();
    assert_eq!(n.comment, Some("hello".into()));
}

#[test]
fn try_from_option_null_becomes_none() {
    let rec = make_record(&[
        ("id",      DbValue::Int(8)),
        ("comment", DbValue::Null),
    ]);
    let n = NullableRow::try_from_record(rec).unwrap();
    assert_eq!(n.comment, None);
}

#[test]
fn try_from_option_missing_key_becomes_none() {
    // `comment` key absent entirely — should yield None, not an error
    let rec = make_record(&[("id", DbValue::Int(9))]);
    let n = NullableRow::try_from_record(rec).unwrap();
    assert_eq!(n.comment, None);
}

#[test]
fn try_from_missing_required_field_is_err() {
    // `name` is missing — must be an Err
    let rec = make_record(&[
        ("id",     DbValue::Int(1)),
        ("active", DbValue::Bool(false)),
    ]);
    let result = UserRow::try_from_record(rec);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("name"));
}

#[test]
fn try_from_wrong_type_is_err() {
    // `id` is a String instead of Int
    let rec = make_record(&[
        ("id",     DbValue::String("not-a-number".into())),
        ("name",   DbValue::String("Alice".into())),
        ("active", DbValue::Bool(true)),
    ]);
    let result = UserRow::try_from_record(rec);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("id"));
}

#[test]
fn try_from_roundtrip_with_to_record() {
    // ToRecord -> Record -> TryFromRecord must yield the original value
    #[derive(Debug, PartialEq, ToRecord, TryFromRecord)]
    struct Item {
        id: i64,
        label: String,
        score: f64,
        enabled: bool,
        note: Option<String>,
    }

    let original = Item {
        id: 99,
        label: "widget".into(),
        score: 3.14,
        enabled: false,
        note: Some("test note".into()),
    };
    let rec = original.to_record();
    let restored = Item::try_from_record(rec).unwrap();
    assert_eq!(original, restored);
}
