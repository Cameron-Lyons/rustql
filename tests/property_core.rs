use proptest::prelude::*;
use rustql::ast::{ColumnDefinition, DataType, Expression, TableConstraint, Value};
use rustql::database::{CompositeIndex, Database, Index, RowId, Table, View};
use rustql::error::{RustqlError, SourceLocation};
use rustql::lexer::{Token, tokenize, tokenize_spanned};
use rustql::parser::{parse_script, parse_script_spanned};
use rustql::storage::{BTreeStorageEngine, StorageEngine};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

const LEXER_ALPHABET: &[u8] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_ ',;()*=<>!+-/\\\n\t\r.|:\"`";
const SQL_TEXT_ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789 _";
const STORAGE_TEXT_ALPHABET: &[u8] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789 _-:/";

#[derive(Debug, Clone)]
struct TableSpec {
    column_types: Vec<DataType>,
    rows: Vec<Vec<Value>>,
    next_row_padding: u8,
}

#[derive(Debug, PartialEq)]
struct CanonicalDatabase {
    tables: BTreeMap<String, CanonicalTable>,
    indexes: BTreeMap<String, CanonicalIndex>,
    composite_indexes: BTreeMap<String, CanonicalCompositeIndex>,
    views: BTreeMap<String, CanonicalView>,
}

#[derive(Debug, PartialEq)]
struct CanonicalTable {
    columns: Vec<ColumnDefinition>,
    rows: Vec<Vec<Value>>,
    row_ids: Vec<u64>,
    next_row_id: u64,
    constraints: Vec<TableConstraint>,
}

#[derive(Debug, PartialEq)]
struct CanonicalIndex {
    name: String,
    table: String,
    column: String,
    entries: Vec<(Value, Vec<u64>)>,
    filter_expr: Option<Expression>,
}

#[derive(Debug, PartialEq)]
struct CanonicalCompositeIndex {
    name: String,
    table: String,
    columns: Vec<String>,
    entries: Vec<(Vec<Value>, Vec<u64>)>,
    filter_expr: Option<Expression>,
}

#[derive(Debug, PartialEq)]
struct CanonicalView {
    name: String,
    query_sql: String,
}

struct TempStoragePath {
    path: PathBuf,
}

impl TempStoragePath {
    fn new(label: &str) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock should be after epoch")
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "rustql-proptest-{}-{}-{}-{}",
            label,
            std::process::id(),
            timestamp,
            counter
        ));
        cleanup_storage_files(&path);
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn wal_path(&self) -> PathBuf {
        wal_path(self.path())
    }
}

impl Drop for TempStoragePath {
    fn drop(&mut self) {
        cleanup_storage_files(&self.path);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    #[test]
    fn lexer_spans_round_trip_lexemes(input in arb_lexer_input()) {
        let Ok(tokens) = tokenize(&input) else {
            return Ok(());
        };
        let spanned = tokenize_spanned(&input)
            .expect("tokenize_spanned should succeed whenever tokenize succeeds");

        prop_assert_eq!(
            spanned.iter().map(|token| token.token.clone()).collect::<Vec<_>>(),
            tokens
        );

        let mut previous_end = 0usize;
        for spanned_token in &spanned {
            let start = byte_index_for_location(&input, spanned_token.span.start)
                .expect("span start should map back into the input");
            let end = byte_index_for_location(&input, spanned_token.span.end)
                .expect("span end should map back into the input");

            prop_assert!(previous_end <= start);
            prop_assert!(start <= end);
            previous_end = end;

            if spanned_token.token == Token::Eof {
                prop_assert_eq!(start, end);
                prop_assert_eq!(end, input.len());
                continue;
            }

            let lexeme = &input[start..end];
            let lexeme_tokens = tokenize(lexeme)
                .expect("a span lexeme should re-tokenize successfully");
            prop_assert_eq!(lexeme_tokens, vec![spanned_token.token.clone(), Token::Eof]);
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(96))]

    #[test]
    fn generated_scripts_parse_identically_with_and_without_spans(script in arb_valid_script()) {
        let tokens = tokenize(&script)
            .expect("generated scripts should tokenize");
        let spanned = tokenize_spanned(&script)
            .expect("generated scripts should tokenize with spans");

        let parsed = parse_script(tokens)
            .expect("generated scripts should parse");
        let parsed_spanned = parse_script_spanned(spanned)
            .expect("generated scripts should parse with spans");

        prop_assert_eq!(parsed, parsed_spanned);
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    #[test]
fn btree_round_trips_generated_databases(db in arb_database()) {
        let db = build_database(db);
        let temp = TempStoragePath::new("roundtrip");
        let engine = BTreeStorageEngine::new(temp.path());

        match engine.save(&db) {
            Ok(()) => {}
            Err(RustqlError::StorageError(message))
                if message.contains("BTreePage too large to fit in fixed page size") =>
            {
                prop_assume!(false);
            }
            Err(error) => panic!("round-trip save should succeed: {error:?}"),
        }
        let loaded = engine.load().expect("round-trip load should succeed");

        prop_assert_eq!(canonical_database(&db), canonical_database(&loaded));
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]

    #[test]
    fn committed_btree_journal_recovers_target_snapshot(
        base in arb_database(),
        target in arb_database(),
    ) {
        let base = build_database(base);
        let target = build_database(target);
        let temp = TempStoragePath::new("recovery");
        let engine = BTreeStorageEngine::new(temp.path());

        match engine.save(&base) {
            Ok(()) => {}
            Err(RustqlError::StorageError(message))
                if message.contains("BTreePage too large to fit in fixed page size") =>
            {
                prop_assume!(false);
            }
            Err(error) => panic!("base save should succeed: {error:?}"),
        }
        engine.begin_transaction().expect("begin transaction should succeed");
        match engine.prepare_commit(&target) {
            Ok(()) => {}
            Err(RustqlError::StorageError(message))
                if message.contains("BTreePage too large to fit in fixed page size") =>
            {
                prop_assume!(false);
            }
            Err(error) => panic!("prepare_commit should succeed: {error:?}"),
        }

        let reloaded = BTreeStorageEngine::new(temp.path());
        let recovered = reloaded.load().expect("load should replay the journal");

        prop_assert_eq!(canonical_database(&target), canonical_database(&recovered));
        prop_assert!(!temp.wal_path().exists());
    }
}

fn arb_lexer_input() -> impl Strategy<Value = String> {
    ascii_string(LEXER_ALPHABET, 80)
}

fn arb_valid_script() -> impl Strategy<Value = String> {
    (
        prop::collection::vec(arb_statement_sql(), 1..=3),
        arb_whitespace(3),
        arb_whitespace(3),
        any::<bool>(),
    )
        .prop_map(
            |(statements, leading_ws, trailing_ws, trailing_semicolon)| {
                let mut script = format!("{leading_ws}{}", statements.join(";\n"));
                if trailing_semicolon {
                    script.push(';');
                }
                script.push_str(&trailing_ws);
                script
            },
        )
}

fn arb_statement_sql() -> BoxedStrategy<String> {
    prop_oneof![
        arb_create_table_sql().boxed(),
        arb_insert_sql().boxed(),
        arb_update_sql().boxed(),
        arb_delete_sql().boxed(),
    ]
    .boxed()
}

fn arb_create_table_sql() -> impl Strategy<Value = String> {
    (0u8..=3, 1usize..=3).prop_flat_map(|(table_idx, column_count)| {
        (
            Just(table_idx),
            prop::collection::vec(arb_sql_data_type_name(), column_count..=column_count),
            any::<bool>(),
        )
            .prop_map(move |(table_idx, types, if_not_exists)| {
                let columns = types
                    .into_iter()
                    .enumerate()
                    .map(|(idx, data_type)| format!("c{idx} {data_type}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                let if_not_exists = if if_not_exists { " IF NOT EXISTS" } else { "" };
                format!("CREATE TABLE{if_not_exists} t{table_idx} ({columns})")
            })
    })
}

fn arb_insert_sql() -> impl Strategy<Value = String> {
    (0u8..=3, 1usize..=3).prop_flat_map(|(table_idx, column_count)| {
        (
            Just(table_idx),
            Just(column_count),
            prop::collection::vec(
                prop::collection::vec(arb_sql_literal(), column_count..=column_count),
                1..=3,
            ),
            any::<bool>(),
            prop::option::of(arb_insert_conflict_clause()),
            prop::option::of(prop::collection::vec(
                any::<bool>(),
                column_count..=column_count,
            )),
        )
            .prop_map(
                move |(table_idx, column_count, rows, include_columns, on_conflict, returning)| {
                    let columns = column_names(column_count);
                    let values = rows
                        .into_iter()
                        .map(|row| format!("({})", row.join(", ")))
                        .collect::<Vec<_>>()
                        .join(", ");

                    let mut sql = format!("INSERT INTO t{table_idx}");
                    if include_columns {
                        sql.push_str(&format!(" ({})", columns.join(", ")));
                    }
                    sql.push_str(&format!(" VALUES {values}"));
                    if let Some(on_conflict) = on_conflict {
                        sql.push(' ');
                        sql.push_str(&on_conflict);
                    }
                    if let Some(returning) = render_returning(column_count, returning) {
                        sql.push_str(&returning);
                    }
                    sql
                },
            )
    })
}

fn arb_update_sql() -> impl Strategy<Value = String> {
    (0u8..=3, 1usize..=3).prop_flat_map(|(table_idx, column_count)| {
        (
            Just(table_idx),
            Just(column_count),
            prop::collection::vec(arb_sql_literal(), column_count..=column_count),
            prop::option::of(arb_predicate_sql(column_count)),
            prop::option::of(prop::collection::vec(
                any::<bool>(),
                column_count..=column_count,
            )),
        )
            .prop_map(
                move |(table_idx, column_count, values, predicate, returning)| {
                    let assignments = values
                        .into_iter()
                        .enumerate()
                        .map(|(idx, value)| format!("c{idx} = {value}"))
                        .collect::<Vec<_>>()
                        .join(", ");

                    let mut sql = format!("UPDATE t{table_idx} SET {assignments}");
                    if let Some(predicate) = predicate {
                        sql.push_str(&format!(" WHERE {predicate}"));
                    }
                    if let Some(returning) = render_returning(column_count, returning) {
                        sql.push_str(&returning);
                    }
                    sql
                },
            )
    })
}

fn arb_delete_sql() -> impl Strategy<Value = String> {
    (0u8..=3, 1usize..=3).prop_flat_map(|(table_idx, column_count)| {
        (
            Just(table_idx),
            Just(column_count),
            prop::option::of(arb_predicate_sql(column_count)),
            prop::option::of(prop::collection::vec(
                any::<bool>(),
                column_count..=column_count,
            )),
        )
            .prop_map(move |(table_idx, column_count, predicate, returning)| {
                let mut sql = format!("DELETE FROM t{table_idx}");
                if let Some(predicate) = predicate {
                    sql.push_str(&format!(" WHERE {predicate}"));
                }
                if let Some(returning) = render_returning(column_count, returning) {
                    sql.push_str(&returning);
                }
                sql
            })
    })
}

fn arb_insert_conflict_clause() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("ON CONFLICT (c0) DO NOTHING".to_string()),
        arb_sql_literal().prop_map(|value| format!("ON CONFLICT (c0) DO UPDATE SET c0 = {value}")),
    ]
}

fn arb_predicate_sql(column_count: usize) -> BoxedStrategy<String> {
    prop_oneof![
        (
            0usize..column_count,
            arb_comparison_operator(),
            arb_sql_literal()
        )
            .prop_map(|(column, op, value)| { format!("c{column} {op} {value}") }),
        (0usize..column_count).prop_map(|column| format!("c{column} IS NULL")),
        (0usize..column_count).prop_map(|column| format!("c{column} IS NOT NULL")),
    ]
    .boxed()
}

fn arb_comparison_operator() -> impl Strategy<Value = &'static str> {
    prop::sample::select(vec!["=", "<>", "<", ">", "<=", ">="])
}

fn arb_sql_data_type_name() -> impl Strategy<Value = String> {
    prop::sample::select(vec![
        "INTEGER".to_string(),
        "FLOAT".to_string(),
        "TEXT".to_string(),
        "BOOLEAN".to_string(),
        "DATE".to_string(),
        "TIME".to_string(),
        "DATETIME".to_string(),
    ])
}

fn arb_sql_literal() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("NULL".to_string()),
        any::<i16>().prop_map(|value| value.to_string()),
        ((-500i16..=500i16), 0u8..=99)
            .prop_map(|(whole, fraction)| format!("{whole}.{fraction:02}")),
        ascii_string(SQL_TEXT_ALPHABET, 8).prop_map(|value| format!("'{}'", value)),
    ]
}

fn arb_whitespace(max_len: usize) -> impl Strategy<Value = String> {
    ascii_string(b" \n\t", max_len)
}

fn arb_database() -> impl Strategy<Value = Vec<TableSpec>> {
    prop::collection::vec(arb_table_spec(), 0..=2)
}

fn arb_table_spec() -> impl Strategy<Value = TableSpec> {
    (1usize..=2).prop_flat_map(|column_count| {
        (
            prop::collection::vec(arb_data_type(), column_count..=column_count),
            prop::collection::vec(
                prop::collection::vec(arb_value(), column_count..=column_count),
                0..=3,
            ),
            0u8..=5,
        )
            .prop_map(|(column_types, rows, next_row_padding)| TableSpec {
                column_types,
                rows,
                next_row_padding,
            })
    })
}

fn arb_data_type() -> impl Strategy<Value = DataType> {
    prop_oneof![
        Just(DataType::Integer),
        Just(DataType::Float),
        Just(DataType::Text),
        Just(DataType::Boolean),
        Just(DataType::Date),
        Just(DataType::Time),
        Just(DataType::DateTime),
    ]
}

fn arb_value() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<i16>().prop_map(|value| Value::Integer(i64::from(value))),
        ((-500i16..=500i16), 0u8..=99).prop_map(|(whole, fraction)| {
            Value::Float(f64::from(whole) + f64::from(fraction) / 100.0)
        }),
        ascii_string(STORAGE_TEXT_ALPHABET, 6).prop_map(Value::Text),
        any::<bool>().prop_map(Value::Boolean),
        (2000u16..=2030, 1u8..=12, 1u8..=28).prop_map(|(year, month, day)| {
            Value::Date(format!("{year:04}-{month:02}-{day:02}"))
        }),
        (0u8..=23, 0u8..=59, 0u8..=59).prop_map(|(hour, minute, second)| {
            Value::Time(format!("{hour:02}:{minute:02}:{second:02}"))
        }),
        (
            (2000u16..=2030, 1u8..=12, 1u8..=28),
            (0u8..=23, 0u8..=59, 0u8..=59),
        )
            .prop_map(|((year, month, day), (hour, minute, second))| {
                Value::DateTime(format!(
                    "{year:04}-{month:02}-{day:02} {hour:02}:{minute:02}:{second:02}"
                ))
            }),
    ]
}

fn build_database(specs: Vec<TableSpec>) -> Database {
    let mut db = Database::new();

    for (table_index, spec) in specs.into_iter().enumerate() {
        let table_name = format!("t{table_index}");
        let columns = spec
            .column_types
            .into_iter()
            .enumerate()
            .map(|(column_index, data_type)| ColumnDefinition {
                name: format!("c{column_index}"),
                data_type,
                nullable: true,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            })
            .collect::<Vec<_>>();
        let row_ids = (1..=spec.rows.len() as u64).map(RowId).collect::<Vec<_>>();
        let next_row_id = spec.rows.len() as u64 + 1 + u64::from(spec.next_row_padding);

        let table = Table::with_rows_and_ids(columns, spec.rows, row_ids, next_row_id, Vec::new());
        db.tables.insert(table_name, table);
    }

    let table_names = db.tables.keys().cloned().collect::<Vec<_>>();
    for (view_index, table_name) in table_names.iter().take(2).enumerate() {
        let view_name = format!("v{view_index}");
        db.views.insert(
            view_name.clone(),
            View {
                name: view_name,
                query_sql: format!("SELECT * FROM {table_name}"),
            },
        );
    }

    for table_name in &table_names {
        let Some(table) = db.tables.get(table_name) else {
            continue;
        };
        if table.columns.is_empty() {
            continue;
        }

        let mut entries = BTreeMap::new();
        for (row_id, row) in table.iter_rows_with_ids() {
            entries
                .entry(row[0].clone())
                .or_insert_with(Vec::new)
                .push(row_id);
        }
        let index_name = format!("idx_{table_name}_c0");
        db.indexes.insert(
            index_name.clone(),
            Index {
                name: index_name,
                table: table_name.clone(),
                column: table.columns[0].name.clone(),
                entries,
                filter_expr: None,
            },
        );

        let composite_column_count = table.columns.len().min(2);
        let composite_columns = table.columns[..composite_column_count]
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let mut composite_entries = BTreeMap::new();
        for (row_id, row) in table.iter_rows_with_ids() {
            composite_entries
                .entry(row[..composite_column_count].to_vec())
                .or_insert_with(Vec::new)
                .push(row_id);
        }
        let composite_name = format!("cidx_{table_name}");
        db.composite_indexes.insert(
            composite_name.clone(),
            CompositeIndex {
                name: composite_name,
                table: table_name.clone(),
                columns: composite_columns,
                entries: composite_entries,
                filter_expr: None,
            },
        );
    }

    db.normalize_row_ids();
    db
}

fn canonical_database(db: &Database) -> CanonicalDatabase {
    let mut normalized = db.clone();
    normalized.normalize_row_ids();

    CanonicalDatabase {
        tables: normalized
            .tables
            .iter()
            .map(|(name, table)| {
                (
                    name.clone(),
                    CanonicalTable {
                        columns: table.columns.clone(),
                        rows: table.rows.clone(),
                        row_ids: table.row_ids.iter().map(|row_id| row_id.0).collect(),
                        next_row_id: table.next_row_id,
                        constraints: table.constraints.clone(),
                    },
                )
            })
            .collect(),
        indexes: normalized
            .indexes
            .iter()
            .map(|(name, index)| {
                (
                    name.clone(),
                    CanonicalIndex {
                        name: index.name.clone(),
                        table: index.table.clone(),
                        column: index.column.clone(),
                        entries: index
                            .entries
                            .iter()
                            .map(|(value, row_ids)| {
                                (
                                    value.clone(),
                                    row_ids.iter().map(|row_id| row_id.0).collect::<Vec<_>>(),
                                )
                            })
                            .collect(),
                        filter_expr: index.filter_expr.clone(),
                    },
                )
            })
            .collect(),
        composite_indexes: normalized
            .composite_indexes
            .iter()
            .map(|(name, index)| {
                (
                    name.clone(),
                    CanonicalCompositeIndex {
                        name: index.name.clone(),
                        table: index.table.clone(),
                        columns: index.columns.clone(),
                        entries: index
                            .entries
                            .iter()
                            .map(|(values, row_ids)| {
                                (
                                    values.clone(),
                                    row_ids.iter().map(|row_id| row_id.0).collect::<Vec<_>>(),
                                )
                            })
                            .collect(),
                        filter_expr: index.filter_expr.clone(),
                    },
                )
            })
            .collect(),
        views: normalized
            .views
            .iter()
            .map(|(name, view)| {
                (
                    name.clone(),
                    CanonicalView {
                        name: view.name.clone(),
                        query_sql: view.query_sql.clone(),
                    },
                )
            })
            .collect(),
    }
}

fn ascii_string(alphabet: &'static [u8], max_len: usize) -> impl Strategy<Value = String> {
    prop::collection::vec(prop::sample::select(alphabet.to_vec()), 0..=max_len)
        .prop_map(|bytes| String::from_utf8(bytes).expect("alphabet should be valid ASCII"))
}

fn column_names(column_count: usize) -> Vec<String> {
    (0..column_count).map(|idx| format!("c{idx}")).collect()
}

fn render_returning(column_count: usize, flags: Option<Vec<bool>>) -> Option<String> {
    let Some(flags) = flags else {
        return None;
    };
    let mut columns = flags
        .into_iter()
        .enumerate()
        .filter_map(|(index, selected)| selected.then(|| format!("c{index}")))
        .collect::<Vec<_>>();

    if columns.is_empty() && column_count > 0 {
        columns.push("c0".to_string());
    }

    (!columns.is_empty()).then(|| format!(" RETURNING {}", columns.join(", ")))
}

fn byte_index_for_location(input: &str, location: SourceLocation) -> Option<usize> {
    if location.line == 1 && location.column == 1 {
        return Some(0);
    }

    let mut line = 1usize;
    let mut column = 1usize;
    for (index, ch) in input.char_indices() {
        if line == location.line && column == location.column {
            return Some(index);
        }
        if ch == '\n' {
            line += 1;
            column = 1;
        } else {
            column += 1;
        }
    }

    (line == location.line && column == location.column).then_some(input.len())
}

fn wal_path(path: &Path) -> PathBuf {
    let mut wal = path.as_os_str().to_os_string();
    wal.push(".wal");
    PathBuf::from(wal)
}

fn cleanup_storage_files(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(wal_path(path));
}
