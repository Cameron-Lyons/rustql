use super::file::BTreeFile;
use super::header::{FILE_HEADER_SIZE, HEADER_RESERVED, write_versioned_header};
use super::journal::{
    JOURNAL_MAGIC, JOURNAL_VERSION, LEGACY_JOURNAL_VERSION, LegacyTransactionJournal,
    TransactionJournal,
};
use super::page::{BTREE_PAGE_SIZE, BTreeEntry, BTreePage, LEAF_INLINE_DATA_FLAG, PageKind};
use super::records::TableStorageRecord;
use super::*;
use crate::ast::{ColumnDefinition, DataType, TableConstraint, Value};
use crate::database::{CompositeIndex, RowId, Table, View};
use crate::storage::atomic_file::atomic_write;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

fn remove_storage_artifacts(path: &Path) {
    let _ = std::fs::remove_file(path);
    let mut journal = path.as_os_str().to_os_string();
    journal.push(".wal");
    let _ = std::fs::remove_file(PathBuf::from(journal));
}

#[test]
fn btree_storage_round_trip() {
    let temp_path = std::env::temp_dir().join("rustql_btree_test.dat");

    remove_storage_artifacts(&temp_path);

    let engine = BTreeStorageEngine::new(&temp_path);

    let mut db = Database::new();

    let columns = vec![
        ColumnDefinition {
            name: "id".to_string(),
            data_type: DataType::Integer,
            nullable: false,
            primary_key: true,
            unique: false,
            default_value: None,
            foreign_key: None,
            check: None,
            auto_increment: false,
            generated: None,
        },
        ColumnDefinition {
            name: "name".to_string(),
            data_type: DataType::Text,
            nullable: false,
            primary_key: false,
            unique: false,
            default_value: None,
            foreign_key: None,
            check: None,
            auto_increment: false,
            generated: None,
        },
    ];

    let rows = vec![vec![Value::Integer(1), Value::Text("Alice".to_string())]];

    let table = Table::new(columns, rows, vec![]);

    let mut tables = HashMap::new();
    tables.insert("users".to_string(), table);

    db.tables = tables;

    engine
        .save(&db)
        .expect("failed to save via BTreeStorageEngine");
    let loaded = engine
        .load()
        .expect("failed to load via BTreeStorageEngine");

    let users = loaded
        .tables
        .get("users")
        .expect("users table missing after load");
    assert_eq!(users.rows.len(), 1);
    assert_eq!(users.columns.len(), 2);
    assert_eq!(users.rows[0][0], Value::Integer(1));
    assert_eq!(users.rows[0][1], Value::Text("Alice".to_string()));

    remove_storage_artifacts(&temp_path);
}

#[test]
fn btree_storage_writes_inline_leaf_payloads() {
    let temp_path = std::env::temp_dir().join("rustql_btree_inline_payload_test.dat");
    remove_storage_artifacts(&temp_path);

    let engine = BTreeStorageEngine::new(&temp_path);
    let mut db = Database::new();
    db.tables.insert(
        "users".to_string(),
        Table::new(
            vec![ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            }],
            vec![vec![Value::Integer(1)]],
            vec![],
        ),
    );

    engine.save(&db).expect("failed to save database");

    let mut file = BTreeFile::open(&temp_path).expect("failed to open BTree file");
    let meta_page = file.read_page(0).expect("failed to read meta page");
    let root_page_id = meta_page
        .entries
        .iter()
        .find(|entry| matches!(&entry.key, Value::Text(text) if text == "root"))
        .expect("root entry should exist")
        .pointer;
    let root_page = file
        .read_page(root_page_id)
        .expect("failed to read root page");

    assert_eq!(root_page.header.kind, PageKind::Leaf);
    assert_ne!(root_page.header.reserved & LEAF_INLINE_DATA_FLAG, 0);
    assert!(
        root_page
            .entries
            .iter()
            .all(|entry| entry.inline_data.is_some())
    );

    remove_storage_artifacts(&temp_path);
}

#[test]
fn btree_storage_loads_legacy_row_keys_in_row_id_order() {
    let temp_path = std::env::temp_dir().join("rustql_btree_legacy_rows_test.dat");
    remove_storage_artifacts(&temp_path);

    let mut file = BTreeFile::create(&temp_path).expect("Failed to create BTree file");
    write_versioned_header(&mut file.file, BTreeFile::MAGIC, 2, "BTree storage file")
        .expect("Failed to write versioned header");

    let mut meta_page = BTreePage::new(0, PageKind::Meta);
    meta_page
        .entries
        .push(BTreeEntry::new(Value::Text("root".to_string()), 1));
    meta_page.header.entry_count = meta_page.entries.len() as u16;
    file.write_page(&meta_page)
        .expect("Failed to write meta page");

    let root_page_id = 1;
    let root_page = BTreePage::new(root_page_id, PageKind::Leaf);
    file.write_page(&root_page)
        .expect("Failed to write root page");

    let schema_json = serde_json::to_string(&TableStorageRecord {
        columns: vec![ColumnDefinition {
            name: "value".to_string(),
            data_type: DataType::Text,
            nullable: false,
            primary_key: false,
            unique: false,
            default_value: None,
            foreign_key: None,
            check: None,
            auto_increment: false,
            generated: None,
        }],
        constraints: Vec::<TableConstraint>::new(),
        next_row_id: 11,
    })
    .expect("Failed to encode schema");
    let schema_pointer = file
        .write_data_to_pointer(&schema_json)
        .expect("Failed to write schema");

    let mut current_root_id = file
        .insert(
            Value::Text("schema:test".to_string()),
            schema_pointer,
            root_page_id,
        )
        .expect("Failed to insert schema");

    for (row_id, value) in [(10u64, "ten"), (2u64, "two")] {
        let row_json =
            serde_json::to_string(&vec![Value::Text(value.to_string())]).expect("row json");
        let row_pointer = file
            .write_data_to_pointer(&row_json)
            .unwrap_or_else(|_| panic!("Failed to write legacy row {}", row_id));
        current_root_id = file
            .insert(
                Value::Text(format!("row:test:{}", row_id)),
                row_pointer,
                current_root_id,
            )
            .unwrap_or_else(|_| panic!("Failed to insert legacy row {}", row_id));
    }

    let mut meta_page = file.read_page(0).expect("Failed to reload meta page");
    meta_page
        .entries
        .iter_mut()
        .find(|entry| matches!(&entry.key, Value::Text(text) if text == "root"))
        .expect("root entry should exist")
        .pointer = current_root_id;
    meta_page.header.entry_count = meta_page.entries.len() as u16;
    file.write_page(&meta_page)
        .expect("Failed to update root pointer");
    drop(file);

    let engine = BTreeStorageEngine::new(&temp_path);
    let loaded = engine.load().expect("Failed to load legacy rows");
    let table = loaded.tables.get("test").expect("table should exist");

    assert_eq!(table.row_ids, vec![RowId(2), RowId(10)]);
    assert_eq!(
        table.rows,
        vec![
            vec![Value::Text("two".to_string())],
            vec![Value::Text("ten".to_string())]
        ]
    );

    remove_storage_artifacts(&temp_path);
}

#[test]
fn btree_search_insert_delete() {
    let temp_path = std::env::temp_dir().join("rustql_btree_ops_test.dat");
    remove_storage_artifacts(&temp_path);

    let mut file = BTreeFile::create(&temp_path).expect("Failed to create BTree file");

    let mut meta_page = BTreePage::new(0, PageKind::Meta);
    meta_page
        .entries
        .push(BTreeEntry::new(Value::Text("root".to_string()), 1));
    meta_page.header.entry_count = meta_page.entries.len() as u16;
    file.write_page(&meta_page)
        .expect("Failed to write meta page");

    let mut root_page = BTreePage::new(1, PageKind::Leaf);
    root_page.header.entry_count = 0;
    file.write_page(&root_page)
        .expect("Failed to write root page");

    let key1 = Value::Integer(10);
    let value1_json = serde_json::to_string(&Value::Text("value1".to_string())).unwrap();
    let data_pointer1 = file
        .write_data_to_pointer(&value1_json)
        .expect("Failed to write data");
    let root_id = file
        .insert(key1.clone(), data_pointer1, 1)
        .expect("Failed to insert");

    let result = file.search(&key1, root_id).expect("Failed to search");
    assert!(result.is_some(), "Key should be found after insert");

    let key2 = Value::Integer(20);
    let value2_json = serde_json::to_string(&Value::Text("value2".to_string())).unwrap();
    let data_pointer2 = file
        .write_data_to_pointer(&value2_json)
        .expect("Failed to write data");
    file.insert(key2.clone(), data_pointer2, root_id)
        .expect("Failed to insert second key");

    let result2 = file
        .search(&key2, root_id)
        .expect("Failed to search for second key");
    assert!(result2.is_some(), "Second key should be found");

    let deleted = file.delete(&key1, root_id).expect("Failed to delete");
    assert!(deleted, "Delete should return true for existing key");

    let result_after_delete = file
        .search(&key1, root_id)
        .expect("Failed to search after delete");
    assert!(
        result_after_delete.is_none(),
        "Key should not be found after delete"
    );

    let result2_after_delete = file
        .search(&key2, root_id)
        .expect("Failed to search for second key after delete");
    assert!(
        result2_after_delete.is_some(),
        "Second key should still exist"
    );

    remove_storage_artifacts(&temp_path);
}

#[test]
fn btree_range_scan() {
    let temp_path = std::env::temp_dir().join("rustql_btree_range_test.dat");
    remove_storage_artifacts(&temp_path);

    let mut file = BTreeFile::create(&temp_path).expect("Failed to create BTree file");

    let mut meta_page = BTreePage::new(0, PageKind::Meta);
    meta_page
        .entries
        .push(BTreeEntry::new(Value::Text("root".to_string()), 1));
    meta_page.header.entry_count = meta_page.entries.len() as u16;
    file.write_page(&meta_page)
        .expect("Failed to write meta page");

    let mut root_page = BTreePage::new(1, PageKind::Leaf);
    root_page.header.entry_count = 0;
    file.write_page(&root_page)
        .expect("Failed to write root page");

    let root_id = 1;
    for i in 1..=10 {
        let key = Value::Integer(i * 10);
        let value_json = serde_json::to_string(&Value::Text(format!("value{}", i))).unwrap();
        let data_pointer = file
            .write_data_to_pointer(&value_json)
            .unwrap_or_else(|_| panic!("Failed to write data for key {}", i));
        file.insert(key, data_pointer, root_id)
            .unwrap_or_else(|_| panic!("Failed to insert key {}", i));
    }

    let start = Value::Integer(30);
    let end = Value::Integer(70);
    let results = file
        .range_scan(Some(&start), Some(&end), root_id)
        .expect("Failed to range scan");

    assert!(results.len() >= 5, "Range scan should find multiple keys");

    remove_storage_artifacts(&temp_path);
}

#[test]
fn btree_range_scan_after_split_stays_ordered() {
    let temp_path = std::env::temp_dir().join("rustql_btree_range_split_test.dat");
    remove_storage_artifacts(&temp_path);

    let mut file = BTreeFile::create(&temp_path).expect("Failed to create BTree file");

    let mut meta_page = BTreePage::new(0, PageKind::Meta);
    meta_page
        .entries
        .push(BTreeEntry::new(Value::Text("root".to_string()), 1));
    meta_page.header.entry_count = meta_page.entries.len() as u16;
    file.write_page(&meta_page)
        .expect("Failed to write meta page");

    let mut root_page = BTreePage::new(1, PageKind::Leaf);
    root_page.header.entry_count = 0;
    file.write_page(&root_page)
        .expect("Failed to write root page");

    let mut root_id = 1;
    for i in 1..=400 {
        let key = Value::Integer(i);
        let value_json = serde_json::to_string(&Value::Text(format!("value{}", i))).unwrap();
        let data_pointer = file
            .write_data_to_pointer(&value_json)
            .unwrap_or_else(|_| panic!("Failed to write data for key {}", i));
        root_id = file
            .insert(key, data_pointer, root_id)
            .unwrap_or_else(|_| panic!("Failed to insert key {}", i));
    }

    let start = Value::Integer(101);
    let end = Value::Integer(199);
    let results = file
        .range_scan(Some(&start), Some(&end), root_id)
        .expect("Failed to range scan after split");

    assert_eq!(results.len(), 99);
    assert!(matches!(results.first(), Some((Value::Integer(101), _))));
    assert!(matches!(results.last(), Some((Value::Integer(199), _))));
    assert!(results.windows(2).all(|pair| pair[0].0 <= pair[1].0));

    remove_storage_artifacts(&temp_path);
}

#[test]
fn btree_concurrent_loads() {
    let temp_path = std::env::temp_dir().join("rustql_btree_concurrent_test.dat");
    remove_storage_artifacts(&temp_path);

    let engine = Arc::new(BTreeStorageEngine::new(&temp_path));

    let mut db = Database::new();
    let columns = vec![ColumnDefinition {
        name: "id".to_string(),
        data_type: DataType::Integer,
        nullable: false,
        primary_key: true,
        unique: false,
        default_value: None,
        foreign_key: None,
        check: None,
        auto_increment: false,
        generated: None,
    }];
    let table = Table::new(columns, vec![vec![Value::Integer(1)]], vec![]);
    db.tables.insert("test".to_string(), table);

    engine.save(&db).expect("Failed to save");

    let loaded_before = engine.load().expect("initial load failed");
    assert_eq!(loaded_before.tables.len(), 1, "Initial load should work");

    use std::thread;
    let handles: Vec<_> = (0..10)
        .map(|_| {
            let engine = Arc::clone(&engine);
            thread::spawn(move || {
                let loaded = engine.load().expect("concurrent load failed");
                assert_eq!(loaded.tables.len(), 1, "Expected 1 table");
                assert!(loaded.tables.contains_key("test"), "Expected 'test' table");
            })
        })
        .collect();

    for handle in handles {
        if let Err(e) = handle.join() {
            panic!("Thread panicked: {:?}", e);
        }
    }

    let (hits, misses, _size) = engine.cache_stats();
    assert!(hits + misses > 0, "Cache should have activity");

    remove_storage_artifacts(&temp_path);
}

#[test]
fn btree_cache_lru_eviction() {
    let temp_path = std::env::temp_dir().join("rustql_btree_cache_test.dat");
    remove_storage_artifacts(&temp_path);

    let engine = BTreeStorageEngine::new(&temp_path);

    let mut db = Database::new();
    let columns = vec![ColumnDefinition {
        name: "id".to_string(),
        data_type: DataType::Integer,
        nullable: false,
        primary_key: true,
        unique: false,
        default_value: None,
        foreign_key: None,
        check: None,
        auto_increment: false,
        generated: None,
    }];
    let table = Table::new(columns, vec![vec![Value::Integer(1)]], vec![]);
    db.tables.insert("test".to_string(), table);

    engine.save(&db).expect("Failed to save");

    let _ = engine.load();
    let (_, _, size1) = engine.cache_stats();
    assert!(size1 > 0, "Cache should have pages after load");

    engine.clear_cache();
    let (_, _, size2) = engine.cache_stats();
    assert_eq!(size2, 0, "Cache should be empty after clear");

    remove_storage_artifacts(&temp_path);
}

#[test]
fn btree_storage_rejects_invalid_header() {
    let temp_path = std::env::temp_dir().join("rustql_btree_invalid_header.dat");
    remove_storage_artifacts(&temp_path);

    std::fs::write(&temp_path, [0u8; FILE_HEADER_SIZE]).expect("failed to write invalid file");

    let engine = BTreeStorageEngine::new(&temp_path);
    let error = match engine.load() {
        Ok(_) => panic!("expected invalid header error"),
        Err(error) => error,
    };
    assert!(error.to_string().contains("invalid header"));

    remove_storage_artifacts(&temp_path);
}

#[test]
fn btree_storage_rejects_unsupported_version() {
    let temp_path = std::env::temp_dir().join("rustql_btree_unsupported_version.dat");
    remove_storage_artifacts(&temp_path);

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&temp_path)
        .expect("failed to create version test file");
    write_versioned_header(&mut file, BTreeFile::MAGIC, 999, "BTree storage file")
        .expect("failed to write test header");

    let engine = BTreeStorageEngine::new(&temp_path);
    let error = match engine.load() {
        Ok(_) => panic!("expected unsupported version error"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("Unsupported BTree storage file format version")
    );

    remove_storage_artifacts(&temp_path);
}

#[test]
fn btree_storage_corruptions_do_not_panic() {
    let temp_path = std::env::temp_dir().join("rustql_btree_corruption_fuzz.dat");
    remove_storage_artifacts(&temp_path);

    let engine = BTreeStorageEngine::new(&temp_path);
    let mut db = Database::new();
    db.tables.insert(
        "test".to_string(),
        Table::new(
            vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: true,
                    unique: false,
                    default_value: None,
                    foreign_key: None,
                    check: None,
                    auto_increment: false,
                    generated: None,
                },
                ColumnDefinition {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    default_value: None,
                    foreign_key: None,
                    check: None,
                    auto_increment: false,
                    generated: None,
                },
            ],
            vec![
                vec![Value::Integer(1), Value::Text("Alice".to_string())],
                vec![Value::Integer(2), Value::Text("Bob".to_string())],
            ],
            vec![],
        ),
    );
    engine.save(&db).expect("failed to save baseline database");

    let baseline = std::fs::read(&temp_path).expect("failed to read baseline storage file");
    assert!(
        baseline.len() > FILE_HEADER_SIZE,
        "baseline file should include header and pages"
    );

    fn next_random(state: &mut u64) -> u64 {
        *state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *state
    }

    for case in 0..64u64 {
        let mut mutated = baseline.clone();
        let mut state = case + 1;
        let mutations = 1 + (next_random(&mut state) % 4) as usize;
        for _ in 0..mutations {
            let index = (next_random(&mut state) as usize) % mutated.len();
            let mask = ((next_random(&mut state) as u8) | 1).max(1);
            mutated[index] ^= mask;
        }
        std::fs::write(&temp_path, &mutated).expect("failed to write mutated storage file");

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mutated_engine = BTreeStorageEngine::new(&temp_path);
            let _ = mutated_engine.load();
        }));
        assert!(result.is_ok(), "load panicked on corruption case {}", case);
    }

    remove_storage_artifacts(&temp_path);
}

#[test]
fn btree_recovers_committed_journal_on_load() {
    let temp_path = std::env::temp_dir().join("rustql_btree_committed_journal.dat");
    remove_storage_artifacts(&temp_path);

    let engine = BTreeStorageEngine::new(&temp_path);

    let mut db = Database::new();
    db.tables.insert(
        "test".to_string(),
        Table::new(
            vec![ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            }],
            vec![vec![Value::Integer(1)]],
            vec![],
        ),
    );
    engine.save(&db).expect("failed to save base database");

    let mut committed = db.clone();
    committed.tables.get_mut("test").unwrap().rows = vec![vec![Value::Integer(2)]];
    engine
        .prepare_commit(&committed)
        .expect("failed to write committed journal");

    let loaded = engine.load().expect("failed to recover committed journal");
    assert_eq!(loaded.tables["test"].rows, committed.tables["test"].rows);
    assert!(
        !engine.journal_path().exists(),
        "journal should be cleared after recovery"
    );

    let loaded_again = engine.load().expect("failed to reload recovered database");
    assert_eq!(
        loaded_again.tables["test"].rows,
        committed.tables["test"].rows
    );

    remove_storage_artifacts(&temp_path);
}

#[test]
fn btree_committed_journal_stores_page_redo_frames() {
    let temp_path = std::env::temp_dir().join("rustql_btree_redo_journal_shape.dat");
    remove_storage_artifacts(&temp_path);

    let engine = BTreeStorageEngine::new(&temp_path);

    let mut db = Database::new();
    db.tables.insert(
        "test".to_string(),
        Table::new(
            vec![ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            }],
            vec![vec![Value::Integer(1)]],
            vec![],
        ),
    );
    engine.save(&db).expect("failed to save base database");

    let mut committed = db.clone();
    committed.tables.get_mut("test").unwrap().rows = vec![vec![Value::Integer(2)]];
    engine
        .prepare_commit(&committed)
        .expect("failed to prepare committed journal");

    let journal_bytes = std::fs::read(engine.journal_path()).expect("failed to read journal");
    assert_eq!(&journal_bytes[0..8], &JOURNAL_MAGIC);
    assert_eq!(
        u32::from_le_bytes([
            journal_bytes[8],
            journal_bytes[9],
            journal_bytes[10],
            journal_bytes[11],
        ]),
        JOURNAL_VERSION
    );

    let payload = &journal_bytes[FILE_HEADER_SIZE..];
    let payload_text = std::str::from_utf8(payload).expect("journal payload should be JSON");
    assert!(payload_text.contains("\"redo\""));
    assert!(payload_text.contains("\"frames\""));
    assert!(!payload_text.contains("\"database\""));
    assert!(!payload_text.contains("\"tables\""));

    let journal: TransactionJournal =
        serde_json::from_slice(payload).expect("failed to decode redo journal");
    let redo = match journal {
        TransactionJournal::Committed { redo } => redo,
        TransactionJournal::Pending => panic!("expected committed journal"),
    };
    redo.validate().expect("redo journal should validate");
    assert_eq!(redo.page_size, BTREE_PAGE_SIZE);
    assert!(!redo.frames.is_empty());

    let target_page_count =
        (redo.target_file_len - FILE_HEADER_SIZE as u64) / BTREE_PAGE_SIZE as u64;
    assert!(
        redo.frames.len() < target_page_count as usize,
        "a one-row update should journal only changed pages"
    );

    remove_storage_artifacts(&temp_path);
}

#[test]
fn btree_committed_journal_is_cleared_when_data_file_already_reached_target() {
    let temp_path = std::env::temp_dir().join("rustql_btree_committed_journal_target.dat");
    remove_storage_artifacts(&temp_path);

    let engine = BTreeStorageEngine::new(&temp_path);

    let mut db = Database::new();
    db.tables.insert(
        "test".to_string(),
        Table::new(
            vec![ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            }],
            vec![vec![Value::Integer(1)]],
            vec![],
        ),
    );
    engine.save(&db).expect("failed to save base database");

    let mut committed = db.clone();
    committed.tables.get_mut("test").unwrap().rows = vec![vec![Value::Integer(2)]];
    engine
        .prepare_commit(&committed)
        .expect("failed to prepare committed journal");
    engine
        .save(&committed)
        .expect("failed to persist committed image");

    let loaded = engine
        .load()
        .expect("failed to load already committed image");
    assert_eq!(loaded.tables["test"].rows, committed.tables["test"].rows);
    assert!(
        !engine.journal_path().exists(),
        "journal should be cleared if data file already matches target"
    );

    remove_storage_artifacts(&temp_path);
}

#[test]
fn btree_recovers_legacy_v1_committed_journal_on_load() {
    let temp_path = std::env::temp_dir().join("rustql_btree_legacy_committed_journal.dat");
    remove_storage_artifacts(&temp_path);

    let engine = BTreeStorageEngine::new(&temp_path);

    let mut db = Database::new();
    db.tables.insert(
        "test".to_string(),
        Table::new(
            vec![ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            }],
            vec![vec![Value::Integer(1)]],
            vec![],
        ),
    );
    engine.save(&db).expect("failed to save base database");

    let mut committed = db.clone();
    committed.tables.get_mut("test").unwrap().rows = vec![vec![Value::Integer(2)]];

    let payload = serde_json::to_vec(&LegacyTransactionJournal::Committed {
        database: committed.clone(),
    })
    .expect("failed to encode legacy journal");
    let mut data = Vec::with_capacity(FILE_HEADER_SIZE + payload.len());
    data.extend_from_slice(&JOURNAL_MAGIC);
    data.extend_from_slice(&LEGACY_JOURNAL_VERSION.to_le_bytes());
    data.extend_from_slice(&HEADER_RESERVED.to_le_bytes());
    data.extend_from_slice(&payload);
    atomic_write(&engine.journal_path(), &data).expect("failed to write legacy journal");

    let loaded = engine
        .load()
        .expect("failed to recover legacy committed journal");
    assert_eq!(loaded.tables["test"].rows, committed.tables["test"].rows);
    assert!(
        !engine.journal_path().exists(),
        "legacy journal should be cleared after recovery"
    );

    remove_storage_artifacts(&temp_path);
}

#[test]
fn btree_discards_pending_journal_on_load() {
    let temp_path = std::env::temp_dir().join("rustql_btree_pending_journal.dat");
    remove_storage_artifacts(&temp_path);

    let engine = BTreeStorageEngine::new(&temp_path);

    let mut db = Database::new();
    db.tables.insert(
        "test".to_string(),
        Table::new(
            vec![ColumnDefinition {
                name: "id".to_string(),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: true,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            }],
            vec![vec![Value::Integer(1)]],
            vec![],
        ),
    );
    engine.save(&db).expect("failed to save base database");
    engine
        .write_journal_locked(&TransactionJournal::Pending)
        .expect("failed to write pending journal");

    let loaded = engine.load().expect("failed to load database");
    assert_eq!(loaded.tables["test"].rows, db.tables["test"].rows);
    assert!(
        !engine.journal_path().exists(),
        "pending journal should be cleared during load"
    );

    remove_storage_artifacts(&temp_path);
}

#[test]
fn btree_persists_views_constraints_and_composite_indexes() {
    let temp_path = std::env::temp_dir().join("rustql_btree_metadata_roundtrip.dat");
    remove_storage_artifacts(&temp_path);

    let engine = BTreeStorageEngine::new(&temp_path);

    let mut db = Database::new();
    db.tables.insert(
        "orders".to_string(),
        Table::new(
            vec![
                ColumnDefinition {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: true,
                    unique: false,
                    default_value: None,
                    foreign_key: None,
                    check: None,
                    auto_increment: false,
                    generated: None,
                },
                ColumnDefinition {
                    name: "customer_id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    default_value: None,
                    foreign_key: None,
                    check: None,
                    auto_increment: false,
                    generated: None,
                },
            ],
            vec![vec![Value::Integer(1), Value::Integer(10)]],
            vec![TableConstraint::Unique {
                name: Some("orders_customer_unique".to_string()),
                columns: vec!["customer_id".to_string()],
            }],
        ),
    );
    db.views.insert(
        "order_ids".to_string(),
        View {
            name: "order_ids".to_string(),
            query_sql: "SELECT id FROM orders".to_string(),
        },
    );
    db.composite_indexes.insert(
        "orders_customer_idx".to_string(),
        CompositeIndex {
            name: "orders_customer_idx".to_string(),
            table: "orders".to_string(),
            columns: vec!["id".to_string(), "customer_id".to_string()],
            entries: std::collections::BTreeMap::from([(
                vec![Value::Integer(1), Value::Integer(10)],
                vec![RowId(1)],
            )]),
            filter_expr: None,
        },
    );

    engine.save(&db).expect("failed to save metadata database");
    let loaded = engine.load().expect("failed to load metadata database");

    assert_eq!(
        loaded.tables["orders"].constraints,
        db.tables["orders"].constraints
    );
    assert_eq!(loaded.views["order_ids"].query_sql, "SELECT id FROM orders");
    assert!(loaded.composite_indexes.contains_key("orders_customer_idx"));

    remove_storage_artifacts(&temp_path);
}
