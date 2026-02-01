use crate::ast::{ColumnDefinition, Value};
use crate::database::{Database, Index, Table};
use crate::error::RustqlError;
use std::sync::{Mutex, OnceLock};

#[derive(Debug, Clone)]
pub enum WalEntry {
    InsertRow {
        table: String,
        row_index: usize,
    },
    UpdateRow {
        table: String,
        row_index: usize,
        old_row: Vec<Value>,
    },
    DeleteRow {
        table: String,
        row_index: usize,
        old_row: Vec<Value>,
    },
    CreateTable {
        name: String,
    },
    DropTable {
        name: String,
        columns: Vec<ColumnDefinition>,
        rows: Vec<Vec<Value>>,
    },
    CreateIndex {
        name: String,
    },
    DropIndex {
        name: String,
        index: Index,
    },
    AlterAddColumn {
        table: String,
        column_name: String,
    },
    AlterDropColumn {
        table: String,
        col_index: usize,
        column: ColumnDefinition,
        values: Vec<Value>,
    },
    AlterRenameColumn {
        table: String,
        old_name: String,
        new_name: String,
    },
}

#[derive(Debug, Default)]
pub struct WalLog {
    entries: Vec<WalEntry>,
}

impl WalLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, entry: WalEntry) {
        self.entries.push(entry);
    }

    pub fn rollback(self, db: &mut Database) -> Result<(), RustqlError> {
        for entry in self.entries.into_iter().rev() {
            match entry {
                WalEntry::InsertRow { table, row_index } => {
                    if let Some(t) = db.tables.get_mut(&table)
                        && row_index < t.rows.len()
                    {
                        t.rows.remove(row_index);
                    }
                }
                WalEntry::UpdateRow {
                    table,
                    row_index,
                    old_row,
                } => {
                    if let Some(t) = db.tables.get_mut(&table)
                        && row_index < t.rows.len()
                    {
                        t.rows[row_index] = old_row;
                    }
                }
                WalEntry::DeleteRow {
                    table,
                    row_index,
                    old_row,
                } => {
                    if let Some(t) = db.tables.get_mut(&table)
                        && row_index <= t.rows.len()
                    {
                        t.rows.insert(row_index, old_row);
                    }
                }
                WalEntry::CreateTable { name } => {
                    db.tables.remove(&name);
                }
                WalEntry::DropTable {
                    name,
                    columns,
                    rows,
                } => {
                    db.tables.insert(name, Table { columns, rows });
                }
                WalEntry::CreateIndex { name } => {
                    db.indexes.remove(&name);
                }
                WalEntry::DropIndex { name, index } => {
                    db.indexes.insert(name, index);
                }
                WalEntry::AlterAddColumn {
                    table,
                    column_name: _,
                } => {
                    if let Some(t) = db.tables.get_mut(&table) {
                        t.columns.pop();
                        for row in &mut t.rows {
                            row.pop();
                        }
                    }
                }
                WalEntry::AlterDropColumn {
                    table,
                    col_index,
                    column,
                    values,
                } => {
                    if let Some(t) = db.tables.get_mut(&table) {
                        t.columns.insert(col_index, column);
                        for (row, val) in t.rows.iter_mut().zip(values) {
                            row.insert(col_index, val);
                        }
                    }
                }
                WalEntry::AlterRenameColumn {
                    table,
                    old_name,
                    new_name,
                } => {
                    if let Some(t) = db.tables.get_mut(&table) {
                        for col in &mut t.columns {
                            if col.name == new_name {
                                col.name = old_name;
                                break;
                            }
                        }
                    }
                }
            }
        }

        rebuild_all_indexes(db);
        Ok(())
    }
}

fn rebuild_all_indexes(db: &mut Database) {
    for index in db.indexes.values_mut() {
        index.entries.clear();
        if let Some(table) = db.tables.get(&index.table)
            && let Some(col_idx) = table.columns.iter().position(|c| c.name == index.column)
        {
            for (row_idx, row) in table.rows.iter().enumerate() {
                let value = row.get(col_idx).cloned().unwrap_or(Value::Null);
                index.entries.entry(value).or_default().push(row_idx);
            }
        }
    }
}

static TRANSACTION_WAL: OnceLock<Mutex<Option<WalLog>>> = OnceLock::new();

fn get_wal_lock() -> &'static Mutex<Option<WalLog>> {
    TRANSACTION_WAL.get_or_init(|| Mutex::new(None))
}

pub fn begin_transaction() -> Result<(), RustqlError> {
    let mut wal = get_wal_lock().lock().unwrap();
    if wal.is_some() {
        return Err(RustqlError::TransactionError(
            "Transaction already in progress".to_string(),
        ));
    }
    *wal = Some(WalLog::new());
    Ok(())
}

pub fn commit_transaction() -> Result<(), RustqlError> {
    let mut wal = get_wal_lock().lock().unwrap();
    if wal.is_none() {
        return Err(RustqlError::TransactionError(
            "No transaction in progress".to_string(),
        ));
    }
    *wal = None;
    Ok(())
}

pub fn rollback_transaction(db: &mut Database) -> Result<(), RustqlError> {
    let mut wal_guard = get_wal_lock().lock().unwrap();
    match wal_guard.take() {
        Some(wal_log) => wal_log.rollback(db),
        None => Err(RustqlError::TransactionError(
            "No transaction in progress".to_string(),
        )),
    }
}

pub fn is_in_transaction() -> bool {
    get_wal_lock().lock().unwrap().is_some()
}

pub fn record_wal_entry(entry: WalEntry) {
    let mut wal = get_wal_lock().lock().unwrap();
    if let Some(ref mut log) = *wal {
        log.record(entry);
    }
}

pub fn reset_wal_state() {
    let mut wal = get_wal_lock().lock().unwrap();
    *wal = None;
}
