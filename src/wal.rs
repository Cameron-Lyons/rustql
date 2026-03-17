use crate::ast::{ColumnDefinition, TableConstraint, Value};
use crate::database::{CompositeIndex, Database, Index, Table};
use crate::engine::{ExecutionContext, WalGuard};
use crate::error::RustqlError;
use std::collections::HashMap;

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
        constraints: Vec<TableConstraint>,
        indexes: Vec<Index>,
        composite_indexes: Vec<CompositeIndex>,
    },
    CreateIndex {
        name: String,
    },
    DropIndex {
        name: String,
        index: Option<Index>,
        composite_index: Box<Option<CompositeIndex>>,
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
    TruncateTable {
        name: String,
        old_rows: Vec<Vec<Value>>,
    },
    CreateView {
        name: String,
    },
    DropView {
        name: String,
        view: crate::database::View,
    },
    AlterRenameTable {
        old_name: String,
        new_name: String,
    },
    AlterAddConstraint {
        table: String,
        constraint: TableConstraint,
    },
    AlterDropConstraint {
        table: String,
        constraint: TableConstraint,
    },
}

#[derive(Debug, Default)]
pub struct WalLog {
    entries: Vec<WalEntry>,
    savepoints: HashMap<String, usize>,
}

impl WalLog {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, entry: WalEntry) {
        self.entries.push(entry);
    }

    pub fn savepoint(&mut self, name: &str) {
        self.savepoints.insert(name.to_string(), self.entries.len());
    }

    pub fn release_savepoint(&mut self, name: &str) -> Result<(), RustqlError> {
        self.savepoints.remove(name).ok_or_else(|| {
            RustqlError::TransactionError(format!("Savepoint '{}' does not exist", name))
        })?;
        Ok(())
    }

    pub fn rollback_to_savepoint(
        &mut self,
        name: &str,
        db: &mut Database,
    ) -> Result<(), RustqlError> {
        let position = self.savepoints.get(name).copied().ok_or_else(|| {
            RustqlError::TransactionError(format!("Savepoint '{}' does not exist", name))
        })?;
        let entries_to_rollback: Vec<WalEntry> = self.entries.drain(position..).collect();
        for entry in entries_to_rollback.into_iter().rev() {
            rollback_single_entry(entry, db);
        }
        rebuild_all_indexes(db);
        Ok(())
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
                    constraints,
                    indexes,
                    composite_indexes,
                } => {
                    db.tables.insert(
                        name,
                        Table {
                            columns,
                            rows,
                            constraints,
                        },
                    );
                    for index in indexes {
                        db.indexes.insert(index.name.clone(), index);
                    }
                    for index in composite_indexes {
                        db.composite_indexes.insert(index.name.clone(), index);
                    }
                }
                WalEntry::CreateIndex { name } => {
                    db.indexes.remove(&name);
                    db.composite_indexes.remove(&name);
                }
                WalEntry::DropIndex {
                    name,
                    index,
                    composite_index,
                } => {
                    if let Some(index) = index {
                        db.indexes.insert(name.clone(), index);
                    }
                    if let Some(index) = *composite_index {
                        db.composite_indexes.insert(name, index);
                    }
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
                WalEntry::TruncateTable { name, old_rows } => {
                    if let Some(t) = db.tables.get_mut(&name) {
                        t.rows = old_rows;
                    }
                }
                WalEntry::CreateView { name } => {
                    db.views.remove(&name);
                }
                WalEntry::DropView { name, view } => {
                    db.views.insert(name, view);
                }
                WalEntry::AlterRenameTable { old_name, new_name } => {
                    if let Some(table_data) = db.tables.remove(&new_name) {
                        db.tables.insert(old_name.clone(), table_data);
                    }
                    for index in db.indexes.values_mut() {
                        if index.table == new_name {
                            index.table = old_name.clone();
                        }
                    }
                    for ci in db.composite_indexes.values_mut() {
                        if ci.table == new_name {
                            ci.table = old_name.clone();
                        }
                    }
                }
                WalEntry::AlterAddConstraint { table, .. } => {
                    if let Some(t) = db.tables.get_mut(&table) {
                        t.constraints.pop();
                    }
                }
                WalEntry::AlterDropConstraint {
                    table, constraint, ..
                } => {
                    if let Some(t) = db.tables.get_mut(&table) {
                        t.constraints.push(constraint);
                    }
                }
            }
        }

        rebuild_all_indexes(db);
        Ok(())
    }
}

fn rollback_single_entry(entry: WalEntry, db: &mut Database) {
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
            constraints,
            indexes,
            composite_indexes,
        } => {
            db.tables.insert(
                name,
                Table {
                    columns,
                    rows,
                    constraints,
                },
            );
            for index in indexes {
                db.indexes.insert(index.name.clone(), index);
            }
            for index in composite_indexes {
                db.composite_indexes.insert(index.name.clone(), index);
            }
        }
        WalEntry::CreateIndex { name } => {
            db.indexes.remove(&name);
            db.composite_indexes.remove(&name);
        }
        WalEntry::DropIndex {
            name,
            index,
            composite_index,
        } => {
            if let Some(index) = index {
                db.indexes.insert(name.clone(), index);
            }
            if let Some(index) = *composite_index {
                db.composite_indexes.insert(name, index);
            }
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
        WalEntry::TruncateTable { name, old_rows } => {
            if let Some(t) = db.tables.get_mut(&name) {
                t.rows = old_rows;
            }
        }
        WalEntry::CreateView { name } => {
            db.views.remove(&name);
        }
        WalEntry::DropView { name, view } => {
            db.views.insert(name, view);
        }
        WalEntry::AlterRenameTable { old_name, new_name } => {
            if let Some(table_data) = db.tables.remove(&new_name) {
                db.tables.insert(old_name.clone(), table_data);
            }
            for index in db.indexes.values_mut() {
                if index.table == new_name {
                    index.table = old_name.clone();
                }
            }
            for ci in db.composite_indexes.values_mut() {
                if ci.table == new_name {
                    ci.table = old_name.clone();
                }
            }
        }
        WalEntry::AlterAddConstraint { table, .. } => {
            if let Some(t) = db.tables.get_mut(&table) {
                t.constraints.pop();
            }
        }
        WalEntry::AlterDropConstraint {
            table, constraint, ..
        } => {
            if let Some(t) = db.tables.get_mut(&table) {
                t.constraints.push(constraint);
            }
        }
    }
}

fn rebuild_all_indexes(db: &mut Database) {
    let db_snapshot = db.clone();
    for index in db.indexes.values_mut() {
        index.entries.clear();
        if let Some(table) = db.tables.get(&index.table)
            && let Some(col_idx) = table.columns.iter().position(|c| c.name == index.column)
        {
            for (row_idx, row) in table.rows.iter().enumerate() {
                if !crate::executor::ddl::row_matches_index_filter(
                    &db_snapshot,
                    table,
                    index.filter_expr.as_ref(),
                    row,
                ) {
                    continue;
                }
                let value = row.get(col_idx).cloned().unwrap_or(Value::Null);
                index.entries.entry(value).or_default().push(row_idx);
            }
        }
    }

    for index in db.composite_indexes.values_mut() {
        index.entries.clear();
        if let Some(table) = db.tables.get(&index.table) {
            let column_positions: Option<Vec<usize>> = index
                .columns
                .iter()
                .map(|column| table.columns.iter().position(|c| c.name == *column))
                .collect();

            if let Some(column_positions) = column_positions {
                for (row_idx, row) in table.rows.iter().enumerate() {
                    if !crate::executor::ddl::row_matches_index_filter(
                        &db_snapshot,
                        table,
                        index.filter_expr.as_ref(),
                        row,
                    ) {
                        continue;
                    }
                    let key = column_positions
                        .iter()
                        .map(|&col_idx| row.get(col_idx).cloned().unwrap_or(Value::Null))
                        .collect();
                    index.entries.entry(key).or_default().push(row_idx);
                }
            }
        }
    }
}

fn get_wal_lock(ctx: &ExecutionContext) -> WalGuard {
    ctx.lock_wal()
}

pub(crate) fn begin_transaction(ctx: &ExecutionContext) -> Result<(), RustqlError> {
    let mut wal = get_wal_lock(ctx);
    if wal.is_some() {
        return Err(RustqlError::TransactionError(
            "Transaction already in progress".to_string(),
        ));
    }
    *wal = Some(WalLog::new());
    Ok(())
}

pub(crate) fn commit_transaction(ctx: &ExecutionContext) -> Result<(), RustqlError> {
    let mut wal = get_wal_lock(ctx);
    if wal.is_none() {
        return Err(RustqlError::TransactionError(
            "No transaction in progress".to_string(),
        ));
    }
    *wal = None;
    Ok(())
}

pub(crate) fn rollback_transaction(
    ctx: &ExecutionContext,
    db: &mut Database,
) -> Result<(), RustqlError> {
    let mut wal_guard = get_wal_lock(ctx);
    match wal_guard.take() {
        Some(wal_log) => wal_log.rollback(db),
        None => Err(RustqlError::TransactionError(
            "No transaction in progress".to_string(),
        )),
    }
}

pub(crate) fn is_in_transaction(ctx: &ExecutionContext) -> bool {
    get_wal_lock(ctx).is_some()
}

pub(crate) fn record_wal_entry(ctx: &ExecutionContext, entry: WalEntry) {
    let mut wal = get_wal_lock(ctx);
    if let Some(ref mut log) = *wal {
        log.record(entry);
    }
}

pub(crate) fn savepoint(ctx: &ExecutionContext, name: &str) -> Result<(), RustqlError> {
    let mut wal = get_wal_lock(ctx);
    if wal.is_none() {
        *wal = Some(WalLog::new());
    }
    if let Some(ref mut log) = *wal {
        log.savepoint(name);
    }
    Ok(())
}

pub(crate) fn release_savepoint(ctx: &ExecutionContext, name: &str) -> Result<(), RustqlError> {
    let mut wal = get_wal_lock(ctx);
    match wal.as_mut() {
        Some(log) => log.release_savepoint(name),
        None => Err(RustqlError::TransactionError(
            "No transaction in progress".to_string(),
        )),
    }
}

pub(crate) fn rollback_to_savepoint(
    ctx: &ExecutionContext,
    name: &str,
    db: &mut Database,
) -> Result<(), RustqlError> {
    let mut wal = get_wal_lock(ctx);
    match wal.as_mut() {
        Some(log) => log.rollback_to_savepoint(name, db),
        None => Err(RustqlError::TransactionError(
            "No transaction in progress".to_string(),
        )),
    }
}
