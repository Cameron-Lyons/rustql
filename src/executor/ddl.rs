use crate::ast::*;
use crate::database::{Database, Table};
use crate::error::RustqlError;
use crate::wal::{self, WalEntry};
use std::collections::{BTreeMap, HashSet};

use super::{get_database_read, get_database_write, save_if_not_in_transaction};

pub fn execute_create_table(stmt: CreateTableStatement) -> Result<String, RustqlError> {
    if let Some(source_query) = stmt.as_query {
        return execute_create_table_as_select(stmt.name, *source_query);
    }

    let mut db = get_database_write();
    if db.tables.contains_key(&stmt.name) {
        return Err(RustqlError::TableAlreadyExists(stmt.name.clone()));
    }
    db.tables.insert(
        stmt.name.clone(),
        Table {
            columns: stmt.columns,
            rows: Vec::new(),
        },
    );
    wal::record_wal_entry(WalEntry::CreateTable {
        name: stmt.name.clone(),
    });
    save_if_not_in_transaction(&db)?;
    Ok(format!("Table '{}' created", stmt.name))
}

fn execute_create_table_as_select(
    name: String,
    query: crate::ast::SelectStatement,
) -> Result<String, RustqlError> {
    let result = {
        let db = get_database_read();
        super::select::execute_select_internal(query, &db)?
    };

    let columns: Vec<crate::ast::ColumnDefinition> = result
        .headers
        .iter()
        .enumerate()
        .map(|(idx, header)| {
            let data_type = result
                .rows
                .first()
                .and_then(|row| row.get(idx))
                .map(|val| match val {
                    crate::ast::Value::Integer(_) => crate::ast::DataType::Integer,
                    crate::ast::Value::Float(_) => crate::ast::DataType::Float,
                    crate::ast::Value::Boolean(_) => crate::ast::DataType::Boolean,
                    crate::ast::Value::Date(_) => crate::ast::DataType::Date,
                    crate::ast::Value::Time(_) => crate::ast::DataType::Time,
                    crate::ast::Value::DateTime(_) => crate::ast::DataType::DateTime,
                    _ => crate::ast::DataType::Text,
                })
                .unwrap_or(crate::ast::DataType::Text);
            crate::ast::ColumnDefinition {
                name: header.clone(),
                data_type,
                nullable: true,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
            }
        })
        .collect();

    let mut db = get_database_write();
    if db.tables.contains_key(&name) {
        return Err(RustqlError::TableAlreadyExists(name));
    }
    db.tables.insert(
        name.clone(),
        Table {
            columns,
            rows: result.rows,
        },
    );
    wal::record_wal_entry(WalEntry::CreateTable { name: name.clone() });
    save_if_not_in_transaction(&db)?;
    Ok(format!("Table '{}' created", name))
}

pub fn execute_drop_table(stmt: DropTableStatement) -> Result<String, RustqlError> {
    let mut db = get_database_write();
    if let Some(removed) = db.tables.remove(&stmt.name) {
        wal::record_wal_entry(WalEntry::DropTable {
            name: stmt.name.clone(),
            columns: removed.columns,
            rows: removed.rows,
        });
        save_if_not_in_transaction(&db)?;
        Ok(format!("Table '{}' dropped", stmt.name))
    } else {
        Err(RustqlError::TableNotFound(stmt.name.clone()))
    }
}

pub fn execute_alter_table(stmt: AlterTableStatement) -> Result<String, RustqlError> {
    let mut db = get_database_write();
    let table = db
        .tables
        .get_mut(&stmt.table)
        .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;

    match stmt.operation {
        AlterOperation::AddColumn(col_def) => {
            if table.columns.iter().any(|c| c.name == col_def.name) {
                return Err(RustqlError::Internal(format!(
                    "Column '{}' already exists",
                    col_def.name
                )));
            }
            table.columns.push(col_def.clone());
            let default_value = match col_def.data_type {
                DataType::Integer => Value::Integer(0),
                DataType::Float => Value::Float(0.0),
                DataType::Text => Value::Text(String::new()),
                DataType::Boolean => Value::Boolean(false),
                DataType::Date => Value::Date("1970-01-01".to_string()),
                DataType::Time => Value::Time("00:00:00".to_string()),
                DataType::DateTime => Value::DateTime("1970-01-01 00:00:00".to_string()),
            };
            for row in &mut table.rows {
                row.push(default_value.clone());
            }
            wal::record_wal_entry(WalEntry::AlterAddColumn {
                table: stmt.table.clone(),
                column_name: col_def.name.clone(),
            });
            save_if_not_in_transaction(&db)?;
            Ok(format!(
                "Column '{}' added to table '{}'",
                col_def.name, stmt.table
            ))
        }
        AlterOperation::DropColumn(col_name) => {
            let col_index = table
                .columns
                .iter()
                .position(|c| c.name == col_name)
                .ok_or_else(|| format!("Column '{}' does not exist", col_name))?;
            let removed_col = table.columns.remove(col_index);
            let mut removed_values = Vec::new();
            for row in &mut table.rows {
                if col_index < row.len() {
                    removed_values.push(row.remove(col_index));
                }
            }
            wal::record_wal_entry(WalEntry::AlterDropColumn {
                table: stmt.table.clone(),
                col_index,
                column: removed_col,
                values: removed_values,
            });
            save_if_not_in_transaction(&db)?;
            Ok(format!(
                "Column '{}' dropped from table '{}'",
                col_name, stmt.table
            ))
        }
        AlterOperation::RenameColumn { old, new } => {
            let col_exists = table.columns.iter().any(|c| c.name == old);
            if !col_exists {
                return Err(RustqlError::ColumnNotFound(old.clone()));
            }
            if table.columns.iter().any(|c| c.name == new && c.name != old) {
                return Err(RustqlError::Internal(format!(
                    "Column '{}' already exists",
                    new
                )));
            }
            for column in &mut table.columns {
                if column.name == old {
                    column.name = new.clone();
                    break;
                }
            }
            wal::record_wal_entry(WalEntry::AlterRenameColumn {
                table: stmt.table.clone(),
                old_name: old.clone(),
                new_name: new.clone(),
            });
            save_if_not_in_transaction(&db)?;
            Ok(format!(
                "Column '{}' renamed to '{}' in table '{}'",
                old, new, stmt.table
            ))
        }
    }
}

pub fn execute_create_index(stmt: CreateIndexStatement) -> Result<String, RustqlError> {
    let mut db = get_database_write();

    if db.indexes.contains_key(&stmt.name) {
        return Err(RustqlError::IndexError(format!(
            "Index '{}' already exists",
            stmt.name
        )));
    }

    let first_column = stmt
        .columns
        .first()
        .ok_or_else(|| RustqlError::Internal("Index must have at least one column".to_string()))?
        .clone();

    let mut indexes_to_insert: Vec<(String, crate::database::Index)> = Vec::new();

    {
        let table = db
            .tables
            .get(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;

        let col_idx = table
            .columns
            .iter()
            .position(|col| col.name == first_column)
            .ok_or_else(|| {
                format!(
                    "Column '{}' does not exist in table '{}'",
                    first_column, stmt.table
                )
            })?;

        let mut index = crate::database::Index {
            name: stmt.name.clone(),
            table: stmt.table.clone(),
            column: first_column.clone(),
            entries: BTreeMap::new(),
        };

        for (row_idx, row) in table.rows.iter().enumerate() {
            let value = row.get(col_idx).cloned().unwrap_or(Value::Null);
            index.entries.entry(value).or_default().push(row_idx);
        }

        indexes_to_insert.push((stmt.name.clone(), index));

        for (i, col_name) in stmt.columns.iter().enumerate().skip(1) {
            let extra_col_idx = table
                .columns
                .iter()
                .position(|col| col.name == *col_name)
                .ok_or_else(|| {
                    format!(
                        "Column '{}' does not exist in table '{}'",
                        col_name, stmt.table
                    )
                })?;

            let extra_index_name = format!("{}_{}", stmt.name, i + 1);
            let mut extra_index = crate::database::Index {
                name: extra_index_name.clone(),
                table: stmt.table.clone(),
                column: col_name.clone(),
                entries: BTreeMap::new(),
            };

            for (row_idx, row) in table.rows.iter().enumerate() {
                let value = row.get(extra_col_idx).cloned().unwrap_or(Value::Null);
                extra_index.entries.entry(value).or_default().push(row_idx);
            }

            indexes_to_insert.push((extra_index_name, extra_index));
        }
    }

    for (index_name, index) in indexes_to_insert {
        db.indexes.insert(index_name.clone(), index);
        wal::record_wal_entry(WalEntry::CreateIndex { name: index_name });
    }

    save_if_not_in_transaction(&db)?;
    let columns_str = stmt.columns.join(", ");
    Ok(format!(
        "Index '{}' created on {}.{}",
        stmt.name, stmt.table, columns_str
    ))
}

pub fn execute_drop_index(stmt: DropIndexStatement) -> Result<String, RustqlError> {
    let mut db = get_database_write();
    if let Some(removed) = db.indexes.remove(&stmt.name) {
        wal::record_wal_entry(WalEntry::DropIndex {
            name: stmt.name.clone(),
            index: removed,
        });
        save_if_not_in_transaction(&db)?;
        Ok(format!("Index '{}' dropped", stmt.name))
    } else {
        Err(RustqlError::IndexError(format!(
            "Index '{}' does not exist",
            stmt.name
        )))
    }
}

pub fn execute_describe(table_name: String) -> Result<String, RustqlError> {
    let db = get_database_read();
    let table = db
        .tables
        .get(&table_name)
        .ok_or_else(|| RustqlError::TableNotFound(table_name.to_string()))?;

    let mut result = String::new();
    result.push_str("Column\tType\tNullable\tPrimary Key\tUnique\tDefault\n");
    result.push_str(&"-".repeat(70));
    result.push('\n');

    for col in &table.columns {
        let type_str = match col.data_type {
            DataType::Integer => "INTEGER",
            DataType::Float => "FLOAT",
            DataType::Text => "TEXT",
            DataType::Boolean => "BOOLEAN",
            DataType::Date => "DATE",
            DataType::Time => "TIME",
            DataType::DateTime => "DATETIME",
        };

        let nullable_str = if col.nullable { "YES" } else { "NO" };
        let pk_str = if col.primary_key { "YES" } else { "NO" };
        let unique_str = if col.unique { "YES" } else { "NO" };
        let default_str = if let Some(ref default) = col.default_value {
            super::expr::format_value(default)
        } else {
            "NULL".to_string()
        };

        result.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\t{}\n",
            col.name, type_str, nullable_str, pk_str, unique_str, default_str
        ));
    }

    Ok(result)
}

pub fn execute_show_tables() -> Result<String, RustqlError> {
    let db = get_database_read();
    let mut result = String::new();
    result.push_str("Tables\n");
    result.push_str(&"-".repeat(40));
    result.push('\n');

    let mut table_names: Vec<&String> = db.tables.keys().collect();
    table_names.sort();

    for table_name in table_names {
        result.push_str(table_name);
        result.push('\n');
    }

    Ok(result)
}

pub fn execute_analyze(table_name: String) -> Result<String, RustqlError> {
    let db = get_database_read();
    let table = db
        .tables
        .get(&table_name)
        .ok_or_else(|| RustqlError::TableNotFound(table_name.clone()))?;

    let row_count = table.rows.len();
    let mut result = format!("Table: {}\nRow count: {}\n", table_name, row_count);
    result.push_str("Column\tDistinct Count\n");
    result.push_str(&"-".repeat(40));
    result.push('\n');

    for (col_idx, col_def) in table.columns.iter().enumerate() {
        let distinct: std::collections::BTreeSet<&Value> = table
            .rows
            .iter()
            .filter_map(|row| row.get(col_idx))
            .collect();
        result.push_str(&format!("{}\t{}\n", col_def.name, distinct.len()));
    }

    Ok(result)
}

pub fn update_indexes_on_insert(
    db: &mut Database,
    table_name: &str,
    row_idx: usize,
    row: &[Value],
) -> Result<(), RustqlError> {
    for index in db.indexes.values_mut() {
        if index.table == table_name {
            let table = db
                .tables
                .get(table_name)
                .ok_or_else(|| RustqlError::TableNotFound(table_name.to_string()))?;

            let col_idx = table
                .columns
                .iter()
                .position(|col| col.name == index.column)
                .ok_or_else(|| format!("Column '{}' not found", index.column))?;

            let value = row.get(col_idx).cloned().unwrap_or(Value::Null);
            index.entries.entry(value).or_default().push(row_idx);
        }
    }
    Ok(())
}

pub fn update_indexes_on_delete(
    db: &mut Database,
    table_name: &str,
    deleted_row_indices: &[usize],
) -> Result<(), RustqlError> {
    for index in db.indexes.values_mut() {
        if index.table == table_name {
            for entry in index.entries.values_mut() {
                entry.retain(|&idx| !deleted_row_indices.contains(&idx));
                for deleted_idx in deleted_row_indices.iter().rev() {
                    for idx in entry.iter_mut() {
                        if *idx > *deleted_idx {
                            *idx -= 1;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn update_indexes_on_update(
    db: &mut Database,
    table_name: &str,
    row_idx: usize,
    old_row: &[Value],
    new_row: &[Value],
) -> Result<(), RustqlError> {
    for index in db.indexes.values_mut() {
        if index.table == table_name {
            let table = db
                .tables
                .get(table_name)
                .ok_or_else(|| RustqlError::TableNotFound(table_name.to_string()))?;

            let col_idx = table
                .columns
                .iter()
                .position(|col| col.name == index.column)
                .ok_or_else(|| format!("Column '{}' not found", index.column))?;

            let old_value = old_row.get(col_idx).cloned().unwrap_or(Value::Null);
            let new_value = new_row.get(col_idx).cloned().unwrap_or(Value::Null);

            if old_value != new_value {
                if let Some(entry) = index.entries.get_mut(&old_value) {
                    entry.retain(|&idx| idx != row_idx);
                    if entry.is_empty() {
                        index.entries.remove(&old_value);
                    }
                }

                index.entries.entry(new_value).or_default().push(row_idx);
            }
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub enum IndexUsage {
    Equality {
        index_name: String,
        value: Value,
    },
    In {
        index_name: String,
        values: Vec<Value>,
    },
    RangeGreater {
        index_name: String,
        value: Value,
        inclusive: bool,
    },
    RangeLess {
        index_name: String,
        value: Value,
        inclusive: bool,
    },
    RangeBetween {
        index_name: String,
        lower: Value,
        upper: Value,
    },
}

pub fn find_index_usage(db: &Database, table_name: &str, expr: &Expression) -> Option<IndexUsage> {
    match expr {
        Expression::BinaryOp { left, op, right } => match op {
            BinaryOperator::Equal => {
                if let (Expression::Column(col_name), Expression::Value(val)) = (&**left, &**right)
                    && let Some(index) = find_index_for_column(db, table_name, col_name)
                {
                    return Some(IndexUsage::Equality {
                        index_name: index.name.clone(),
                        value: val.clone(),
                    });
                } else if let (Expression::Value(val), Expression::Column(col_name)) =
                    (&**left, &**right)
                    && let Some(index) = find_index_for_column(db, table_name, col_name)
                {
                    return Some(IndexUsage::Equality {
                        index_name: index.name.clone(),
                        value: val.clone(),
                    });
                }
            }
            BinaryOperator::GreaterThan => {
                if let (Expression::Column(col_name), Expression::Value(val)) = (&**left, &**right)
                    && let Some(index) = find_index_for_column(db, table_name, col_name)
                {
                    return Some(IndexUsage::RangeGreater {
                        index_name: index.name.clone(),
                        value: val.clone(),
                        inclusive: false,
                    });
                }
            }
            BinaryOperator::GreaterThanOrEqual => {
                if let (Expression::Column(col_name), Expression::Value(val)) = (&**left, &**right)
                    && let Some(index) = find_index_for_column(db, table_name, col_name)
                {
                    return Some(IndexUsage::RangeGreater {
                        index_name: index.name.clone(),
                        value: val.clone(),
                        inclusive: true,
                    });
                }
            }
            BinaryOperator::LessThan => {
                if let (Expression::Column(col_name), Expression::Value(val)) = (&**left, &**right)
                    && let Some(index) = find_index_for_column(db, table_name, col_name)
                {
                    return Some(IndexUsage::RangeLess {
                        index_name: index.name.clone(),
                        value: val.clone(),
                        inclusive: false,
                    });
                }
            }
            BinaryOperator::LessThanOrEqual => {
                if let (Expression::Column(col_name), Expression::Value(val)) = (&**left, &**right)
                    && let Some(index) = find_index_for_column(db, table_name, col_name)
                {
                    return Some(IndexUsage::RangeLess {
                        index_name: index.name.clone(),
                        value: val.clone(),
                        inclusive: true,
                    });
                }
            }
            BinaryOperator::Between => {
                if let Expression::Column(col_name) = &**left
                    && let Expression::BinaryOp {
                        left: lb,
                        op: lb_op,
                        right: rb,
                    } = &**right
                    && *lb_op == BinaryOperator::And
                    && let (Expression::Value(lower), Expression::Value(upper)) = (&**lb, &**rb)
                    && let Some(index) = find_index_for_column(db, table_name, col_name)
                {
                    return Some(IndexUsage::RangeBetween {
                        index_name: index.name.clone(),
                        lower: lower.clone(),
                        upper: upper.clone(),
                    });
                }
            }
            BinaryOperator::And => {
                if let Some(usage) = find_index_usage(db, table_name, left) {
                    return Some(usage);
                }
                if let Some(usage) = find_index_usage(db, table_name, right) {
                    return Some(usage);
                }
            }
            _ => {}
        },
        Expression::In { left, values } => {
            if let Expression::Column(col_name) = &**left
                && let Some(index) = find_index_for_column(db, table_name, col_name)
            {
                return Some(IndexUsage::In {
                    index_name: index.name.clone(),
                    values: values.clone(),
                });
            }
        }
        Expression::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => {
            return find_index_usage(db, table_name, expr);
        }
        _ => {}
    }
    None
}

fn find_index_for_column<'a>(
    db: &'a Database,
    table_name: &str,
    column_name: &str,
) -> Option<&'a crate::database::Index> {
    let normalized_col = if column_name.contains('.') {
        column_name.split('.').next_back().unwrap_or(column_name)
    } else {
        column_name
    };

    db.indexes
        .values()
        .find(|idx| idx.table == table_name && idx.column == normalized_col)
}

pub fn get_indexed_rows(
    db: &Database,
    table: &Table,
    usage: &IndexUsage,
) -> Result<HashSet<usize>, RustqlError> {
    let index = db
        .indexes
        .get(match usage {
            IndexUsage::Equality { index_name, .. } => index_name,
            IndexUsage::In { index_name, .. } => index_name,
            IndexUsage::RangeGreater { index_name, .. } => index_name,
            IndexUsage::RangeLess { index_name, .. } => index_name,
            IndexUsage::RangeBetween { index_name, .. } => index_name,
        })
        .ok_or_else(|| "Index not found".to_string())?;

    let mut row_indices = HashSet::new();

    match usage {
        IndexUsage::Equality { value, .. } => {
            if let Some(rows) = index.entries.get(value) {
                row_indices.extend(rows.iter().copied());
            }
        }
        IndexUsage::In { values, .. } => {
            for value in values {
                if let Some(rows) = index.entries.get(value) {
                    row_indices.extend(rows.iter().copied());
                }
            }
        }
        IndexUsage::RangeGreater {
            value, inclusive, ..
        } => {
            if *inclusive {
                for (_, rows) in index.entries.range(value..) {
                    row_indices.extend(rows.iter().copied());
                }
            } else {
                use std::ops::Bound;
                for (_, rows) in index
                    .entries
                    .range((Bound::Excluded(value), Bound::Unbounded))
                {
                    row_indices.extend(rows.iter().copied());
                }
            }
        }
        IndexUsage::RangeLess {
            value, inclusive, ..
        } => {
            if *inclusive {
                for (_, rows) in index.entries.range(..=value) {
                    row_indices.extend(rows.iter().copied());
                }
            } else {
                for (_, rows) in index.entries.range(..value) {
                    row_indices.extend(rows.iter().copied());
                }
            }
        }
        IndexUsage::RangeBetween { lower, upper, .. } => {
            for (_, rows) in index.entries.range(lower..=upper) {
                row_indices.extend(rows.iter().copied());
            }
        }
    }

    let valid_indices: HashSet<usize> = row_indices
        .into_iter()
        .filter(|&idx| idx < table.rows.len())
        .collect();

    Ok(valid_indices)
}

pub fn execute_truncate_table(table_name: String) -> Result<String, RustqlError> {
    let mut db = get_database_write();
    let table = db
        .tables
        .get_mut(&table_name)
        .ok_or_else(|| RustqlError::TableNotFound(table_name.clone()))?;
    let old_rows = std::mem::take(&mut table.rows);
    wal::record_wal_entry(WalEntry::TruncateTable {
        name: table_name.clone(),
        old_rows,
    });
    for index in db.indexes.values_mut() {
        if index.table == table_name {
            index.entries.clear();
        }
    }
    save_if_not_in_transaction(&db)?;
    Ok(format!("Table '{}' truncated", table_name))
}

pub fn execute_create_view(name: String, query_sql: String) -> Result<String, RustqlError> {
    let mut db = get_database_write();
    if db.views.contains_key(&name) {
        return Err(RustqlError::Internal(format!(
            "View '{}' already exists",
            name
        )));
    }
    db.views.insert(
        name.clone(),
        crate::database::View {
            name: name.clone(),
            query_sql,
        },
    );
    wal::record_wal_entry(WalEntry::CreateView { name: name.clone() });
    save_if_not_in_transaction(&db)?;
    Ok(format!("View '{}' created", name))
}

pub fn execute_drop_view(name: String, if_exists: bool) -> Result<String, RustqlError> {
    let mut db = get_database_write();
    if let Some(removed) = db.views.remove(&name) {
        wal::record_wal_entry(WalEntry::DropView {
            name: name.clone(),
            view: removed,
        });
        save_if_not_in_transaction(&db)?;
        Ok(format!("View '{}' dropped", name))
    } else if if_exists {
        Ok(format!("View '{}' does not exist (skipped)", name))
    } else {
        Err(RustqlError::Internal(format!(
            "View '{}' does not exist",
            name
        )))
    }
}
