use crate::ast::*;
use crate::database::{CompositeIndex, Database, Index, RowId, Table};
use crate::engine::{CommandTag, QueryResult};
use crate::error::RustqlError;
use crate::wal::WalEntry;
use std::collections::{BTreeMap, HashMap, HashSet};

use super::{
    ExecutionContext, SelectResult, command_result, get_database_read, get_database_write,
    rows_result, save_if_not_in_transaction,
};

pub fn execute_create_table(
    context: &ExecutionContext,
    stmt: CreateTableStatement,
) -> Result<QueryResult, RustqlError> {
    if let Some(source_query) = stmt.as_query {
        return execute_create_table_as_select(context, stmt.name, *source_query);
    }

    let mut db = get_database_write(context);
    if db.tables.contains_key(&stmt.name) {
        if stmt.if_not_exists {
            return Ok(command_result(CommandTag::CreateTable, 0));
        }
        return Err(RustqlError::TableAlreadyExists(stmt.name.clone()));
    }
    db.tables.insert(
        stmt.name.clone(),
        Table::new(stmt.columns, Vec::new(), stmt.constraints),
    );
    super::record_wal_entry(
        context,
        WalEntry::CreateTable {
            name: stmt.name.clone(),
        },
    );
    save_if_not_in_transaction(context, &db)?;
    Ok(command_result(CommandTag::CreateTable, 0))
}

fn execute_create_table_as_select(
    context: &ExecutionContext,
    name: String,
    query: crate::ast::SelectStatement,
) -> Result<QueryResult, RustqlError> {
    let result = {
        let db = get_database_read(context);
        super::select::execute_select_internal(Some(context), query, &db)?
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
                generated: None,
            }
        })
        .collect();

    let mut db = get_database_write(context);
    if db.tables.contains_key(&name) {
        return Err(RustqlError::TableAlreadyExists(name));
    }
    let row_count = result.rows.len() as u64;
    db.tables
        .insert(name.clone(), Table::new(columns, result.rows, Vec::new()));
    super::record_wal_entry(context, WalEntry::CreateTable { name: name.clone() });
    save_if_not_in_transaction(context, &db)?;
    Ok(command_result(CommandTag::CreateTable, row_count))
}

pub fn execute_drop_table(
    context: &ExecutionContext,
    stmt: DropTableStatement,
) -> Result<QueryResult, RustqlError> {
    let mut db = get_database_write(context);
    if let Some(removed) = db.tables.remove(&stmt.name) {
        let (removed_indexes, removed_composite_indexes) =
            remove_indexes_for_table(&mut db, &stmt.name);
        super::record_wal_entry(
            context,
            WalEntry::DropTable {
                name: stmt.name.clone(),
                columns: removed.columns,
                rows: removed.rows,
                row_ids: removed.row_ids,
                next_row_id: removed.next_row_id,
                constraints: removed.constraints,
                indexes: removed_indexes,
                composite_indexes: removed_composite_indexes,
            },
        );
        save_if_not_in_transaction(context, &db)?;
        Ok(command_result(CommandTag::DropTable, 0))
    } else if stmt.if_exists {
        Ok(command_result(CommandTag::DropTable, 0))
    } else {
        Err(RustqlError::TableNotFound(stmt.name.clone()))
    }
}

pub fn execute_alter_table(
    context: &ExecutionContext,
    stmt: AlterTableStatement,
) -> Result<QueryResult, RustqlError> {
    let mut db = get_database_write(context);

    if let AlterOperation::RenameTable(ref new_name) = stmt.operation {
        let table_data = db
            .tables
            .remove(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;
        db.tables.insert(new_name.clone(), table_data);
        for index in db.indexes.values_mut() {
            if index.table == stmt.table {
                index.table = new_name.clone();
            }
        }
        for ci in db.composite_indexes.values_mut() {
            if ci.table == stmt.table {
                ci.table = new_name.clone();
            }
        }
        super::record_wal_entry(
            context,
            WalEntry::AlterRenameTable {
                old_name: stmt.table.clone(),
                new_name: new_name.clone(),
            },
        );
        save_if_not_in_transaction(context, &db)?;
        return Ok(command_result(CommandTag::AlterTable, 0));
    }

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
            super::record_wal_entry(
                context,
                WalEntry::AlterAddColumn {
                    table: stmt.table.clone(),
                    column_name: col_def.name.clone(),
                },
            );
            save_if_not_in_transaction(context, &db)?;
            Ok(command_result(CommandTag::AlterTable, 0))
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
            super::record_wal_entry(
                context,
                WalEntry::AlterDropColumn {
                    table: stmt.table.clone(),
                    col_index,
                    column: removed_col,
                    values: removed_values,
                },
            );
            save_if_not_in_transaction(context, &db)?;
            Ok(command_result(CommandTag::AlterTable, 0))
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
            super::record_wal_entry(
                context,
                WalEntry::AlterRenameColumn {
                    table: stmt.table.clone(),
                    old_name: old.clone(),
                    new_name: new.clone(),
                },
            );
            save_if_not_in_transaction(context, &db)?;
            Ok(command_result(CommandTag::AlterTable, 0))
        }
        AlterOperation::RenameTable(_) => unreachable!(),
        AlterOperation::AddConstraint(constraint) => {
            let constraint_cols = match &constraint {
                crate::ast::TableConstraint::PrimaryKey { columns, .. } => columns.clone(),
                crate::ast::TableConstraint::Unique { columns, .. } => columns.clone(),
            };
            let col_indices: Vec<usize> = constraint_cols
                .iter()
                .map(|col_name| {
                    table
                        .columns
                        .iter()
                        .position(|c| c.name == *col_name)
                        .ok_or_else(|| {
                            RustqlError::ColumnNotFound(format!(
                                "{} (table: {})",
                                col_name, stmt.table
                            ))
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let mut seen: std::collections::BTreeSet<Vec<Value>> =
                std::collections::BTreeSet::new();
            for row in &table.rows {
                let key: Vec<Value> = col_indices.iter().map(|&i| row[i].clone()).collect();
                if matches!(&constraint, crate::ast::TableConstraint::PrimaryKey { .. })
                    && key.iter().any(|v| matches!(v, Value::Null))
                {
                    return Err(RustqlError::Internal(
                        "Cannot add PRIMARY KEY constraint: column contains NULL values"
                            .to_string(),
                    ));
                }
                if !seen.insert(key) {
                    return Err(RustqlError::Internal(
                        "Cannot add constraint: duplicate values exist".to_string(),
                    ));
                }
            }
            table.constraints.push(constraint.clone());
            super::record_wal_entry(
                context,
                WalEntry::AlterAddConstraint {
                    table: stmt.table.clone(),
                    constraint: constraint.clone(),
                },
            );
            save_if_not_in_transaction(context, &db)?;
            Ok(command_result(CommandTag::AlterTable, 0))
        }
        AlterOperation::DropConstraint(constraint_name) => {
            let mut removed_constraint = None;
            let before_len = table.constraints.len();
            table.constraints.retain(|c| {
                let name = match c {
                    crate::ast::TableConstraint::PrimaryKey { name, .. } => name.as_deref(),
                    crate::ast::TableConstraint::Unique { name, .. } => name.as_deref(),
                };
                if name == Some(constraint_name.as_str()) {
                    removed_constraint = Some(c.clone());
                    false
                } else {
                    true
                }
            });
            if table.constraints.len() == before_len {
                return Err(RustqlError::Internal(format!(
                    "Constraint '{}' not found",
                    constraint_name
                )));
            }
            if let Some(removed) = removed_constraint {
                super::record_wal_entry(
                    context,
                    WalEntry::AlterDropConstraint {
                        table: stmt.table.clone(),
                        constraint: removed,
                    },
                );
            }
            save_if_not_in_transaction(context, &db)?;
            Ok(command_result(CommandTag::AlterTable, 0))
        }
    }
}

pub fn execute_create_index(
    context: &ExecutionContext,
    stmt: CreateIndexStatement,
) -> Result<QueryResult, RustqlError> {
    let mut db = get_database_write(context);

    if db.indexes.contains_key(&stmt.name) || db.composite_indexes.contains_key(&stmt.name) {
        if stmt.if_not_exists {
            return Ok(command_result(CommandTag::CreateIndex, 0));
        }
        return Err(RustqlError::IndexError(format!(
            "Index '{}' already exists",
            stmt.name
        )));
    }

    {
        let table = db
            .tables
            .get(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;
        let column_positions = get_column_positions(table, &stmt.columns)?;

        if stmt.columns.len() == 1 {
            let mut index = Index {
                name: stmt.name.clone(),
                table: stmt.table.clone(),
                column: stmt.columns[0].clone(),
                entries: BTreeMap::new(),
                filter_expr: stmt.where_clause.clone(),
            };

            for (row_id, row) in table.iter_rows_with_ids() {
                if !row_matches_index_filter(&db, table, index.filter_expr.as_ref(), row) {
                    continue;
                }
                let value = row
                    .get(*column_positions.first().unwrap())
                    .cloned()
                    .unwrap_or(Value::Null);
                index.entries.entry(value).or_default().push(row_id);
            }

            db.indexes.insert(stmt.name.clone(), index);
        } else {
            let mut index = CompositeIndex {
                name: stmt.name.clone(),
                table: stmt.table.clone(),
                columns: stmt.columns.clone(),
                entries: BTreeMap::new(),
                filter_expr: stmt.where_clause.clone(),
            };

            for (row_id, row) in table.iter_rows_with_ids() {
                if !row_matches_index_filter(&db, table, index.filter_expr.as_ref(), row) {
                    continue;
                }
                let key = composite_key_for_row(row, &column_positions);
                index.entries.entry(key).or_default().push(row_id);
            }

            db.composite_indexes.insert(stmt.name.clone(), index);
        }
    }

    super::record_wal_entry(
        context,
        WalEntry::CreateIndex {
            name: stmt.name.clone(),
        },
    );
    save_if_not_in_transaction(context, &db)?;
    Ok(command_result(CommandTag::CreateIndex, 0))
}

pub fn execute_drop_index(
    context: &ExecutionContext,
    stmt: DropIndexStatement,
) -> Result<QueryResult, RustqlError> {
    let mut db = get_database_write(context);
    let removed_index = db.indexes.remove(&stmt.name);
    let removed_composite_index = db.composite_indexes.remove(&stmt.name);

    if removed_index.is_some() || removed_composite_index.is_some() {
        super::record_wal_entry(
            context,
            WalEntry::DropIndex {
                name: stmt.name.clone(),
                index: removed_index,
                composite_index: Box::new(removed_composite_index),
            },
        );
        save_if_not_in_transaction(context, &db)?;
        Ok(command_result(CommandTag::DropIndex, 0))
    } else if stmt.if_exists {
        Ok(command_result(CommandTag::DropIndex, 0))
    } else {
        Err(RustqlError::IndexError(format!(
            "Index '{}' does not exist",
            stmt.name
        )))
    }
}

fn remove_indexes_for_table(
    db: &mut Database,
    table_name: &str,
) -> (Vec<Index>, Vec<CompositeIndex>) {
    let index_names: Vec<String> = db
        .indexes
        .iter()
        .filter(|(_, index)| index.table == table_name)
        .map(|(name, _)| name.clone())
        .collect();
    let composite_index_names: Vec<String> = db
        .composite_indexes
        .iter()
        .filter(|(_, index)| index.table == table_name)
        .map(|(name, _)| name.clone())
        .collect();

    let removed_indexes = index_names
        .into_iter()
        .filter_map(|name| db.indexes.remove(&name))
        .collect();
    let removed_composite_indexes = composite_index_names
        .into_iter()
        .filter_map(|name| db.composite_indexes.remove(&name))
        .collect();

    (removed_indexes, removed_composite_indexes)
}

pub fn execute_describe(
    context: &ExecutionContext,
    table_name: String,
) -> Result<QueryResult, RustqlError> {
    let db = get_database_read(context);
    let table = db
        .tables
        .get(&table_name)
        .ok_or_else(|| RustqlError::TableNotFound(table_name.to_string()))?;

    let headers = vec![
        "Column".to_string(),
        "Type".to_string(),
        "Nullable".to_string(),
        "Primary Key".to_string(),
        "Unique".to_string(),
        "Default".to_string(),
    ];
    let mut rows = Vec::new();

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

        rows.push(vec![
            Value::Text(col.name.clone()),
            Value::Text(type_str.to_string()),
            Value::Text(nullable_str.to_string()),
            Value::Text(pk_str.to_string()),
            Value::Text(unique_str.to_string()),
            Value::Text(default_str),
        ]);
    }

    Ok(rows_result(SelectResult { headers, rows }))
}

pub fn execute_show_tables(context: &ExecutionContext) -> Result<QueryResult, RustqlError> {
    let db = get_database_read(context);
    let mut table_names: Vec<&String> = db.tables.keys().collect();
    table_names.sort();
    let rows = table_names
        .into_iter()
        .map(|table_name| vec![Value::Text(table_name.clone())])
        .collect();
    Ok(rows_result(SelectResult {
        headers: vec!["table".to_string()],
        rows,
    }))
}

pub fn execute_analyze(
    context: &ExecutionContext,
    table_name: String,
) -> Result<QueryResult, RustqlError> {
    let db = get_database_read(context);
    let table = db
        .tables
        .get(&table_name)
        .ok_or_else(|| RustqlError::TableNotFound(table_name.clone()))?;

    Ok(command_result(CommandTag::Analyze, table.rows.len() as u64))
}

pub fn update_indexes_on_insert(
    db: &mut Database,
    table_name: &str,
    row_id: RowId,
    row: &[Value],
) -> Result<(), RustqlError> {
    let db_snapshot = db.clone();
    let table = db_snapshot
        .tables
        .get(table_name)
        .ok_or_else(|| RustqlError::TableNotFound(table_name.to_string()))?;

    for index in db.indexes.values_mut() {
        if index.table == table_name {
            if !row_matches_index_filter(&db_snapshot, table, index.filter_expr.as_ref(), row) {
                continue;
            }

            let col_idx = table
                .columns
                .iter()
                .position(|col| col.name == index.column)
                .ok_or_else(|| format!("Column '{}' not found", index.column))?;

            let value = row.get(col_idx).cloned().unwrap_or(Value::Null);
            index.entries.entry(value).or_default().push(row_id);
        }
    }

    for index in db.composite_indexes.values_mut() {
        if index.table == table_name {
            if !row_matches_index_filter(&db_snapshot, table, index.filter_expr.as_ref(), row) {
                continue;
            }

            let column_positions = get_column_positions(table, &index.columns)?;
            let key = composite_key_for_row(row, &column_positions);
            index.entries.entry(key).or_default().push(row_id);
        }
    }

    Ok(())
}

pub fn update_indexes_on_delete(
    db: &mut Database,
    table_name: &str,
    deleted_row_ids: &[RowId],
) -> Result<(), RustqlError> {
    for index in db.indexes.values_mut() {
        if index.table == table_name {
            for entry in index.entries.values_mut() {
                entry.retain(|row_id| !deleted_row_ids.contains(row_id));
            }
        }
    }

    for index in db.composite_indexes.values_mut() {
        if index.table == table_name {
            for entry in index.entries.values_mut() {
                entry.retain(|row_id| !deleted_row_ids.contains(row_id));
            }
        }
    }

    Ok(())
}

pub fn update_indexes_on_update(
    db: &mut Database,
    table_name: &str,
    row_id: RowId,
    old_row: &[Value],
    new_row: &[Value],
) -> Result<(), RustqlError> {
    let db_snapshot = db.clone();
    let table = db_snapshot
        .tables
        .get(table_name)
        .ok_or_else(|| RustqlError::TableNotFound(table_name.to_string()))?;

    for index in db.indexes.values_mut() {
        if index.table == table_name {
            let col_idx = table
                .columns
                .iter()
                .position(|col| col.name == index.column)
                .ok_or_else(|| format!("Column '{}' not found", index.column))?;

            let old_value = old_row.get(col_idx).cloned().unwrap_or(Value::Null);
            let new_value = new_row.get(col_idx).cloned().unwrap_or(Value::Null);
            let old_matches =
                row_matches_index_filter(&db_snapshot, table, index.filter_expr.as_ref(), old_row);
            let new_matches =
                row_matches_index_filter(&db_snapshot, table, index.filter_expr.as_ref(), new_row);

            match (old_matches, new_matches) {
                (true, true) if old_value != new_value => {
                    remove_row_from_entries(&mut index.entries, &old_value, row_id);
                    index.entries.entry(new_value).or_default().push(row_id);
                }
                (true, false) => {
                    remove_row_from_entries(&mut index.entries, &old_value, row_id);
                }
                (false, true) => {
                    index.entries.entry(new_value).or_default().push(row_id);
                }
                _ => {}
            }
        }
    }

    for index in db.composite_indexes.values_mut() {
        if index.table == table_name {
            let column_positions = get_column_positions(table, &index.columns)?;
            let old_key = composite_key_for_row(old_row, &column_positions);
            let new_key = composite_key_for_row(new_row, &column_positions);
            let old_matches =
                row_matches_index_filter(&db_snapshot, table, index.filter_expr.as_ref(), old_row);
            let new_matches =
                row_matches_index_filter(&db_snapshot, table, index.filter_expr.as_ref(), new_row);

            match (old_matches, new_matches) {
                (true, true) if old_key != new_key => {
                    remove_row_from_entries(&mut index.entries, &old_key, row_id);
                    index.entries.entry(new_key).or_default().push(row_id);
                }
                (true, false) => {
                    remove_row_from_entries(&mut index.entries, &old_key, row_id);
                }
                (false, true) => {
                    index.entries.entry(new_key).or_default().push(row_id);
                }
                _ => {}
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
    CompositePrefix {
        index_name: String,
        values: Vec<Value>,
    },
}

pub fn find_index_usage(db: &Database, table_name: &str, expr: &Expression) -> Option<IndexUsage> {
    let composite_usage = find_best_composite_index_usage(db, table_name, expr);
    let single_usage = find_single_index_usage(db, table_name, expr, expr);

    match (composite_usage, single_usage) {
        (Some((prefix_len, composite_usage)), Some(_single_usage)) if prefix_len > 1 => {
            Some(composite_usage)
        }
        (Some((_prefix_len, _)), Some(single_usage)) => Some(single_usage),
        (Some((_prefix_len, composite_usage)), None) => Some(composite_usage),
        (None, Some(single_usage)) => Some(single_usage),
        (None, None) => None,
    }
}

impl IndexUsage {
    pub fn index_name(&self) -> &str {
        match self {
            IndexUsage::Equality { index_name, .. }
            | IndexUsage::In { index_name, .. }
            | IndexUsage::RangeGreater { index_name, .. }
            | IndexUsage::RangeLess { index_name, .. }
            | IndexUsage::RangeBetween { index_name, .. }
            | IndexUsage::CompositePrefix { index_name, .. } => index_name,
        }
    }
}

fn find_best_composite_index_usage(
    db: &Database,
    table_name: &str,
    expr: &Expression,
) -> Option<(usize, IndexUsage)> {
    let equality_predicates = extract_equality_predicates(expr);
    let mut best_match: Option<(usize, IndexUsage)> = None;

    for (index_name, index) in &db.composite_indexes {
        if index.table != table_name || !query_implies_filter(expr, index.filter_expr.as_ref()) {
            continue;
        }

        let mut prefix_values = Vec::new();
        for column in &index.columns {
            if let Some(value) = equality_predicates.get(column) {
                prefix_values.push(value.clone());
            } else {
                break;
            }
        }

        let prefix_len = prefix_values.len();
        if prefix_len == 0 {
            continue;
        }

        let usage = IndexUsage::CompositePrefix {
            index_name: index_name.clone(),
            values: prefix_values,
        };

        if best_match
            .as_ref()
            .is_none_or(|(best_len, _)| prefix_len > *best_len)
        {
            best_match = Some((prefix_len, usage));
        }
    }

    best_match
}

fn extract_equality_predicates(expr: &Expression) -> HashMap<String, Value> {
    let mut predicates = HashMap::new();
    let mut conjuncts = Vec::new();
    collect_conjuncts(expr, &mut conjuncts);

    for conjunct in conjuncts {
        if let Some((column, value)) = extract_column_equality(conjunct) {
            predicates.insert(column, value);
        }
    }

    predicates
}

fn collect_conjuncts<'a>(expr: &'a Expression, conjuncts: &mut Vec<&'a Expression>) {
    if let Expression::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = expr
    {
        collect_conjuncts(left, conjuncts);
        collect_conjuncts(right, conjuncts);
    } else {
        conjuncts.push(expr);
    }
}

fn extract_column_equality(expr: &Expression) -> Option<(String, Value)> {
    if let Expression::BinaryOp { left, op, right } = expr
        && *op == BinaryOperator::Equal
    {
        if let (Expression::Column(column), Expression::Value(value)) = (&**left, &**right) {
            return Some((normalize_column_name(column).to_string(), value.clone()));
        }

        if let (Expression::Value(value), Expression::Column(column)) = (&**left, &**right) {
            return Some((normalize_column_name(column).to_string(), value.clone()));
        }
    }

    None
}

fn find_single_index_usage(
    db: &Database,
    table_name: &str,
    expr: &Expression,
    query_expr: &Expression,
) -> Option<IndexUsage> {
    match expr {
        Expression::BinaryOp { left, op, right } => match op {
            BinaryOperator::Equal => {
                if let (Expression::Column(col_name), Expression::Value(val)) = (&**left, &**right)
                    && let Some(index) = find_index_for_column(db, table_name, col_name, query_expr)
                {
                    return Some(IndexUsage::Equality {
                        index_name: index.name.clone(),
                        value: val.clone(),
                    });
                } else if let (Expression::Value(val), Expression::Column(col_name)) =
                    (&**left, &**right)
                    && let Some(index) = find_index_for_column(db, table_name, col_name, query_expr)
                {
                    return Some(IndexUsage::Equality {
                        index_name: index.name.clone(),
                        value: val.clone(),
                    });
                }
            }
            BinaryOperator::GreaterThan => {
                if let (Expression::Column(col_name), Expression::Value(val)) = (&**left, &**right)
                    && let Some(index) = find_index_for_column(db, table_name, col_name, query_expr)
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
                    && let Some(index) = find_index_for_column(db, table_name, col_name, query_expr)
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
                    && let Some(index) = find_index_for_column(db, table_name, col_name, query_expr)
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
                    && let Some(index) = find_index_for_column(db, table_name, col_name, query_expr)
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
                    && let Some(index) = find_index_for_column(db, table_name, col_name, query_expr)
                {
                    return Some(IndexUsage::RangeBetween {
                        index_name: index.name.clone(),
                        lower: lower.clone(),
                        upper: upper.clone(),
                    });
                }
            }
            BinaryOperator::And => {
                if let Some(usage) = find_single_index_usage(db, table_name, left, query_expr) {
                    return Some(usage);
                }
                if let Some(usage) = find_single_index_usage(db, table_name, right, query_expr) {
                    return Some(usage);
                }
            }
            _ => {}
        },
        Expression::In { left, values } => {
            if let Expression::Column(col_name) = &**left
                && let Some(index) = find_index_for_column(db, table_name, col_name, query_expr)
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
            return find_single_index_usage(db, table_name, expr, query_expr);
        }
        _ => {}
    }
    None
}

fn find_index_for_column<'a>(
    db: &'a Database,
    table_name: &str,
    column_name: &str,
    query_expr: &Expression,
) -> Option<&'a Index> {
    let normalized_col = normalize_column_name(column_name);

    db.indexes.values().find(|idx| {
        idx.table == table_name
            && idx.column == normalized_col
            && query_implies_filter(query_expr, idx.filter_expr.as_ref())
    })
}

fn normalize_column_name(column_name: &str) -> &str {
    if column_name.contains('.') {
        column_name.split('.').next_back().unwrap_or(column_name)
    } else {
        column_name
    }
}

fn get_column_positions(table: &Table, columns: &[String]) -> Result<Vec<usize>, RustqlError> {
    columns
        .iter()
        .map(|column| {
            table
                .columns
                .iter()
                .position(|col| col.name == *column)
                .ok_or_else(|| {
                    RustqlError::Internal(format!("Column '{}' does not exist in table", column))
                })
        })
        .collect()
}

pub(crate) fn row_matches_index_filter(
    db: &Database,
    table: &Table,
    filter_expr: Option<&Expression>,
    row: &[Value],
) -> bool {
    filter_expr.is_none_or(|expr| {
        super::expr::evaluate_expression(Some(db), expr, &table.columns, row).unwrap_or(false)
    })
}

fn remove_row_from_entries<K: Ord + Clone>(
    entries: &mut BTreeMap<K, Vec<RowId>>,
    key: &K,
    row_id: RowId,
) {
    if let Some(entry) = entries.get_mut(key) {
        entry.retain(|candidate| *candidate != row_id);
        if entry.is_empty() {
            entries.remove(key);
        }
    }
}

fn query_implies_filter(query_expr: &Expression, filter_expr: Option<&Expression>) -> bool {
    let Some(filter_expr) = filter_expr else {
        return true;
    };

    let query_equalities = extract_equality_predicates(query_expr);
    let mut filter_conjuncts = Vec::new();
    collect_conjuncts(filter_expr, &mut filter_conjuncts);

    filter_conjuncts.into_iter().all(|conjunct| {
        extract_column_equality(conjunct)
            .and_then(|(column, value)| query_equalities.get(&column).map(|query| query == &value))
            .unwrap_or(false)
    })
}

fn composite_key_for_row(row: &[Value], column_positions: &[usize]) -> Vec<Value> {
    column_positions
        .iter()
        .map(|&col_idx| row.get(col_idx).cloned().unwrap_or(Value::Null))
        .collect()
}

pub fn get_indexed_rows(
    db: &Database,
    table: &Table,
    usage: &IndexUsage,
) -> Result<HashSet<RowId>, RustqlError> {
    let mut row_ids = HashSet::new();

    match usage {
        IndexUsage::Equality { value, .. } => {
            let index = db
                .indexes
                .get(usage.index_name())
                .ok_or_else(|| "Index not found".to_string())?;
            if let Some(rows) = index.entries.get(value) {
                row_ids.extend(rows.iter().copied());
            }
        }
        IndexUsage::In { values, .. } => {
            let index = db
                .indexes
                .get(usage.index_name())
                .ok_or_else(|| "Index not found".to_string())?;
            for value in values {
                if let Some(rows) = index.entries.get(value) {
                    row_ids.extend(rows.iter().copied());
                }
            }
        }
        IndexUsage::RangeGreater {
            value, inclusive, ..
        } => {
            let index = db
                .indexes
                .get(usage.index_name())
                .ok_or_else(|| "Index not found".to_string())?;
            if *inclusive {
                for (_, rows) in index.entries.range(value..) {
                    row_ids.extend(rows.iter().copied());
                }
            } else {
                use std::ops::Bound;
                for (_, rows) in index
                    .entries
                    .range((Bound::Excluded(value), Bound::Unbounded))
                {
                    row_ids.extend(rows.iter().copied());
                }
            }
        }
        IndexUsage::RangeLess {
            value, inclusive, ..
        } => {
            let index = db
                .indexes
                .get(usage.index_name())
                .ok_or_else(|| "Index not found".to_string())?;
            if *inclusive {
                for (_, rows) in index.entries.range(..=value) {
                    row_ids.extend(rows.iter().copied());
                }
            } else {
                for (_, rows) in index.entries.range(..value) {
                    row_ids.extend(rows.iter().copied());
                }
            }
        }
        IndexUsage::RangeBetween { lower, upper, .. } => {
            let index = db
                .indexes
                .get(usage.index_name())
                .ok_or_else(|| "Index not found".to_string())?;
            for (_, rows) in index.entries.range(lower..=upper) {
                row_ids.extend(rows.iter().copied());
            }
        }
        IndexUsage::CompositePrefix { values, .. } => {
            let index = db
                .composite_indexes
                .get(usage.index_name())
                .ok_or_else(|| "Index not found".to_string())?;

            if values.len() == index.columns.len() {
                if let Some(rows) = index.entries.get(values) {
                    row_ids.extend(rows.iter().copied());
                }
            } else {
                for (key, rows) in &index.entries {
                    if key.starts_with(values) {
                        row_ids.extend(rows.iter().copied());
                    }
                }
            }
        }
    }

    Ok(row_ids
        .into_iter()
        .filter(|row_id| table.position_of_row_id(*row_id).is_some())
        .collect())
}

pub fn execute_truncate_table(
    context: &ExecutionContext,
    table_name: String,
) -> Result<QueryResult, RustqlError> {
    let mut db = get_database_write(context);
    let table = db
        .tables
        .get_mut(&table_name)
        .ok_or_else(|| RustqlError::TableNotFound(table_name.clone()))?;
    let old_rows = std::mem::take(&mut table.rows);
    let old_row_ids = std::mem::take(&mut table.row_ids);
    let old_next_row_id = table.next_row_id;
    table.next_row_id = 1;
    super::record_wal_entry(
        context,
        WalEntry::TruncateTable {
            name: table_name.clone(),
            old_rows,
            old_row_ids,
            old_next_row_id,
        },
    );
    for index in db.indexes.values_mut() {
        if index.table == table_name {
            index.entries.clear();
        }
    }
    for index in db.composite_indexes.values_mut() {
        if index.table == table_name {
            index.entries.clear();
        }
    }
    save_if_not_in_transaction(context, &db)?;
    Ok(command_result(CommandTag::TruncateTable, 0))
}

pub fn execute_create_view(
    context: &ExecutionContext,
    name: String,
    query_sql: String,
) -> Result<QueryResult, RustqlError> {
    let mut db = get_database_write(context);
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
    super::record_wal_entry(context, WalEntry::CreateView { name: name.clone() });
    save_if_not_in_transaction(context, &db)?;
    Ok(command_result(CommandTag::CreateView, 0))
}

pub fn execute_drop_view(
    context: &ExecutionContext,
    name: String,
    if_exists: bool,
) -> Result<QueryResult, RustqlError> {
    let mut db = get_database_write(context);
    if let Some(removed) = db.views.remove(&name) {
        super::record_wal_entry(
            context,
            WalEntry::DropView {
                name: name.clone(),
                view: removed,
            },
        );
        save_if_not_in_transaction(context, &db)?;
        Ok(command_result(CommandTag::DropView, 0))
    } else if if_exists {
        Ok(command_result(CommandTag::DropView, 0))
    } else {
        Err(RustqlError::Internal(format!(
            "View '{}' does not exist",
            name
        )))
    }
}
