use crate::ast::*;
use crate::database::Database;
use crate::error::{ConstraintKind, RustqlError};
use crate::wal::{self, WalEntry};
use std::collections::HashSet;

use super::expr::{evaluate_expression, evaluate_value_expression, parse_value_from_string};
use super::{get_database_write, save_if_not_in_transaction};

pub fn execute_insert(mut stmt: InsertStatement) -> Result<String, RustqlError> {
    if let Some(source_query) = stmt.source_query.take() {
        let select_result = super::select::execute_select(*source_query.clone())?;
        let lines: Vec<&str> = select_result.lines().collect();
        if lines.len() >= 2 {
            for line in lines.iter().skip(2) {
                if line.trim().is_empty() {
                    continue;
                }
                let values: Vec<Value> = line
                    .split('\t')
                    .filter(|s| !s.is_empty())
                    .map(parse_value_from_string)
                    .collect();
                stmt.values.push(values);
            }
        }
    }

    let mut db = get_database_write();

    let table_ref = db
        .tables
        .get(&stmt.table)
        .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;

    let mapped_values: Vec<Vec<Value>> = if let Some(ref specified_columns) = stmt.columns {
        for col_name in specified_columns {
            if !table_ref.columns.iter().any(|c| c.name == *col_name) {
                return Err(RustqlError::ColumnNotFound(format!(
                    "{} (table: {})",
                    col_name, stmt.table
                )));
            }
        }

        stmt.values
            .iter()
            .map(|values| {
                if values.len() != specified_columns.len() {
                    return Err(RustqlError::Internal(format!(
                        "Column count mismatch: expected {} values for {} columns, got {}",
                        specified_columns.len(),
                        specified_columns.len(),
                        values.len()
                    )));
                }

                let mut full_row: Vec<Value> = table_ref
                    .columns
                    .iter()
                    .map(|col| col.default_value.clone().unwrap_or(Value::Null))
                    .collect();

                for (idx, col_name) in specified_columns.iter().enumerate() {
                    let col_pos = table_ref
                        .columns
                        .iter()
                        .position(|c| c.name == *col_name)
                        .unwrap();
                    full_row[col_pos] = values[idx].clone();
                }

                Ok(full_row)
            })
            .collect::<Result<Vec<Vec<Value>>, RustqlError>>()?
    } else {
        stmt.values
            .iter()
            .map(|values| {
                let mut full_row: Vec<Value> = table_ref
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(idx, col)| {
                        if idx < values.len() {
                            values[idx].clone()
                        } else {
                            col.default_value.clone().unwrap_or(Value::Null)
                        }
                    })
                    .collect();

                for (idx, val) in values.iter().enumerate() {
                    if idx < full_row.len() {
                        full_row[idx] = val.clone();
                    }
                }

                if values.len() != table_ref.columns.len() {
                    return Err(RustqlError::Internal(format!(
                        "Column count mismatch: expected {}, got {}",
                        table_ref.columns.len(),
                        values.len()
                    )));
                }

                Ok(full_row)
            })
            .collect::<Result<Vec<Vec<Value>>, RustqlError>>()?
    };

    let mut mapped_values = mapped_values;
    for values in &mut mapped_values {
        for (col_idx, col_def) in table_ref.columns.iter().enumerate() {
            if col_def.auto_increment
                && col_idx < values.len()
                && matches!(values[col_idx], Value::Null)
            {
                let max_val = table_ref
                    .rows
                    .iter()
                    .filter_map(|row| match row.get(col_idx) {
                        Some(Value::Integer(i)) => Some(*i),
                        _ => None,
                    })
                    .max()
                    .unwrap_or(0);
                values[col_idx] = Value::Integer(max_val + 1);
            }
        }
    }

    let columns_snapshot = table_ref.columns.clone();

    let mut inserted_count = 0usize;
    let mut updated_count = 0usize;

    for values in &mapped_values {
        validate_not_null_constraints(&columns_snapshot, values)?;
        validate_foreign_keys_for_insert(&db, &columns_snapshot, values)?;
        validate_check_constraints(&columns_snapshot, values)?;

        let pk_result =
            validate_primary_keys_for_insert(&db, &columns_snapshot, values, &stmt.table);
        let unique_result = validate_unique_constraints_for_insert(
            &db,
            &columns_snapshot,
            values,
            &stmt.table,
            None,
        );

        let conflict = pk_result.is_err() || unique_result.is_err();

        if conflict {
            if let Some(ref on_conflict) = stmt.on_conflict {
                match &on_conflict.action {
                    OnConflictAction::DoNothing => {
                        continue;
                    }
                    OnConflictAction::DoUpdate { assignments } => {
                        let conflict_row_idx = find_conflict_row(
                            &db,
                            &stmt.table,
                            &columns_snapshot,
                            &on_conflict.columns,
                            values,
                        );
                        if let Some(row_idx) = conflict_row_idx {
                            let table = db
                                .tables
                                .get(&stmt.table)
                                .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;
                            let existing_row = table.rows[row_idx].clone();
                            let mut updated_row = existing_row.clone();
                            for assignment in assignments {
                                if let Some(idx) = columns_snapshot
                                    .iter()
                                    .position(|c| c.name == assignment.column)
                                {
                                    updated_row[idx] = evaluate_value_expression(
                                        &assignment.value,
                                        &columns_snapshot,
                                        values,
                                    )?;
                                }
                            }
                            let table = db
                                .tables
                                .get_mut(&stmt.table)
                                .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;
                            let old_row = table.rows[row_idx].clone();
                            table.rows[row_idx] = updated_row.clone();
                            wal::record_wal_entry(WalEntry::UpdateRow {
                                table: stmt.table.clone(),
                                row_index: row_idx,
                                old_row: old_row.clone(),
                            });
                            super::ddl::update_indexes_on_update(
                                &mut db,
                                &stmt.table,
                                row_idx,
                                &old_row,
                                &updated_row,
                            )?;
                            updated_count += 1;
                        }
                        continue;
                    }
                }
            } else {
                pk_result?;
                unique_result?;
            }
        }

        let table = db
            .tables
            .get_mut(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;
        let row_idx = table.rows.len();
        table.rows.push(values.clone());
        wal::record_wal_entry(WalEntry::InsertRow {
            table: stmt.table.clone(),
            row_index: row_idx,
        });
        super::ddl::update_indexes_on_insert(&mut db, &stmt.table, row_idx, values)?;
        inserted_count += 1;
    }

    save_if_not_in_transaction(&db)?;
    if updated_count > 0 {
        Ok(format!(
            "{} row(s) inserted, {} row(s) updated",
            inserted_count, updated_count
        ))
    } else {
        Ok(format!("{} row(s) inserted", inserted_count))
    }
}

pub fn execute_update(stmt: UpdateStatement) -> Result<String, RustqlError> {
    let mut db = get_database_write();

    let candidate_indices: Option<HashSet<usize>> = {
        let table_ref_immut = db
            .tables
            .get(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;

        if let Some(ref where_expr) = stmt.where_clause {
            if let Some(index_usage) = super::ddl::find_index_usage(&db, &stmt.table, where_expr) {
                super::ddl::get_indexed_rows(&db, table_ref_immut, &index_usage).ok()
            } else {
                None
            }
        } else {
            None
        }
    };

    let table_ref = db
        .tables
        .get(&stmt.table)
        .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;

    let mut rows_to_update: Vec<(usize, Vec<Value>)> = Vec::new();

    let rows_to_check: Vec<(usize, &Vec<Value>)> =
        if let Some(ref candidate_set) = candidate_indices {
            table_ref
                .rows
                .iter()
                .enumerate()
                .filter(|(idx, _)| candidate_set.contains(idx))
                .collect()
        } else {
            table_ref.rows.iter().enumerate().collect()
        };

    for (row_idx, row) in rows_to_check {
        let should_update = if let Some(ref where_expr) = stmt.where_clause {
            evaluate_expression(None, where_expr, &table_ref.columns, row)?
        } else {
            true
        };
        if should_update {
            let mut updated_row = row.clone();
            for assignment in &stmt.assignments {
                if let Some(idx) = table_ref
                    .columns
                    .iter()
                    .position(|c| c.name == assignment.column)
                {
                    updated_row[idx] =
                        evaluate_value_expression(&assignment.value, &table_ref.columns, row)?;
                } else {
                    return Err(RustqlError::ColumnNotFound(assignment.column.clone()));
                }
            }
            validate_not_null_constraints(&table_ref.columns, &updated_row)?;
            validate_unique_constraints_for_insert(
                &db,
                &table_ref.columns,
                &updated_row,
                &stmt.table,
                Some(row_idx),
            )?;
            validate_foreign_keys_for_update(&db, &table_ref.columns, &updated_row)?;
            validate_check_constraints(&table_ref.columns, &updated_row)?;
            rows_to_update.push((row_idx, updated_row));
        }
    }

    let updated_count = rows_to_update.len();
    let columns = table_ref.columns.clone();
    let mut update_info = Vec::new();
    {
        let table = db
            .tables
            .get_mut(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;
        for (row_idx, updated_row) in rows_to_update {
            let old_row = table.rows[row_idx].clone();
            table.rows[row_idx] = updated_row.clone();
            wal::record_wal_entry(WalEntry::UpdateRow {
                table: stmt.table.clone(),
                row_index: row_idx,
                old_row: old_row.clone(),
            });
            update_info.push((row_idx, old_row, updated_row));
        }
    }
    for (_, old_row, updated_row) in &update_info {
        handle_foreign_keys_for_update(&mut db, &stmt.table, &columns, old_row, updated_row)?;
    }
    for (row_idx, old_row, updated_row) in update_info {
        super::ddl::update_indexes_on_update(
            &mut db,
            &stmt.table,
            row_idx,
            &old_row,
            &updated_row,
        )?;
    }
    save_if_not_in_transaction(&db)?;
    Ok(format!("{} row(s) updated", updated_count))
}

pub fn execute_delete(stmt: DeleteStatement) -> Result<String, RustqlError> {
    let mut db = get_database_write();

    let (columns, rows_to_delete) = {
        let table_ref = db
            .tables
            .get(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;

        let mut rows: Vec<Vec<Value>> = Vec::new();

        let candidate_indices: Option<HashSet<usize>> = if let Some(ref where_expr) =
            stmt.where_clause
        {
            if let Some(index_usage) = super::ddl::find_index_usage(&db, &stmt.table, where_expr) {
                super::ddl::get_indexed_rows(&db, table_ref, &index_usage).ok()
            } else {
                None
            }
        } else {
            None
        };

        if let Some(ref where_expr) = stmt.where_clause {
            let rows_to_check: Vec<(usize, &Vec<Value>)> =
                if let Some(ref candidate_set) = candidate_indices {
                    table_ref
                        .rows
                        .iter()
                        .enumerate()
                        .filter(|(idx, _)| candidate_set.contains(idx))
                        .collect()
                } else {
                    table_ref.rows.iter().enumerate().collect()
                };

            for (_, row) in rows_to_check {
                if evaluate_expression(None, where_expr, &table_ref.columns, row).unwrap_or(false) {
                    rows.push(row.clone());
                }
            }
        } else {
            rows = table_ref.rows.clone();
        }

        (table_ref.columns.clone(), rows)
    };

    for row_to_delete in &rows_to_delete {
        handle_foreign_keys_for_delete(&mut db, &stmt.table, &columns, row_to_delete)?;
    }

    let table_ref = db
        .tables
        .get(&stmt.table)
        .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;

    let candidate_indices: Option<HashSet<usize>> = if let Some(ref where_expr) = stmt.where_clause
    {
        if let Some(index_usage) = super::ddl::find_index_usage(&db, &stmt.table, where_expr) {
            super::ddl::get_indexed_rows(&db, table_ref, &index_usage).ok()
        } else {
            None
        }
    } else {
        None
    };

    let (_, mut rows_to_delete_indices) = {
        let table = db
            .tables
            .get_mut(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;
        let initial_count = table.rows.len();

        let mut rows_to_delete_indices = Vec::new();
        if let Some(ref where_expr) = stmt.where_clause {
            let rows_to_check: Vec<(usize, &Vec<Value>)> =
                if let Some(ref candidate_set) = candidate_indices {
                    table
                        .rows
                        .iter()
                        .enumerate()
                        .filter(|(idx, _)| candidate_set.contains(idx))
                        .collect()
                } else {
                    table.rows.iter().enumerate().collect()
                };

            for (idx, row) in rows_to_check {
                if evaluate_expression(None, where_expr, &table.columns, row).unwrap_or(false) {
                    rows_to_delete_indices.push(idx);
                }
            }
        } else {
            rows_to_delete_indices = (0..table.rows.len()).collect();
        }

        rows_to_delete_indices.sort();
        rows_to_delete_indices.reverse();
        for idx in &rows_to_delete_indices {
            let old_row = table.rows[*idx].clone();
            wal::record_wal_entry(WalEntry::DeleteRow {
                table: stmt.table.clone(),
                row_index: *idx,
                old_row,
            });
            table.rows.remove(*idx);
        }

        (initial_count, rows_to_delete_indices)
    };

    let deleted_count = rows_to_delete_indices.len();
    rows_to_delete_indices.reverse();
    super::ddl::update_indexes_on_delete(&mut db, &stmt.table, &rows_to_delete_indices)?;

    save_if_not_in_transaction(&db)?;
    Ok(format!("{} row(s) deleted", deleted_count))
}

pub fn validate_not_null_constraints(
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<(), RustqlError> {
    for (col_idx, col_def) in columns.iter().enumerate() {
        if !col_def.nullable && col_idx < row.len() && matches!(row[col_idx], Value::Null) {
            return Err(RustqlError::ConstraintViolation {
                kind: ConstraintKind::NotNull,
                message: format!(
                    "NOT NULL constraint violation: Column '{}' cannot be NULL",
                    col_def.name
                ),
            });
        }
    }
    Ok(())
}

pub fn validate_primary_keys_for_insert(
    db: &Database,
    columns: &[ColumnDefinition],
    row: &[Value],
    table_name: &str,
) -> Result<(), RustqlError> {
    for (col_idx, col_def) in columns.iter().enumerate() {
        if col_def.primary_key {
            let pk_value = &row[col_idx];

            if matches!(pk_value, Value::Null) {
                return Err(RustqlError::ConstraintViolation {
                    kind: ConstraintKind::PrimaryKey,
                    message: format!(
                        "Primary key constraint violation: Column '{}' cannot be NULL",
                        col_def.name
                    ),
                });
            }

            let table = db
                .tables
                .get(table_name)
                .ok_or_else(|| RustqlError::TableNotFound(table_name.to_string()))?;

            for existing_row in &table.rows {
                if existing_row[col_idx] == *pk_value {
                    return Err(RustqlError::ConstraintViolation {
                        kind: ConstraintKind::PrimaryKey,
                        message: format!(
                            "Primary key constraint violation: Duplicate value for column '{}'",
                            col_def.name
                        ),
                    });
                }
            }
        }
    }
    Ok(())
}

pub fn validate_unique_constraints_for_insert(
    db: &Database,
    columns: &[ColumnDefinition],
    row: &[Value],
    table_name: &str,
    exclude_row_idx: Option<usize>,
) -> Result<(), RustqlError> {
    for (col_idx, col_def) in columns.iter().enumerate() {
        if col_def.unique {
            let unique_value = &row[col_idx];

            if matches!(unique_value, Value::Null) {
                continue;
            }

            let table = db
                .tables
                .get(table_name)
                .ok_or_else(|| RustqlError::TableNotFound(table_name.to_string()))?;

            for (row_idx, existing_row) in table.rows.iter().enumerate() {
                if let Some(exclude_idx) = exclude_row_idx
                    && row_idx == exclude_idx
                {
                    continue;
                }
                if existing_row[col_idx] == *unique_value {
                    return Err(RustqlError::ConstraintViolation {
                        kind: ConstraintKind::Unique,
                        message: format!(
                            "Unique constraint violation: Duplicate value for column '{}'",
                            col_def.name
                        ),
                    });
                }
            }
        }
    }
    Ok(())
}

pub fn validate_foreign_keys_for_insert(
    db: &Database,
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<(), RustqlError> {
    for (col_idx, col_def) in columns.iter().enumerate() {
        if let Some(ref fk) = col_def.foreign_key {
            let fk_value = &row[col_idx];

            if matches!(fk_value, Value::Null) {
                continue;
            }

            let ref_table = db.tables.get(&fk.referenced_table).ok_or_else(|| {
                format!(
                    "Foreign key constraint violation: Referenced table '{}' does not exist",
                    fk.referenced_table
                )
            })?;

            let ref_col_idx = ref_table
                .columns
                .iter()
                .position(|c| c.name == fk.referenced_column)
                .ok_or_else(|| {
                    format!(
                        "Foreign key constraint violation: Referenced column '{}' does not exist in table '{}'",
                        fk.referenced_column, fk.referenced_table
                    )
                })?;

            let value_exists = ref_table.rows.iter().any(|ref_row| {
                ref_row
                    .get(ref_col_idx)
                    .map(|v| v == fk_value)
                    .unwrap_or(false)
            });

            if !value_exists {
                return Err(RustqlError::ConstraintViolation {
                    kind: ConstraintKind::ForeignKey,
                    message: format!(
                        "Foreign key constraint violation: Value {:?} does not exist in referenced table '{}'.{}",
                        fk_value, fk.referenced_table, fk.referenced_column
                    ),
                });
            }
        }
    }
    Ok(())
}

fn validate_foreign_keys_for_update(
    db: &Database,
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<(), RustqlError> {
    validate_foreign_keys_for_insert(db, columns, row)
}

pub fn handle_foreign_keys_for_delete(
    db: &mut Database,
    table_name: &str,
    columns: &[ColumnDefinition],
    row_to_delete: &[Value],
) -> Result<(), RustqlError> {
    for (other_table_name, other_table) in db.tables.iter_mut() {
        if other_table_name == table_name {
            continue;
        }

        for (col_idx, col_def) in other_table.columns.iter().enumerate() {
            if let Some(ref fk) = col_def.foreign_key
                && fk.referenced_table == table_name
            {
                let ref_col_idx = columns
                        .iter()
                        .position(|c| c.name == fk.referenced_column)
                        .ok_or_else(|| {
                            format!(
                                "Foreign key constraint: Referenced column '{}' not found in table '{}'",
                                fk.referenced_column, table_name
                            )
                        })?;

                let ref_value = &row_to_delete[ref_col_idx];

                let mut rows_to_modify: Vec<usize> = Vec::new();
                for (row_idx, other_row) in other_table.rows.iter().enumerate() {
                    if other_row
                        .get(col_idx)
                        .map(|v| v == ref_value)
                        .unwrap_or(false)
                    {
                        rows_to_modify.push(row_idx);
                    }
                }

                match fk.on_delete {
                    ForeignKeyAction::Restrict | ForeignKeyAction::NoAction => {
                        if !rows_to_modify.is_empty() {
                            return Err(RustqlError::ConstraintViolation {
                                kind: ConstraintKind::ForeignKey,
                                message: format!(
                                    "Foreign key constraint violation: Cannot delete row from '{}' because it is referenced by '{}'",
                                    table_name, other_table_name
                                ),
                            });
                        }
                    }
                    ForeignKeyAction::Cascade => {
                        rows_to_modify.sort();
                        rows_to_modify.reverse();
                        for row_idx in &rows_to_modify {
                            let old_row = other_table.rows[*row_idx].clone();
                            wal::record_wal_entry(WalEntry::DeleteRow {
                                table: other_table_name.clone(),
                                row_index: *row_idx,
                                old_row,
                            });
                            other_table.rows.remove(*row_idx);
                        }
                    }
                    ForeignKeyAction::SetNull => {
                        for row_idx in rows_to_modify {
                            if let Some(row) = other_table.rows.get_mut(row_idx) {
                                wal::record_wal_entry(WalEntry::UpdateRow {
                                    table: other_table_name.clone(),
                                    row_index: row_idx,
                                    old_row: row.clone(),
                                });
                                row[col_idx] = Value::Null;
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn handle_foreign_keys_for_update(
    db: &mut Database,
    table_name: &str,
    columns: &[ColumnDefinition],
    old_row: &[Value],
    new_row: &[Value],
) -> Result<(), RustqlError> {
    for (other_table_name, other_table) in db.tables.iter_mut() {
        if other_table_name == table_name {
            continue;
        }

        for (col_idx, col_def) in other_table.columns.iter().enumerate() {
            if let Some(ref fk) = col_def.foreign_key
                && fk.referenced_table == table_name
            {
                let ref_col_idx = columns
                    .iter()
                    .position(|c| c.name == fk.referenced_column)
                    .ok_or_else(|| {
                        format!(
                            "Foreign key constraint: Referenced column '{}' not found in table '{}'",
                            fk.referenced_column, table_name
                        )
                    })?;

                if old_row[ref_col_idx] == new_row[ref_col_idx] {
                    continue;
                }

                let old_value = &old_row[ref_col_idx];

                let mut rows_to_modify: Vec<usize> = Vec::new();
                for (row_idx, other_row) in other_table.rows.iter().enumerate() {
                    if other_row
                        .get(col_idx)
                        .map(|v| v == old_value)
                        .unwrap_or(false)
                    {
                        rows_to_modify.push(row_idx);
                    }
                }

                match fk.on_update {
                    ForeignKeyAction::Restrict | ForeignKeyAction::NoAction => {
                        if !rows_to_modify.is_empty() {
                            return Err(RustqlError::ConstraintViolation {
                                kind: ConstraintKind::ForeignKey,
                                message: format!(
                                    "Foreign key constraint violation: Cannot update row in '{}' because it is referenced by '{}'",
                                    table_name, other_table_name
                                ),
                            });
                        }
                    }
                    ForeignKeyAction::Cascade => {
                        let new_value = new_row[ref_col_idx].clone();
                        for row_idx in rows_to_modify {
                            if let Some(row) = other_table.rows.get_mut(row_idx) {
                                wal::record_wal_entry(WalEntry::UpdateRow {
                                    table: other_table_name.clone(),
                                    row_index: row_idx,
                                    old_row: row.clone(),
                                });
                                row[col_idx] = new_value.clone();
                            }
                        }
                    }
                    ForeignKeyAction::SetNull => {
                        for row_idx in rows_to_modify {
                            if let Some(row) = other_table.rows.get_mut(row_idx) {
                                wal::record_wal_entry(WalEntry::UpdateRow {
                                    table: other_table_name.clone(),
                                    row_index: row_idx,
                                    old_row: row.clone(),
                                });
                                row[col_idx] = Value::Null;
                            }
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn find_conflict_row(
    db: &Database,
    table_name: &str,
    columns: &[ColumnDefinition],
    conflict_columns: &[String],
    new_row: &[Value],
) -> Option<usize> {
    let table = db.tables.get(table_name)?;
    let conflict_indices: Vec<usize> = conflict_columns
        .iter()
        .filter_map(|name| columns.iter().position(|c| c.name == *name))
        .collect();

    for (row_idx, existing_row) in table.rows.iter().enumerate() {
        let matches = conflict_indices.iter().all(|&col_idx| {
            col_idx < existing_row.len()
                && col_idx < new_row.len()
                && existing_row[col_idx] == new_row[col_idx]
                && !matches!(existing_row[col_idx], Value::Null)
        });
        if matches {
            return Some(row_idx);
        }
    }
    None
}

pub fn validate_check_constraints(
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<(), RustqlError> {
    for col_def in columns {
        if let Some(ref check_expr_str) = col_def.check {
            let wrapped = format!("SELECT * FROM _dummy WHERE {}", check_expr_str);
            if let Ok(tokens) = crate::lexer::tokenize(&wrapped) {
                if let Ok(Statement::Select(select_stmt)) = crate::parser::parse(tokens) {
                    if let Some(where_expr) = select_stmt.where_clause {
                        let result = evaluate_expression(None, &where_expr, columns, row)?;
                        if !result {
                            return Err(RustqlError::ConstraintViolation {
                                kind: ConstraintKind::NotNull,
                                message: format!(
                                    "CHECK constraint violation on column '{}': {}",
                                    col_def.name, check_expr_str
                                ),
                            });
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
