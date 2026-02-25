use crate::ast::*;
use crate::database::Database;
use crate::error::{ConstraintKind, RustqlError};
use crate::wal::{self, WalEntry};
use std::collections::HashSet;

use super::expr::{
    evaluate_expression, evaluate_value_expression, evaluate_value_expression_with_db,
    format_value, parse_value_from_string,
};
use super::{get_database_write, save_if_not_in_transaction};

fn format_returning(
    returning: &[Column],
    columns: &[ColumnDefinition],
    rows: &[Vec<Value>],
) -> Result<String, RustqlError> {
    let mut headers: Vec<String> = Vec::new();
    for col in returning {
        match col {
            Column::All => {
                for c in columns {
                    headers.push(c.name.clone());
                }
            }
            Column::Named { name, alias } => {
                headers.push(alias.clone().unwrap_or_else(|| name.clone()));
            }
            Column::Expression { alias, .. } => {
                headers.push(alias.clone().unwrap_or_else(|| "?column?".to_string()));
            }
            _ => {
                headers.push("?column?".to_string());
            }
        }
    }

    let mut result = String::new();
    for h in &headers {
        result.push_str(&format!("{}\t", h));
    }
    result.push('\n');
    result.push_str(&"-".repeat(40));
    result.push('\n');

    for row in rows {
        let mut projected: Vec<Value> = Vec::new();
        for col in returning {
            match col {
                Column::All => {
                    for v in row {
                        projected.push(v.clone());
                    }
                }
                Column::Named { name, .. } => {
                    let col_name = if name.contains('.') {
                        name.split('.').next_back().unwrap_or(name)
                    } else {
                        name.as_str()
                    };
                    let idx = columns
                        .iter()
                        .position(|c| c.name == col_name)
                        .ok_or_else(|| RustqlError::ColumnNotFound(name.clone()))?;
                    projected.push(row[idx].clone());
                }
                Column::Expression { expr, .. } => {
                    let val = evaluate_value_expression(expr, columns, row)?;
                    projected.push(val);
                }
                _ => {
                    projected.push(Value::Null);
                }
            }
        }
        for val in &projected {
            result.push_str(&format!("{}\t", format_value(val)));
        }
        result.push('\n');
    }

    Ok(result)
}

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

    for values in &mut mapped_values {
        evaluate_generated_columns(&table_ref.columns, values, &stmt.columns)?;
    }

    let columns_snapshot = table_ref.columns.clone();

    let mut inserted_count = 0usize;
    let mut updated_count = 0usize;
    let mut affected_rows: Vec<Vec<Value>> = Vec::new();

    for values in &mapped_values {
        validate_not_null_constraints(&columns_snapshot, values)?;
        validate_foreign_keys_for_insert(&db, &columns_snapshot, values)?;
        validate_check_constraints(&columns_snapshot, values)?;
        validate_table_constraints_for_insert(&db, &columns_snapshot, values, &stmt.table, None)?;

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
                                    updated_row[idx] = evaluate_value_expression_with_db(
                                        &assignment.value,
                                        &columns_snapshot,
                                        values,
                                        Some(&*db),
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
                            if stmt.returning.is_some() {
                                affected_rows.push(updated_row);
                            }
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
        if stmt.returning.is_some() {
            affected_rows.push(values.clone());
        }
        inserted_count += 1;
    }

    save_if_not_in_transaction(&db)?;

    if let Some(ref returning) = stmt.returning {
        return format_returning(returning, &columns_snapshot, &affected_rows);
    }

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
            evaluate_expression(Some(&*db), where_expr, &table_ref.columns, row)?
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
                    updated_row[idx] = evaluate_value_expression_with_db(
                        &assignment.value,
                        &table_ref.columns,
                        row,
                        Some(&*db),
                    )?;
                } else {
                    return Err(RustqlError::ColumnNotFound(assignment.column.clone()));
                }
            }
            evaluate_generated_columns_update(&table_ref.columns, &mut updated_row)?;
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
            validate_table_constraints_for_insert(
                &db,
                &table_ref.columns,
                &updated_row,
                &stmt.table,
                Some(row_idx),
            )?;
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
    let mut returning_rows: Vec<Vec<Value>> = Vec::new();
    for (row_idx, old_row, updated_row) in update_info {
        if stmt.returning.is_some() {
            returning_rows.push(updated_row.clone());
        }
        super::ddl::update_indexes_on_update(
            &mut db,
            &stmt.table,
            row_idx,
            &old_row,
            &updated_row,
        )?;
    }
    save_if_not_in_transaction(&db)?;

    if let Some(ref returning) = stmt.returning {
        return format_returning(returning, &columns, &returning_rows);
    }

    Ok(format!("{} row(s) updated", updated_count))
}

pub fn execute_delete(stmt: DeleteStatement) -> Result<String, RustqlError> {
    let mut db = get_database_write();

    let using_matches: Option<HashSet<usize>> = if let Some(ref using) = stmt.using {
        let using_table = db
            .tables
            .get(&using.table)
            .ok_or_else(|| RustqlError::TableNotFound(using.table.clone()))?;
        let main_table = db
            .tables
            .get(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;

        let using_rows = using_table.rows.clone();
        let using_columns = using_table.columns.clone();
        let main_columns = main_table.columns.clone();

        let mut combined_columns: Vec<ColumnDefinition> = main_columns.clone();
        combined_columns.extend(using_columns.clone());

        let mut matching_indices: HashSet<usize> = HashSet::new();
        for (main_idx, main_row) in main_table.rows.iter().enumerate() {
            for using_row in &using_rows {
                let mut combined_row: Vec<Value> = main_row.clone();
                combined_row.extend(using_row.clone());

                let matches = if let Some(ref where_expr) = stmt.where_clause {
                    evaluate_expression(Some(&*db), where_expr, &combined_columns, &combined_row)
                        .unwrap_or(false)
                } else {
                    true
                };

                if matches {
                    matching_indices.insert(main_idx);
                    break;
                }
            }
        }
        Some(matching_indices)
    } else {
        None
    };

    let (columns, rows_to_delete) = {
        let table_ref = db
            .tables
            .get(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;

        let mut rows: Vec<Vec<Value>> = Vec::new();

        if let Some(ref using_set) = using_matches {
            for idx in using_set {
                if let Some(row) = table_ref.rows.get(*idx) {
                    rows.push(row.clone());
                }
            }
        } else {
            let candidate_indices: Option<HashSet<usize>> =
                if let Some(ref where_expr) = stmt.where_clause {
                    if let Some(index_usage) =
                        super::ddl::find_index_usage(&db, &stmt.table, where_expr)
                    {
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
                    if evaluate_expression(Some(&*db), where_expr, &table_ref.columns, row)
                        .unwrap_or(false)
                    {
                        rows.push(row.clone());
                    }
                }
            } else {
                rows = table_ref.rows.clone();
            }
        }

        (table_ref.columns.clone(), rows)
    };

    for row_to_delete in &rows_to_delete {
        handle_foreign_keys_for_delete(&mut db, &stmt.table, &columns, row_to_delete)?;
    }

    let mut rows_to_delete_indices = {
        let table_ref = db
            .tables
            .get(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;

        let candidate_indices: Option<HashSet<usize>> = if using_matches.is_some() {
            using_matches.clone()
        } else if let Some(ref where_expr) = stmt.where_clause {
            if let Some(index_usage) = super::ddl::find_index_usage(&db, &stmt.table, where_expr) {
                super::ddl::get_indexed_rows(&db, table_ref, &index_usage).ok()
            } else {
                None
            }
        } else {
            None
        };

        let columns = table_ref.columns.clone();
        let rows: Vec<(usize, Vec<Value>)> = table_ref
            .rows
            .iter()
            .enumerate()
            .map(|(i, r)| (i, r.clone()))
            .collect();

        let mut rows_to_delete_indices = Vec::new();

        if using_matches.is_some() {
            if let Some(ref candidate_set) = candidate_indices {
                for &idx in candidate_set {
                    rows_to_delete_indices.push(idx);
                }
            }
        } else if let Some(ref where_expr) = stmt.where_clause {
            let rows_to_check: Vec<(usize, &Vec<Value>)> =
                if let Some(ref candidate_set) = candidate_indices {
                    rows.iter()
                        .filter(|(idx, _)| candidate_set.contains(idx))
                        .map(|(i, r)| (*i, r))
                        .collect()
                } else {
                    rows.iter().map(|(i, r)| (*i, r)).collect()
                };

            for (idx, row) in rows_to_check {
                if evaluate_expression(Some(&*db), where_expr, &columns, row).unwrap_or(false) {
                    rows_to_delete_indices.push(idx);
                }
            }
        } else {
            rows_to_delete_indices = (0..rows.len()).collect();
        }

        rows_to_delete_indices.sort();
        rows_to_delete_indices
    };

    let returning_rows = {
        let table = db
            .tables
            .get(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;

        let mut returning_rows: Vec<Vec<Value>> = Vec::new();
        if stmt.returning.is_some() {
            for &idx in &rows_to_delete_indices {
                returning_rows.push(table.rows[idx].clone());
            }
        }
        returning_rows
    };

    let deleted_count = rows_to_delete_indices.len();
    rows_to_delete_indices.reverse();

    {
        let table = db
            .tables
            .get_mut(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;

        for idx in &rows_to_delete_indices {
            let old_row = table.rows[*idx].clone();
            wal::record_wal_entry(WalEntry::DeleteRow {
                table: stmt.table.clone(),
                row_index: *idx,
                old_row,
            });
            table.rows.remove(*idx);
        }
    }
    super::ddl::update_indexes_on_delete(&mut db, &stmt.table, &rows_to_delete_indices)?;

    save_if_not_in_transaction(&db)?;

    if let Some(ref returning) = stmt.returning {
        return format_returning(returning, &columns, &returning_rows);
    }

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

pub fn validate_table_constraints_for_insert(
    db: &Database,
    columns: &[ColumnDefinition],
    row: &[Value],
    table_name: &str,
    exclude_row_idx: Option<usize>,
) -> Result<(), RustqlError> {
    let table = db
        .tables
        .get(table_name)
        .ok_or_else(|| RustqlError::TableNotFound(table_name.to_string()))?;

    for constraint in &table.constraints {
        match constraint {
            crate::ast::TableConstraint::PrimaryKey {
                columns: pk_cols, ..
            } => {
                let col_indices: Vec<usize> = pk_cols
                    .iter()
                    .filter_map(|name| columns.iter().position(|c| c.name == *name))
                    .collect();
                if col_indices.len() != pk_cols.len() {
                    continue;
                }
                let key: Vec<Value> = col_indices.iter().map(|&i| row[i].clone()).collect();
                if key.iter().any(|v| matches!(v, Value::Null)) {
                    return Err(RustqlError::ConstraintViolation {
                        kind: crate::error::ConstraintKind::PrimaryKey,
                        message: format!(
                            "Composite PRIMARY KEY constraint violation: NULL value in columns {:?}",
                            pk_cols
                        ),
                    });
                }
                for (row_idx, existing_row) in table.rows.iter().enumerate() {
                    if let Some(exclude_idx) = exclude_row_idx
                        && row_idx == exclude_idx
                    {
                        continue;
                    }
                    let existing_key: Vec<Value> = col_indices
                        .iter()
                        .map(|&i| existing_row[i].clone())
                        .collect();
                    if existing_key == key {
                        return Err(RustqlError::ConstraintViolation {
                            kind: crate::error::ConstraintKind::PrimaryKey,
                            message: format!(
                                "Composite PRIMARY KEY constraint violation: duplicate value for columns {:?}",
                                pk_cols
                            ),
                        });
                    }
                }
            }
            crate::ast::TableConstraint::Unique {
                columns: uq_cols, ..
            } => {
                let col_indices: Vec<usize> = uq_cols
                    .iter()
                    .filter_map(|name| columns.iter().position(|c| c.name == *name))
                    .collect();
                if col_indices.len() != uq_cols.len() {
                    continue;
                }
                let key: Vec<Value> = col_indices.iter().map(|&i| row[i].clone()).collect();
                if key.iter().all(|v| matches!(v, Value::Null)) {
                    continue;
                }
                for (row_idx, existing_row) in table.rows.iter().enumerate() {
                    if let Some(exclude_idx) = exclude_row_idx
                        && row_idx == exclude_idx
                    {
                        continue;
                    }
                    let existing_key: Vec<Value> = col_indices
                        .iter()
                        .map(|&i| existing_row[i].clone())
                        .collect();
                    if existing_key == key {
                        return Err(RustqlError::ConstraintViolation {
                            kind: crate::error::ConstraintKind::Unique,
                            message: format!(
                                "Composite UNIQUE constraint violation: duplicate value for columns {:?}",
                                uq_cols
                            ),
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

pub fn execute_merge(stmt: MergeStatement) -> Result<String, RustqlError> {
    let mut db = get_database_write();

    let target_columns = {
        let table = db
            .tables
            .get(&stmt.target_table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.target_table.clone()))?;
        table.columns.clone()
    };

    let source_rows: Vec<Vec<Value>>;
    let source_columns: Vec<ColumnDefinition>;
    let source_alias: Option<String>;

    match &stmt.source {
        MergeSource::Table { name, alias } => {
            let table = db
                .tables
                .get(name)
                .ok_or_else(|| RustqlError::TableNotFound(name.clone()))?;
            source_rows = table.rows.clone();
            source_columns = table.columns.clone();
            source_alias = alias.clone();
        }
        MergeSource::Subquery { query, alias } => {
            let result = super::select::execute_select_internal(*query.clone(), &db)?;
            source_columns = result
                .headers
                .iter()
                .map(|name| ColumnDefinition {
                    name: name.clone(),
                    data_type: DataType::Text,
                    nullable: true,
                    primary_key: false,
                    unique: false,
                    default_value: None,
                    foreign_key: None,
                    check: None,
                    auto_increment: false,
                    generated: None,
                })
                .collect();
            source_rows = result.rows;
            source_alias = Some(alias.clone());
        }
    }

    let source_name = match &stmt.source {
        MergeSource::Table { name, alias } => alias.as_deref().unwrap_or(name).to_string(),
        MergeSource::Subquery { alias, .. } => alias.clone(),
    };
    let _ = source_alias;

    let mut combined_columns: Vec<ColumnDefinition> = target_columns
        .iter()
        .map(|c| {
            let mut c = c.clone();
            c.name = format!("{}.{}", stmt.target_table, c.name);
            c
        })
        .collect();
    combined_columns.extend(source_columns.iter().map(|c| {
        let mut c = c.clone();
        c.name = format!("{}.{}", source_name, c.name);
        c
    }));

    let mut affected = 0usize;

    for source_row in &source_rows {
        let target_table = db
            .tables
            .get(&stmt.target_table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.target_table.clone()))?;
        let target_rows_snapshot: Vec<(usize, Vec<Value>)> = target_table
            .rows
            .iter()
            .enumerate()
            .map(|(i, r)| (i, r.clone()))
            .collect();

        let mut matched_indices: Vec<usize> = Vec::new();
        for (row_idx, target_row) in &target_rows_snapshot {
            let mut combined_row: Vec<Value> = target_row.clone();
            combined_row.extend(source_row.clone());
            if evaluate_expression(
                Some(&*db),
                &stmt.on_condition,
                &combined_columns,
                &combined_row,
            )? {
                matched_indices.push(*row_idx);
            }
        }

        let is_matched = !matched_indices.is_empty();

        for when_clause in &stmt.when_clauses {
            match when_clause {
                MergeWhenClause::Matched { condition, action } if is_matched => {
                    for &row_idx in &matched_indices {
                        let target_row = target_rows_snapshot
                            .iter()
                            .find(|(i, _)| *i == row_idx)
                            .map(|(_, r)| r.clone())
                            .unwrap();
                        let mut combined_row: Vec<Value> = target_row.clone();
                        combined_row.extend(source_row.clone());

                        let passes_condition = if let Some(cond) = condition {
                            evaluate_expression(Some(&*db), cond, &combined_columns, &combined_row)?
                        } else {
                            true
                        };

                        if !passes_condition {
                            continue;
                        }

                        match action {
                            MergeMatchedAction::Update { assignments } => {
                                let mut updated_row = target_row.clone();
                                for assignment in assignments {
                                    if let Some(idx) = target_columns
                                        .iter()
                                        .position(|c| c.name == assignment.column)
                                    {
                                        updated_row[idx] = evaluate_value_expression(
                                            &assignment.value,
                                            &combined_columns,
                                            &combined_row,
                                        )?;
                                    }
                                }
                                let table = db.tables.get_mut(&stmt.target_table).unwrap();
                                let old_row = table.rows[row_idx].clone();
                                table.rows[row_idx] = updated_row.clone();
                                wal::record_wal_entry(WalEntry::UpdateRow {
                                    table: stmt.target_table.clone(),
                                    row_index: row_idx,
                                    old_row,
                                });
                                affected += 1;
                            }
                            MergeMatchedAction::Delete => {
                                let table = db.tables.get_mut(&stmt.target_table).unwrap();
                                let old_row = table.rows[row_idx].clone();
                                wal::record_wal_entry(WalEntry::DeleteRow {
                                    table: stmt.target_table.clone(),
                                    row_index: row_idx,
                                    old_row,
                                });
                                table.rows.remove(row_idx);
                                affected += 1;
                            }
                        }
                    }
                }
                MergeWhenClause::NotMatched { condition, action } if !is_matched => {
                    let dummy_target: Vec<Value> =
                        target_columns.iter().map(|_| Value::Null).collect();
                    let mut combined_row: Vec<Value> = dummy_target;
                    combined_row.extend(source_row.clone());

                    let passes_condition = if let Some(cond) = condition {
                        evaluate_expression(Some(&*db), cond, &combined_columns, &combined_row)?
                    } else {
                        true
                    };

                    if !passes_condition {
                        continue;
                    }

                    match action {
                        MergeNotMatchedAction::Insert { columns, values } => {
                            let mut new_row: Vec<Value> = target_columns
                                .iter()
                                .map(|c| c.default_value.clone().unwrap_or(Value::Null))
                                .collect();

                            if let Some(cols) = columns {
                                for (i, col_name) in cols.iter().enumerate() {
                                    if let Some(col_idx) =
                                        target_columns.iter().position(|c| c.name == *col_name)
                                        && i < values.len()
                                    {
                                        new_row[col_idx] = evaluate_value_expression(
                                            &values[i],
                                            &combined_columns,
                                            &combined_row,
                                        )?;
                                    }
                                }
                            } else {
                                for (i, val_expr) in values.iter().enumerate() {
                                    if i < new_row.len() {
                                        new_row[i] = evaluate_value_expression(
                                            val_expr,
                                            &combined_columns,
                                            &combined_row,
                                        )?;
                                    }
                                }
                            }

                            let table = db.tables.get_mut(&stmt.target_table).unwrap();
                            let row_idx = table.rows.len();
                            table.rows.push(new_row);
                            wal::record_wal_entry(WalEntry::InsertRow {
                                table: stmt.target_table.clone(),
                                row_index: row_idx,
                            });
                            affected += 1;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    save_if_not_in_transaction(&db)?;
    Ok(format!("{} row(s) affected", affected))
}

pub fn validate_check_constraints(
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<(), RustqlError> {
    for col_def in columns {
        if let Some(ref check_expr_str) = col_def.check {
            let wrapped = format!("SELECT * FROM _dummy WHERE {}", check_expr_str);
            if let Ok(tokens) = crate::lexer::tokenize(&wrapped)
                && let Ok(Statement::Select(select_stmt)) = crate::parser::parse(tokens)
                && let Some(where_expr) = select_stmt.where_clause
            {
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
    Ok(())
}

fn evaluate_generated_columns(
    columns: &[ColumnDefinition],
    row: &mut [Value],
    insert_columns: &Option<Vec<String>>,
) -> Result<(), RustqlError> {
    for (col_idx, col_def) in columns.iter().enumerate() {
        if let Some(ref generated) = col_def.generated {
            if generated.always
                && let Some(specified_cols) = insert_columns
                && specified_cols.iter().any(|c| c == &col_def.name)
            {
                return Err(RustqlError::Internal(format!(
                    "Cannot insert into generated column '{}'",
                    col_def.name
                )));
            }

            let wrapped = format!("SELECT {} FROM _dummy", generated.expr_sql);
            if let Ok(tokens) = crate::lexer::tokenize(&wrapped)
                && let Ok(Statement::Select(select_stmt)) = crate::parser::parse(tokens)
            {
                if let Some(Column::Expression { expr, .. }) = select_stmt.columns.first() {
                    if col_idx < row.len() {
                        row[col_idx] = evaluate_value_expression(expr, columns, row)?;
                    }
                } else if let Some(Column::Named { name, .. }) = select_stmt.columns.first()
                    && let Some(src_idx) = columns.iter().position(|c| c.name == *name)
                    && col_idx < row.len()
                    && src_idx < row.len()
                {
                    row[col_idx] = row[src_idx].clone();
                }
            }
        }
    }
    Ok(())
}

fn evaluate_generated_columns_update(
    columns: &[ColumnDefinition],
    row: &mut [Value],
) -> Result<(), RustqlError> {
    for (col_idx, col_def) in columns.iter().enumerate() {
        if let Some(ref generated) = col_def.generated {
            let wrapped = format!("SELECT {} FROM _dummy", generated.expr_sql);
            if let Ok(tokens) = crate::lexer::tokenize(&wrapped)
                && let Ok(Statement::Select(select_stmt)) = crate::parser::parse(tokens)
            {
                if let Some(Column::Expression { expr, .. }) = select_stmt.columns.first() {
                    if col_idx < row.len() {
                        row[col_idx] = evaluate_value_expression(expr, columns, row)?;
                    }
                } else if let Some(Column::Named { name, .. }) = select_stmt.columns.first()
                    && let Some(src_idx) = columns.iter().position(|c| c.name == *name)
                    && col_idx < row.len()
                    && src_idx < row.len()
                {
                    row[col_idx] = row[src_idx].clone();
                }
            }
        }
    }
    Ok(())
}
