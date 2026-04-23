use super::generated::parse_wrapped_select;
use super::*;

pub(super) fn validate_not_null_constraints(
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

pub(super) fn validate_primary_keys_for_insert(
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

pub(super) fn validate_unique_constraints_for_insert(
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

pub(super) fn validate_foreign_keys_for_insert(
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

pub(super) fn validate_foreign_keys_for_update(
    db: &Database,
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<(), RustqlError> {
    validate_foreign_keys_for_insert(db, columns, row)
}

pub(super) fn handle_foreign_keys_for_delete(
    context: &ExecutionContext,
    db: &mut Database,
    table_name: &str,
    columns: &[ColumnDefinition],
    row_to_delete: &[Value],
) -> Result<(), RustqlError> {
    for (other_table_name, other_table) in db.tables.iter_mut() {
        if other_table_name == table_name {
            continue;
        }

        let foreign_keys: Vec<(usize, ForeignKeyConstraint)> = other_table
            .columns
            .iter()
            .enumerate()
            .filter_map(|(col_idx, col_def)| col_def.foreign_key.clone().map(|fk| (col_idx, fk)))
            .collect();

        for (col_idx, fk) in foreign_keys {
            if fk.referenced_table == table_name {
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
                            let row_id = other_table.row_id_at(*row_idx).ok_or_else(|| {
                                RustqlError::Internal(
                                    "Missing row id for cascading delete".to_string(),
                                )
                            })?;
                            let old_row = other_table.rows[*row_idx].clone();
                            record_wal_entry(
                                context,
                                WalEntry::DeleteRow {
                                    table: other_table_name.clone(),
                                    row_id,
                                    position: *row_idx,
                                    old_row,
                                },
                            );
                            let _ = other_table.remove_row_by_id(row_id);
                        }
                    }
                    ForeignKeyAction::SetNull => {
                        for row_idx in rows_to_modify {
                            let row_id = other_table.row_id_at(row_idx).ok_or_else(|| {
                                RustqlError::Internal(
                                    "Missing row id for foreign key update".to_string(),
                                )
                            })?;
                            if let Some(row) = other_table.rows.get_mut(row_idx) {
                                record_wal_entry(
                                    context,
                                    WalEntry::UpdateRow {
                                        table: other_table_name.clone(),
                                        row_id,
                                        old_row: row.clone(),
                                    },
                                );
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

pub(super) fn handle_foreign_keys_for_update(
    context: &ExecutionContext,
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

        let foreign_keys: Vec<(usize, ForeignKeyConstraint)> = other_table
            .columns
            .iter()
            .enumerate()
            .filter_map(|(col_idx, col_def)| col_def.foreign_key.clone().map(|fk| (col_idx, fk)))
            .collect();

        for (col_idx, fk) in foreign_keys {
            if fk.referenced_table == table_name {
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
                            let row_id = other_table.row_id_at(row_idx).ok_or_else(|| {
                                RustqlError::Internal(
                                    "Missing row id for cascading update".to_string(),
                                )
                            })?;
                            if let Some(row) = other_table.rows.get_mut(row_idx) {
                                record_wal_entry(
                                    context,
                                    WalEntry::UpdateRow {
                                        table: other_table_name.clone(),
                                        row_id,
                                        old_row: row.clone(),
                                    },
                                );
                                row[col_idx] = new_value.clone();
                            }
                        }
                    }
                    ForeignKeyAction::SetNull => {
                        for row_idx in rows_to_modify {
                            let row_id = other_table.row_id_at(row_idx).ok_or_else(|| {
                                RustqlError::Internal(
                                    "Missing row id for foreign key set null".to_string(),
                                )
                            })?;
                            if let Some(row) = other_table.rows.get_mut(row_idx) {
                                record_wal_entry(
                                    context,
                                    WalEntry::UpdateRow {
                                        table: other_table_name.clone(),
                                        row_id,
                                        old_row: row.clone(),
                                    },
                                );
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

pub(super) fn find_conflict_row(
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

pub(super) fn validate_table_constraints_for_insert(
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

pub(super) fn validate_check_constraints(
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<(), RustqlError> {
    for col_def in columns {
        if let Some(ref check_expr_str) = col_def.check {
            let wrapped = format!("SELECT * FROM _dummy WHERE {}", check_expr_str);
            let select_stmt = parse_wrapped_select(&wrapped)?;
            let Some(where_expr) = select_stmt.where_clause else {
                return Err(RustqlError::Internal(format!(
                    "CHECK constraint on column '{}' did not produce a predicate",
                    col_def.name
                )));
            };

            let result = evaluate_expression(None, &where_expr, columns, row)?;
            if !result {
                return Err(RustqlError::ConstraintViolation {
                    kind: ConstraintKind::Check,
                    message: format!(
                        "CHECK constraint violation on column '{}': {}",
                        col_def.name, check_expr_str
                    ),
                });
            }
        }
    }
    Ok(())
}
