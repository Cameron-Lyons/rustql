use super::*;

pub(crate) fn execute_insert(
    context: &ExecutionContext,
    mut stmt: InsertStatement,
) -> Result<QueryResult, RustqlError> {
    if let Some(source_query) = stmt.source_query.take() {
        let typed_rows = {
            let db = get_database_read(context);
            select::execute_select_internal(Some(context), *source_query, &db)?
        };

        for row in typed_rows.rows {
            stmt.values.push(row);
        }
    }

    let mut db = get_database_write(context);

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
                        .ok_or_else(|| RustqlError::ColumnNotFound(col_name.clone()))?;
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
        coerce_row_to_column_types(&table_ref.columns, values)?;
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
                            evaluate_generated_columns_update(&columns_snapshot, &mut updated_row)?;
                            coerce_row_to_column_types(&columns_snapshot, &mut updated_row)?;
                            validate_not_null_constraints(&columns_snapshot, &updated_row)?;
                            validate_unique_constraints_for_insert(
                                &db,
                                &columns_snapshot,
                                &updated_row,
                                &stmt.table,
                                Some(row_idx),
                            )?;
                            validate_foreign_keys_for_update(&db, &columns_snapshot, &updated_row)?;
                            validate_check_constraints(&columns_snapshot, &updated_row)?;
                            validate_table_constraints_for_insert(
                                &db,
                                &columns_snapshot,
                                &updated_row,
                                &stmt.table,
                                Some(row_idx),
                            )?;
                            let table = db
                                .tables
                                .get_mut(&stmt.table)
                                .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;
                            let row_id = table.row_id_at(row_idx).ok_or_else(|| {
                                RustqlError::Internal(
                                    "Missing row id for conflicting row".to_string(),
                                )
                            })?;
                            let old_row = table.rows[row_idx].clone();
                            table.rows[row_idx] = updated_row.clone();
                            record_wal_entry(
                                context,
                                WalEntry::UpdateRow {
                                    table: stmt.table.clone(),
                                    row_id,
                                    old_row: old_row.clone(),
                                },
                            );
                            ddl::update_indexes_on_update(
                                &mut db,
                                &stmt.table,
                                row_id,
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
        let row_id = table.insert_row(values.clone());
        record_wal_entry(
            context,
            WalEntry::InsertRow {
                table: stmt.table.clone(),
                row_id,
            },
        );
        ddl::update_indexes_on_insert(&mut db, &stmt.table, row_id, values)?;
        if stmt.returning.is_some() {
            affected_rows.push(values.clone());
        }
        inserted_count += 1;
    }

    save_if_not_in_transaction(context, &db)?;

    if let Some(ref returning) = stmt.returning {
        return format_returning(returning, &columns_snapshot, &affected_rows);
    }

    if updated_count > 0 {
        Ok(command_result(
            CommandTag::Insert,
            (inserted_count + updated_count) as u64,
        ))
    } else {
        Ok(command_result(CommandTag::Insert, inserted_count as u64))
    }
}
