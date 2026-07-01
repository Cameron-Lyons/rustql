use super::*;

type PendingUpdate = (usize, crate::database::RowId, Vec<Value>);

pub(crate) fn execute_update(
    context: &ExecutionContext,
    stmt: UpdateStatement,
) -> Result<QueryResult, RustqlError> {
    let mut db = get_database_write(context);

    let rows_to_update = if stmt.from.is_some() {
        collect_update_from_rows(&db, &stmt)?
    } else {
        collect_simple_update_rows(&db, &stmt)?
    };

    let updated_count = rows_to_update.len();
    let columns = db
        .tables
        .get(&stmt.table)
        .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?
        .columns
        .clone();
    let mut update_info = Vec::new();
    {
        let table = db
            .tables
            .get_mut(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;
        for (row_idx, row_id, updated_row) in rows_to_update {
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
            update_info.push((row_id, old_row, updated_row));
        }
    }
    for (_, old_row, updated_row) in &update_info {
        handle_foreign_keys_for_update(
            context,
            &mut db,
            &stmt.table,
            &columns,
            old_row,
            updated_row,
        )?;
    }
    let mut returning_rows: Vec<Vec<Value>> = Vec::new();
    for (row_id, old_row, updated_row) in update_info {
        if stmt.returning.is_some() {
            returning_rows.push(updated_row.clone());
        }
        ddl::update_indexes_on_update(&mut db, &stmt.table, row_id, &old_row, &updated_row)?;
    }
    save_if_not_in_transaction(context, &db)?;

    if let Some(ref returning) = stmt.returning {
        return format_returning(returning, &columns, &returning_rows);
    }

    Ok(command_result(CommandTag::Update, updated_count as u64))
}

fn collect_simple_update_rows(
    db: &Database,
    stmt: &UpdateStatement,
) -> Result<Vec<PendingUpdate>, RustqlError> {
    let candidate_indices: Option<HashSet<crate::database::RowId>> = {
        let table_ref_immut = db
            .tables
            .get(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;

        if let Some(ref where_expr) = stmt.where_clause {
            if let Some(index_usage) = ddl::find_index_usage(db, &stmt.table, where_expr) {
                Some(ddl::get_indexed_rows(db, table_ref_immut, &index_usage)?)
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

    let mut rows_to_update: Vec<(usize, crate::database::RowId, Vec<Value>)> = Vec::new();

    let rows_to_check: Vec<(usize, crate::database::RowId, &Vec<Value>)> =
        if let Some(ref candidate_set) = candidate_indices {
            table_ref
                .iter_rows_with_ids()
                .enumerate()
                .filter(|(_, (row_id, _))| candidate_set.contains(row_id))
                .map(|(idx, (row_id, row))| (idx, row_id, row))
                .collect()
        } else {
            table_ref
                .iter_rows_with_ids()
                .enumerate()
                .map(|(idx, (row_id, row))| (idx, row_id, row))
                .collect()
        };

    for (row_idx, row_id, row) in rows_to_check {
        let should_update = if let Some(ref where_expr) = stmt.where_clause {
            evaluate_expression(Some(db), where_expr, &table_ref.columns, row)?
        } else {
            true
        };
        if should_update {
            let mut updated_row =
                apply_update_assignments(db, stmt, &table_ref.columns, &table_ref.columns, row)?;
            validate_updated_row(db, stmt, &table_ref.columns, row_idx, &mut updated_row)?;
            rows_to_update.push((row_idx, row_id, updated_row));
        }
    }

    Ok(rows_to_update)
}

fn collect_update_from_rows(
    db: &Database,
    stmt: &UpdateStatement,
) -> Result<Vec<PendingUpdate>, RustqlError> {
    let update_from = stmt.from.as_ref().ok_or_else(|| {
        RustqlError::Internal("UPDATE FROM collection requires a source table".to_string())
    })?;

    let target_table = db
        .tables
        .get(&stmt.table)
        .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;
    let source = build_joined_dml_source(
        db,
        &update_from.table,
        update_from.alias.as_deref(),
        &update_from.joins,
        "UPDATE FROM",
    )?;

    let target_columns = target_table.columns.clone();
    let mut combined_columns = qualify_columns(&target_columns, &stmt.table);
    combined_columns.extend(source.columns.clone());

    let target_rows: Vec<(usize, crate::database::RowId, Vec<Value>)> = target_table
        .iter_rows_with_ids()
        .enumerate()
        .map(|(idx, (row_id, row))| (idx, row_id, row.clone()))
        .collect();

    let mut rows_to_update = Vec::new();
    for (row_idx, row_id, target_row) in target_rows {
        let mut matched_row: Option<Vec<Value>> = None;
        for source_row in &source.rows {
            let mut combined_row = target_row.clone();
            combined_row.extend(source_row.clone());
            let matches = if let Some(ref where_expr) = stmt.where_clause {
                evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
            } else {
                true
            };
            if matches {
                matched_row = Some(combined_row);
                break;
            }
        }

        if let Some(combined_row) = matched_row {
            let mut updated_row = apply_update_assignments(
                db,
                stmt,
                &target_columns,
                &combined_columns,
                &combined_row,
            )?;
            validate_updated_row(db, stmt, &target_columns, row_idx, &mut updated_row)?;
            rows_to_update.push((row_idx, row_id, updated_row));
        }
    }

    Ok(rows_to_update)
}

fn apply_update_assignments(
    db: &Database,
    stmt: &UpdateStatement,
    target_columns: &[ColumnDefinition],
    eval_columns: &[ColumnDefinition],
    eval_row: &[Value],
) -> Result<Vec<Value>, RustqlError> {
    let mut updated_row: Vec<Value> = eval_row
        .iter()
        .take(target_columns.len())
        .cloned()
        .collect();
    for assignment in &stmt.assignments {
        if let Some(idx) = target_columns
            .iter()
            .position(|c| c.name == assignment.column)
        {
            updated_row[idx] = evaluate_assignment_value(
                &assignment.value,
                &target_columns[idx],
                eval_columns,
                eval_row,
                db,
            )?;
        } else {
            return Err(RustqlError::ColumnNotFound(assignment.column.clone()));
        }
    }
    Ok(updated_row)
}

fn validate_updated_row(
    db: &Database,
    stmt: &UpdateStatement,
    target_columns: &[ColumnDefinition],
    row_idx: usize,
    updated_row: &mut [Value],
) -> Result<(), RustqlError> {
    evaluate_generated_columns_update(target_columns, updated_row)?;
    coerce_row_to_column_types(target_columns, updated_row)?;
    validate_not_null_constraints(target_columns, updated_row)?;
    validate_unique_constraints_for_insert(
        db,
        target_columns,
        updated_row,
        &stmt.table,
        Some(row_idx),
    )?;
    validate_foreign_keys_for_update(db, target_columns, updated_row)?;
    validate_check_constraints(target_columns, updated_row)?;
    validate_table_constraints_for_insert(
        db,
        target_columns,
        updated_row,
        &stmt.table,
        Some(row_idx),
    )?;
    Ok(())
}
