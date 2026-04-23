use super::*;

pub(crate) fn execute_update(
    context: &ExecutionContext,
    stmt: UpdateStatement,
) -> Result<QueryResult, RustqlError> {
    let mut db = get_database_write(context);

    let candidate_indices: Option<HashSet<crate::database::RowId>> = {
        let table_ref_immut = db
            .tables
            .get(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;

        if let Some(ref where_expr) = stmt.where_clause {
            if let Some(index_usage) = ddl::find_index_usage(&db, &stmt.table, where_expr) {
                Some(ddl::get_indexed_rows(&db, table_ref_immut, &index_usage)?)
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
            coerce_row_to_column_types(&table_ref.columns, &mut updated_row)?;
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
            rows_to_update.push((row_idx, row_id, updated_row));
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
