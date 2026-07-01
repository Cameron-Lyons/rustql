use super::*;

pub(crate) fn execute_merge(
    context: &ExecutionContext,
    stmt: MergeStatement,
) -> Result<QueryResult, RustqlError> {
    let mut db = get_database_write(context);

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
            let result = select::execute_select_internal(Some(context), *query.clone(), &db)?;
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
                            .ok_or_else(|| {
                                RustqlError::Internal(
                                    "Missing matched target row for merge".to_string(),
                                )
                            })?;
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
                                        updated_row[idx] = evaluate_assignment_value(
                                            &assignment.value,
                                            &target_columns[idx],
                                            &combined_columns,
                                            &combined_row,
                                            &*db,
                                        )?;
                                    }
                                }
                                evaluate_generated_columns_update(
                                    &target_columns,
                                    &mut updated_row,
                                )?;
                                coerce_row_to_column_types(&target_columns, &mut updated_row)?;
                                validate_not_null_constraints(&target_columns, &updated_row)?;
                                validate_unique_constraints_for_insert(
                                    &db,
                                    &target_columns,
                                    &updated_row,
                                    &stmt.target_table,
                                    Some(row_idx),
                                )?;
                                validate_foreign_keys_for_update(
                                    &db,
                                    &target_columns,
                                    &updated_row,
                                )?;
                                validate_check_constraints(&target_columns, &updated_row)?;
                                validate_table_constraints_for_insert(
                                    &db,
                                    &target_columns,
                                    &updated_row,
                                    &stmt.target_table,
                                    Some(row_idx),
                                )?;
                                let table =
                                    db.tables.get_mut(&stmt.target_table).ok_or_else(|| {
                                        RustqlError::TableNotFound(stmt.target_table.clone())
                                    })?;
                                let row_id = table.row_id_at(row_idx).ok_or_else(|| {
                                    RustqlError::Internal(
                                        "Missing row id for merge update".to_string(),
                                    )
                                })?;
                                let old_row = table.rows[row_idx].clone();
                                table.rows[row_idx] = updated_row.clone();
                                record_wal_entry(
                                    context,
                                    WalEntry::UpdateRow {
                                        table: stmt.target_table.clone(),
                                        row_id,
                                        old_row: old_row.clone(),
                                    },
                                );
                                handle_foreign_keys_for_update(
                                    context,
                                    &mut db,
                                    &stmt.target_table,
                                    &target_columns,
                                    &old_row,
                                    &updated_row,
                                )?;
                                ddl::update_indexes_on_update(
                                    &mut db,
                                    &stmt.target_table,
                                    row_id,
                                    &old_row,
                                    &updated_row,
                                )?;
                                affected += 1;
                            }
                            MergeMatchedAction::Delete => {
                                handle_foreign_keys_for_delete(
                                    context,
                                    &mut db,
                                    &stmt.target_table,
                                    &target_columns,
                                    &target_row,
                                )?;
                                let table =
                                    db.tables.get_mut(&stmt.target_table).ok_or_else(|| {
                                        RustqlError::TableNotFound(stmt.target_table.clone())
                                    })?;
                                let row_id = table.row_id_at(row_idx).ok_or_else(|| {
                                    RustqlError::Internal(
                                        "Missing row id for merge delete".to_string(),
                                    )
                                })?;
                                let old_row = table.rows[row_idx].clone();
                                record_wal_entry(
                                    context,
                                    WalEntry::DeleteRow {
                                        table: stmt.target_table.clone(),
                                        row_id,
                                        position: row_idx,
                                        old_row,
                                    },
                                );
                                let _ = table.remove_row_by_id(row_id);
                                ddl::update_indexes_on_delete(
                                    &mut db,
                                    &stmt.target_table,
                                    &[row_id],
                                )?;
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
                                        new_row[col_idx] = evaluate_merge_insert_value(
                                            &values[i],
                                            &target_columns[col_idx],
                                            &combined_columns,
                                            &combined_row,
                                        )?;
                                    }
                                }
                            } else {
                                for (i, val_expr) in values.iter().enumerate() {
                                    if i < new_row.len() {
                                        new_row[i] = evaluate_merge_insert_value(
                                            val_expr,
                                            &target_columns[i],
                                            &combined_columns,
                                            &combined_row,
                                        )?;
                                    }
                                }
                            }
                            apply_merge_auto_increment_values(
                                &db,
                                &stmt.target_table,
                                &target_columns,
                                &mut new_row,
                            )?;
                            evaluate_generated_columns(&target_columns, &mut new_row, columns)?;
                            coerce_row_to_column_types(&target_columns, &mut new_row)?;
                            validate_not_null_constraints(&target_columns, &new_row)?;
                            validate_foreign_keys_for_insert(&db, &target_columns, &new_row)?;
                            validate_check_constraints(&target_columns, &new_row)?;
                            validate_table_constraints_for_insert(
                                &db,
                                &target_columns,
                                &new_row,
                                &stmt.target_table,
                                None,
                            )?;
                            validate_primary_keys_for_insert(
                                &db,
                                &target_columns,
                                &new_row,
                                &stmt.target_table,
                            )?;
                            validate_unique_constraints_for_insert(
                                &db,
                                &target_columns,
                                &new_row,
                                &stmt.target_table,
                                None,
                            )?;

                            let table = db.tables.get_mut(&stmt.target_table).ok_or_else(|| {
                                RustqlError::TableNotFound(stmt.target_table.clone())
                            })?;
                            let row_id = table.insert_row(new_row.clone());
                            record_wal_entry(
                                context,
                                WalEntry::InsertRow {
                                    table: stmt.target_table.clone(),
                                    row_id,
                                },
                            );
                            ddl::update_indexes_on_insert(
                                &mut db,
                                &stmt.target_table,
                                row_id,
                                &new_row,
                            )?;
                            affected += 1;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    save_if_not_in_transaction(context, &db)?;
    Ok(command_result(CommandTag::Merge, affected as u64))
}

fn apply_merge_auto_increment_values(
    db: &Database,
    table_name: &str,
    columns: &[ColumnDefinition],
    row: &mut [Value],
) -> Result<(), RustqlError> {
    let table = db
        .tables
        .get(table_name)
        .ok_or_else(|| RustqlError::TableNotFound(table_name.to_string()))?;

    for (col_idx, col_def) in columns.iter().enumerate() {
        if col_def.auto_increment && col_idx < row.len() && matches!(row[col_idx], Value::Null) {
            let max_val = table
                .rows
                .iter()
                .filter_map(|existing_row| match existing_row.get(col_idx) {
                    Some(Value::Integer(value)) => Some(*value),
                    _ => None,
                })
                .max()
                .unwrap_or(0);
            row[col_idx] = Value::Integer(max_val + 1);
        }
    }

    Ok(())
}
