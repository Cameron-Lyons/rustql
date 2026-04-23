use super::*;

pub(crate) fn execute_delete(
    context: &ExecutionContext,
    stmt: DeleteStatement,
) -> Result<QueryResult, RustqlError> {
    let mut db = get_database_write(context);

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
                    evaluate_expression(Some(&*db), where_expr, &combined_columns, &combined_row)?
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
            let candidate_indices: Option<HashSet<crate::database::RowId>> =
                if let Some(ref where_expr) = stmt.where_clause {
                    if let Some(index_usage) = ddl::find_index_usage(&db, &stmt.table, where_expr) {
                        Some(ddl::get_indexed_rows(&db, table_ref, &index_usage)?)
                    } else {
                        None
                    }
                } else {
                    None
                };

            if let Some(ref where_expr) = stmt.where_clause {
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

                for (_, _, row) in rows_to_check {
                    if evaluate_expression(Some(&*db), where_expr, &table_ref.columns, row)? {
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
        handle_foreign_keys_for_delete(context, &mut db, &stmt.table, &columns, row_to_delete)?;
    }

    let mut rows_to_delete_indices = {
        let table_ref = db
            .tables
            .get(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;

        let candidate_indices: Option<HashSet<crate::database::RowId>> = if using_matches.is_some()
        {
            using_matches.clone().map(|positions| {
                positions
                    .into_iter()
                    .filter_map(|position| table_ref.row_id_at(position))
                    .collect()
            })
        } else if let Some(ref where_expr) = stmt.where_clause {
            if let Some(index_usage) = ddl::find_index_usage(&db, &stmt.table, where_expr) {
                Some(ddl::get_indexed_rows(&db, table_ref, &index_usage)?)
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
                for row_id in candidate_set {
                    if let Some(idx) = table_ref.position_of_row_id(*row_id) {
                        rows_to_delete_indices.push(idx);
                    }
                }
            }
        } else if let Some(ref where_expr) = stmt.where_clause {
            let rows_to_check: Vec<(usize, &Vec<Value>)> =
                if let Some(ref candidate_set) = candidate_indices {
                    rows.iter()
                        .filter(|(idx, _)| {
                            table_ref
                                .row_id_at(*idx)
                                .map(|row_id| candidate_set.contains(&row_id))
                                .unwrap_or(false)
                        })
                        .map(|(i, r)| (*i, r))
                        .collect()
                } else {
                    rows.iter().map(|(i, r)| (*i, r)).collect()
                };

            for (idx, row) in rows_to_check {
                if evaluate_expression(Some(&*db), where_expr, &columns, row)? {
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
    let deleted_row_ids = {
        let table = db
            .tables
            .get(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;
        rows_to_delete_indices
            .iter()
            .filter_map(|idx| table.row_id_at(*idx))
            .collect::<Vec<_>>()
    };
    rows_to_delete_indices.reverse();

    {
        let table = db
            .tables
            .get_mut(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;

        for idx in &rows_to_delete_indices {
            let row_id = table.row_id_at(*idx).ok_or_else(|| {
                RustqlError::Internal("Missing row id for deleted row".to_string())
            })?;
            let old_row = table.rows[*idx].clone();
            record_wal_entry(
                context,
                WalEntry::DeleteRow {
                    table: stmt.table.clone(),
                    row_id,
                    position: *idx,
                    old_row: old_row.clone(),
                },
            );
            let _ = table.remove_row_by_id(row_id);
        }
    }
    ddl::update_indexes_on_delete(&mut db, &stmt.table, &deleted_row_ids)?;

    save_if_not_in_transaction(context, &db)?;

    if let Some(ref returning) = stmt.returning {
        return format_returning(returning, &columns, &returning_rows);
    }

    Ok(command_result(CommandTag::Delete, deleted_count as u64))
}
