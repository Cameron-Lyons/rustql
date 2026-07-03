use super::*;
use crate::database::RowId;

type PendingDelete = (usize, RowId, Vec<Value>);

pub(crate) fn execute_delete(
    context: &ExecutionContext,
    stmt: DeleteStatement,
) -> Result<QueryResult, RustqlError> {
    let mut db = get_database_write(context);

    let using_matches = collect_delete_using_matches(&db, &stmt)?;
    let (columns, mut rows_to_delete) = collect_delete_rows(&db, &stmt, using_matches.as_ref())?;

    for (_, _, row_to_delete) in &rows_to_delete {
        handle_foreign_keys_for_delete(context, &mut db, &stmt.table, &columns, row_to_delete)?;
    }

    let returning_rows: Vec<Vec<Value>> = if stmt.returning.is_some() {
        rows_to_delete
            .iter()
            .map(|(_, _, row)| row.clone())
            .collect()
    } else {
        Vec::new()
    };
    let deleted_count = rows_to_delete.len();
    let deleted_row_ids: Vec<RowId> = rows_to_delete
        .iter()
        .map(|(_, row_id, _)| *row_id)
        .collect();
    rows_to_delete.sort_by(|(left_idx, _, _), (right_idx, _, _)| right_idx.cmp(left_idx));

    {
        let table = db
            .tables
            .get_mut(&stmt.table)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;

        for (position, row_id, old_row) in &rows_to_delete {
            record_wal_entry(
                context,
                WalEntry::DeleteRow {
                    table: stmt.table.clone(),
                    row_id: *row_id,
                    position: *position,
                    old_row: old_row.clone(),
                },
            );
            table.remove_row_by_id(*row_id).ok_or_else(|| {
                RustqlError::Internal("Missing row id for deleted row".to_string())
            })?;
        }
    }
    ddl::update_indexes_on_delete(&mut db, &stmt.table, &deleted_row_ids)?;

    save_if_not_in_transaction(context, &db)?;

    if let Some(ref returning) = stmt.returning {
        return format_returning(returning, &columns, &returning_rows);
    }

    Ok(command_result(CommandTag::Delete, deleted_count as u64))
}

fn collect_delete_using_matches(
    db: &Database,
    stmt: &DeleteStatement,
) -> Result<Option<HashSet<usize>>, RustqlError> {
    let Some(ref using) = stmt.using else {
        return Ok(None);
    };

    let main_table = db
        .tables
        .get(&stmt.table)
        .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;
    let using_source = build_joined_dml_source(
        db,
        &using.table,
        using.alias.as_deref(),
        &using.joins,
        "DELETE USING",
    )?;

    let main_columns = main_table.columns.clone();
    let mut combined_columns = qualify_columns(&main_columns, &stmt.table);
    combined_columns.extend(using_source.columns.clone());

    let mut matching_indices: HashSet<usize> = HashSet::new();
    for (main_idx, main_row) in main_table.rows.iter().enumerate() {
        for using_row in &using_source.rows {
            let mut combined_row: Vec<Value> = main_row.clone();
            combined_row.extend(using_row.clone());

            let matches = if let Some(ref where_expr) = stmt.where_clause {
                evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
            } else {
                true
            };

            if matches {
                matching_indices.insert(main_idx);
                break;
            }
        }
    }

    Ok(Some(matching_indices))
}

fn collect_delete_rows(
    db: &Database,
    stmt: &DeleteStatement,
    using_matches: Option<&HashSet<usize>>,
) -> Result<(Vec<ColumnDefinition>, Vec<PendingDelete>), RustqlError> {
    let table = db
        .tables
        .get(&stmt.table)
        .ok_or_else(|| RustqlError::TableNotFound(stmt.table.clone()))?;
    let columns = table.columns.clone();

    if let Some(using_set) = using_matches {
        let mut rows_to_delete = Vec::new();
        for position in using_set {
            let row_id = table.row_id_at(*position).ok_or_else(|| {
                RustqlError::Internal("Missing row id for DELETE USING row".to_string())
            })?;
            let row = table.rows.get(*position).ok_or_else(|| {
                RustqlError::Internal("Missing row for DELETE USING row".to_string())
            })?;
            rows_to_delete.push((*position, row_id, row.clone()));
        }
        rows_to_delete.sort_by_key(|(position, _, _)| *position);
        return Ok((columns, rows_to_delete));
    }

    let candidate_row_ids: Option<HashSet<RowId>> = if let Some(ref where_expr) = stmt.where_clause
        && let Some(index_usage) = ddl::find_index_usage(db, &stmt.table, where_expr)
    {
        Some(ddl::get_indexed_rows(db, table, &index_usage)?)
    } else {
        None
    };

    let mut rows_to_delete = Vec::new();
    for (position, (row_id, row)) in table.iter_rows_with_ids().enumerate() {
        if let Some(ref candidate_set) = candidate_row_ids
            && !candidate_set.contains(&row_id)
        {
            continue;
        }

        let should_delete = if let Some(ref where_expr) = stmt.where_clause {
            evaluate_expression(Some(db), where_expr, &columns, row)?
        } else {
            true
        };

        if should_delete {
            rows_to_delete.push((position, row_id, row.clone()));
        }
    }

    Ok((columns, rows_to_delete))
}
