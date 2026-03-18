use crate::ast::*;
use crate::database::{DatabaseCatalog, ScopedDatabase, Table};
use crate::error::RustqlError;
use std::borrow::Cow;
use std::collections::HashSet;

use super::ExecutionContext;
use super::expr::evaluate_expression;
use super::select::execute_select_internal;

pub fn perform_multiple_joins(
    context: Option<&ExecutionContext>,
    db: &dyn DatabaseCatalog,
    from_table: &Table,
    from_table_name: &str,
    joins: &[Join],
) -> Result<(Vec<Vec<Value>>, Vec<ColumnDefinition>), RustqlError> {
    let mut current_rows: Vec<Cow<'_, [Value]>> = from_table
        .rows
        .iter()
        .map(|row| Cow::Borrowed(row.as_slice()))
        .collect();
    let mut all_columns = from_table.columns.clone();
    let mut table_names = vec![from_table_name.to_string()];
    let mut table_column_counts = vec![from_table.columns.len()];

    for join in joins {
        if join.lateral
            && let Some((ref subquery, ref alias)) = join.subquery
        {
            let mut joined_rows: Vec<Cow<'_, [Value]>> = Vec::new();
            let mut sub_columns: Option<Vec<ColumnDefinition>> = None;
            let outer_scope_columns =
                lateral_outer_scope_columns(&table_names, &table_column_counts, &all_columns);
            let temp_table_name = format!("__lateral_outer_{}", alias);
            let rewritten_subquery = lateral_subquery_with_outer_scope(subquery, &temp_table_name);
            let mut scoped_db =
                ScopedDatabase::new(db, temp_table_name, outer_scope_columns.clone());

            for current_row in &current_rows {
                let current_row = current_row.as_ref();
                scoped_db.update_temp_row(current_row);
                let sub_result =
                    execute_select_internal(None, rewritten_subquery.clone(), &scoped_db);

                match sub_result {
                    Ok(result) => {
                        if sub_columns.is_none() {
                            sub_columns = Some(
                                result
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
                                    .collect(),
                            );
                        }
                        let lateral_eval_columns =
                            qualified_subquery_columns(alias, sub_columns.as_ref().unwrap());
                        let mut join_eval_columns = outer_scope_columns.clone();
                        join_eval_columns.extend(lateral_eval_columns);

                        let mut has_match = false;
                        if result.rows.is_empty() {
                        } else {
                            for sub_row in &result.rows {
                                let combined = combine_rows(current_row, sub_row);
                                let include = if let Some(ref on_expr) = join.on {
                                    evaluate_expression(
                                        Some(db),
                                        on_expr,
                                        &join_eval_columns,
                                        &combined,
                                    )?
                                } else {
                                    true
                                };
                                if include {
                                    joined_rows.push(Cow::Owned(combined));
                                    has_match = true;
                                }
                            }
                        }
                        if matches!(join.join_type, JoinType::Left | JoinType::Full) && !has_match {
                            let null_count = sub_columns.as_ref().map_or(0, |c| c.len());
                            joined_rows.push(Cow::Owned(combine_row_with_right_nulls(
                                current_row,
                                null_count,
                            )));
                        }
                    }
                    Err(_) => {
                        if matches!(join.join_type, JoinType::Left | JoinType::Full) {
                            let null_count = sub_columns.as_ref().map_or(0, |c| c.len());
                            joined_rows.push(Cow::Owned(combine_row_with_right_nulls(
                                current_row,
                                null_count,
                            )));
                        }
                    }
                }
            }

            if let Some(sc) = sub_columns {
                table_names.push(alias.clone());
                table_column_counts.push(sc.len());
                all_columns.extend(sc);
            }
            current_rows = joined_rows;
            continue;
        }

        if let Some((ref subquery, ref alias)) = join.subquery {
            let sub_result = execute_select_internal(context, *subquery.clone(), db)?;
            let sub_columns: Vec<ColumnDefinition> = sub_result
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
            let sub_rows = sub_result.rows;
            let mut joined_rows: Vec<Cow<'_, [Value]>> = Vec::new();
            let mut temp_all_cols = all_columns.clone();
            temp_all_cols.extend(sub_columns.clone());

            for current_row in &current_rows {
                let current_row = current_row.as_ref();
                let mut has_match = false;
                for sub_row in &sub_rows {
                    let combined = combine_rows(current_row, sub_row);
                    if let Some(ref on_expr) = join.on {
                        if evaluate_expression(Some(db), on_expr, &temp_all_cols, &combined)? {
                            joined_rows.push(Cow::Owned(combined));
                            has_match = true;
                        }
                    } else {
                        joined_rows.push(Cow::Owned(combined));
                        has_match = true;
                    }
                }
                if matches!(join.join_type, JoinType::Left | JoinType::Full) && !has_match {
                    joined_rows.push(Cow::Owned(combine_row_with_right_nulls(
                        current_row,
                        sub_columns.len(),
                    )));
                }
            }

            table_names.push(alias.clone());
            table_column_counts.push(sub_columns.len());
            all_columns.extend(sub_columns);
            current_rows = joined_rows;
            continue;
        }

        let join_table = db
            .get_table(&join.table)
            .ok_or_else(|| RustqlError::TableNotFound(join.table.clone()))?;

        let join_table_name = join.table.clone();
        table_names.push(join_table_name.clone());
        table_column_counts.push(join_table.columns.len());

        let mut joined_rows: Vec<Cow<'_, [Value]>> = Vec::new();
        let mut matched_pairs = HashSet::new();

        let check_join_match = |current_row: &[Value], join_row: &[Value]| -> bool {
            if let Some(Expression::BinaryOp { left, op, right }) = &join.on
                && *op == BinaryOperator::Equal
                && let (Expression::Column(left_col), Expression::Column(right_col)) =
                    (left.as_ref(), right.as_ref())
            {
                let get_col_idx = |col_name: &str| -> Option<usize> {
                    if col_name.contains('.') {
                        let parts: Vec<&str> = col_name.split('.').collect();
                        if parts.len() == 2 {
                            let table_name = parts[0];
                            let col_name = parts[1];
                            if let Some(tbl_idx) = table_names.iter().position(|n| n == table_name)
                            {
                                let mut col_offset = 0;
                                for (idx, &col_count) in table_column_counts.iter().enumerate() {
                                    if idx == tbl_idx {
                                        let table = if idx == 0 {
                                            from_table
                                        } else {
                                            db.get_table(&joins[idx - 1].table).unwrap()
                                        };
                                        if let Some(col_idx) =
                                            table.columns.iter().position(|c| c.name == col_name)
                                        {
                                            return Some(col_offset + col_idx);
                                        }
                                    }
                                    col_offset += col_count;
                                }
                            }
                        }
                    }
                    None
                };

                let left_col_idx = get_col_idx(left_col);
                let right_col_idx = get_col_idx(right_col);

                let left_val = left_col_idx.and_then(|idx| {
                    if idx < current_row.len() {
                        current_row.get(idx)
                    } else {
                        join_row.get(idx - current_row.len())
                    }
                });
                let right_val = right_col_idx.and_then(|idx| {
                    if idx < current_row.len() {
                        current_row.get(idx)
                    } else {
                        join_row.get(idx - current_row.len())
                    }
                });

                if let (Some(lv), Some(rv)) = (left_val, right_val) {
                    return lv == rv;
                }
            }
            false
        };

        if let Some(ref using_cols) = join.using_columns {
            let common_columns: Vec<(usize, usize)> = using_cols
                .iter()
                .filter_map(|col_name| {
                    let left_idx = all_columns.iter().position(|c| c.name == *col_name);
                    let right_idx = join_table.columns.iter().position(|c| c.name == *col_name);
                    left_idx.zip(right_idx)
                })
                .collect();

            let left_row_len = current_rows.first().map_or(0, |row| row.len());
            for (curr_idx, current_row) in current_rows.iter().enumerate() {
                let current_row = current_row.as_ref();
                let mut has_match = false;
                for (ji, join_row) in join_table.rows.iter().enumerate() {
                    let matches = common_columns.iter().all(|(left_idx, right_idx)| {
                        current_row
                            .get(*left_idx)
                            .zip(join_row.get(*right_idx))
                            .map(|(l, r)| l == r)
                            .unwrap_or(false)
                    });
                    if matches {
                        joined_rows.push(Cow::Owned(combine_rows(current_row, join_row)));
                        has_match = true;
                        matched_pairs.insert((curr_idx, ji));
                    }
                }
                if matches!(join.join_type, JoinType::Left | JoinType::Full) && !has_match {
                    joined_rows.push(Cow::Owned(combine_row_with_right_nulls(
                        current_row,
                        join_table.columns.len(),
                    )));
                }
            }

            if matches!(join.join_type, JoinType::Right | JoinType::Full) {
                for (ji, join_row) in join_table.rows.iter().enumerate() {
                    let has_match = current_rows
                        .iter()
                        .enumerate()
                        .any(|(curr_idx, _)| matched_pairs.contains(&(curr_idx, ji)));
                    if !has_match {
                        joined_rows.push(Cow::Owned(combine_row_with_left_nulls(
                            left_row_len,
                            join_row,
                        )));
                    }
                }
            }
        } else {
            match join.join_type {
                JoinType::Inner | JoinType::Left | JoinType::Full => {
                    let left_row_len = current_rows.first().map_or(0, |row| row.len());
                    for (curr_idx, current_row) in current_rows.iter().enumerate() {
                        let current_row = current_row.as_ref();
                        let mut has_match = false;
                        for (ji, join_row) in join_table.rows.iter().enumerate() {
                            if check_join_match(current_row, join_row) {
                                joined_rows.push(Cow::Owned(combine_rows(current_row, join_row)));
                                has_match = true;
                                matched_pairs.insert((curr_idx, ji));
                            }
                        }
                        if matches!(join.join_type, JoinType::Left | JoinType::Full) && !has_match {
                            joined_rows.push(Cow::Owned(combine_row_with_right_nulls(
                                current_row,
                                join_table.columns.len(),
                            )));
                        }
                    }
                    if matches!(join.join_type, JoinType::Right | JoinType::Full) {
                        for (ji, join_row) in join_table.rows.iter().enumerate() {
                            let mut has_match = false;
                            for (curr_idx, current_row) in current_rows.iter().enumerate() {
                                if check_join_match(current_row.as_ref(), join_row) {
                                    has_match = true;
                                    if !matches!(join.join_type, JoinType::Full)
                                        || !matched_pairs.contains(&(curr_idx, ji))
                                    {
                                        joined_rows.push(Cow::Owned(combine_rows(
                                            current_row.as_ref(),
                                            join_row,
                                        )));
                                    }
                                }
                            }
                            if !has_match {
                                joined_rows.push(Cow::Owned(combine_row_with_left_nulls(
                                    left_row_len,
                                    join_row,
                                )));
                            }
                        }
                    }
                }
                JoinType::Cross => {
                    for current_row in current_rows.iter() {
                        let current_row = current_row.as_ref();
                        for join_row in join_table.rows.iter() {
                            joined_rows.push(Cow::Owned(combine_rows(current_row, join_row)));
                        }
                    }
                }
                JoinType::Natural => {
                    let common_columns: Vec<(usize, usize)> = all_columns
                        .iter()
                        .enumerate()
                        .filter_map(|(left_idx, left_col)| {
                            join_table
                                .columns
                                .iter()
                                .position(|right_col| right_col.name == left_col.name)
                                .map(|right_idx| (left_idx, right_idx))
                        })
                        .collect();

                    for (curr_idx, current_row) in current_rows.iter().enumerate() {
                        let current_row = current_row.as_ref();
                        let mut has_match = false;
                        for (ji, join_row) in join_table.rows.iter().enumerate() {
                            let matches = common_columns.iter().all(|(left_idx, right_idx)| {
                                current_row
                                    .get(*left_idx)
                                    .zip(join_row.get(*right_idx))
                                    .map(|(l, r)| l == r)
                                    .unwrap_or(false)
                            });
                            if matches {
                                joined_rows.push(Cow::Owned(combine_rows(current_row, join_row)));
                                has_match = true;
                                matched_pairs.insert((curr_idx, ji));
                            }
                        }
                        if !has_match {
                            joined_rows.push(Cow::Owned(combine_row_with_right_nulls(
                                current_row,
                                join_table.columns.len(),
                            )));
                        }
                    }
                }
                _ => {}
            }
        }

        all_columns.extend(join_table.columns.clone());
        current_rows = joined_rows;
    }

    Ok((
        current_rows.into_iter().map(Cow::into_owned).collect(),
        all_columns,
    ))
}

fn lateral_outer_scope_columns(
    table_names: &[String],
    table_column_counts: &[usize],
    all_columns: &[ColumnDefinition],
) -> Vec<ColumnDefinition> {
    let mut qualified = Vec::with_capacity(all_columns.len());
    let mut offset = 0usize;

    for (table_name, column_count) in table_names.iter().zip(table_column_counts.iter()) {
        for column in all_columns.iter().skip(offset).take(*column_count) {
            let mut scoped = column.clone();
            if !scoped.name.contains('.') {
                scoped.name = format!("{}.{}", table_name, scoped.name);
            }
            qualified.push(scoped);
        }
        offset += column_count;
    }

    qualified
}

fn combine_rows(left: &[Value], right: &[Value]) -> Vec<Value> {
    let mut combined = Vec::with_capacity(left.len() + right.len());
    combined.extend_from_slice(left);
    combined.extend_from_slice(right);
    combined
}

fn combine_row_with_right_nulls(left: &[Value], right_len: usize) -> Vec<Value> {
    let mut combined = Vec::with_capacity(left.len() + right_len);
    combined.extend_from_slice(left);
    combined.resize(left.len() + right_len, Value::Null);
    combined
}

fn combine_row_with_left_nulls(left_len: usize, right: &[Value]) -> Vec<Value> {
    let mut combined = Vec::with_capacity(left_len + right.len());
    combined.resize(left_len, Value::Null);
    combined.extend_from_slice(right);
    combined
}

fn lateral_subquery_with_outer_scope(
    subquery: &SelectStatement,
    outer_table_name: &str,
) -> SelectStatement {
    let mut rewritten = subquery.clone();
    if rewritten.from.is_empty()
        && rewritten.from_subquery.is_none()
        && rewritten.from_function.is_none()
        && rewritten.from_values.is_none()
    {
        rewritten.from = outer_table_name.to_string();
    } else {
        rewritten.joins.push(Join {
            join_type: JoinType::Cross,
            table: outer_table_name.to_string(),
            table_alias: None,
            on: None,
            using_columns: None,
            lateral: true,
            subquery: None,
        });
    }
    rewritten
}

fn qualified_subquery_columns(alias: &str, columns: &[ColumnDefinition]) -> Vec<ColumnDefinition> {
    columns
        .iter()
        .map(|column| {
            let mut qualified = column.clone();
            if !qualified.name.contains('.') {
                qualified.name = format!("{}.{}", alias, qualified.name);
            }
            qualified
        })
        .collect()
}
