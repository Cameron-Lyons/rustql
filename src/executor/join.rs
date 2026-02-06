use crate::ast::*;
use crate::database::{Database, Table};
use crate::error::RustqlError;
use std::collections::HashSet;

use super::expr::evaluate_expression;
use super::select::execute_select_internal;

pub fn perform_multiple_joins(
    db: &Database,
    from_table: &Table,
    from_table_name: &str,
    joins: &[Join],
) -> Result<(Vec<Vec<Value>>, Vec<ColumnDefinition>), RustqlError> {
    let mut current_rows: Vec<Vec<Value>> = from_table.rows.clone();
    let mut all_columns = from_table.columns.clone();
    let mut table_names = vec![from_table_name.to_string()];
    let mut table_column_counts = vec![from_table.columns.len()];

    for join in joins {
        if join.lateral {
            if let Some((ref subquery, ref alias)) = join.subquery {
                let mut joined_rows: Vec<Vec<Value>> = Vec::new();
                let mut sub_columns: Option<Vec<ColumnDefinition>> = None;

                for current_row in &current_rows {
                    let temp_table_name = format!("__lateral_outer_{}", alias);
                    {
                        let mut db_write = super::get_database_write();
                        db_write.tables.insert(
                            temp_table_name.clone(),
                            Table {
                                columns: all_columns.clone(),
                                rows: vec![current_row.clone()],
                                constraints: vec![],
                            },
                        );
                    }

                    let sub_result = {
                        let db_read = super::get_database_read();
                        execute_select_internal(*subquery.clone(), &db_read)
                    };

                    {
                        let mut db_write = super::get_database_write();
                        db_write.tables.remove(&temp_table_name);
                    }

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
                            if result.rows.is_empty() {
                                if matches!(join.join_type, JoinType::Left | JoinType::Full) {
                                    let null_count = sub_columns.as_ref().map_or(0, |c| c.len());
                                    let mut combined = current_row.clone();
                                    combined.extend(vec![Value::Null; null_count]);
                                    joined_rows.push(combined);
                                }
                            } else {
                                for sub_row in &result.rows {
                                    let mut combined = current_row.clone();
                                    combined.extend(sub_row.clone());
                                    joined_rows.push(combined);
                                }
                            }
                        }
                        Err(_) => {
                            if matches!(join.join_type, JoinType::Left | JoinType::Full) {
                                let null_count = sub_columns.as_ref().map_or(0, |c| c.len());
                                let mut combined = current_row.clone();
                                combined.extend(vec![Value::Null; null_count]);
                                joined_rows.push(combined);
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
        }

        if let Some((ref subquery, ref alias)) = join.subquery {
            let sub_result = execute_select_internal(*subquery.clone(), db)?;
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

            let sub_table = Table {
                columns: sub_columns.clone(),
                rows: sub_result.rows,
                constraints: vec![],
            };

            let mut joined_rows: Vec<Vec<Value>> = Vec::new();
            let mut temp_all_cols = all_columns.clone();
            temp_all_cols.extend(sub_columns.clone());

            for current_row in &current_rows {
                let mut has_match = false;
                for sub_row in &sub_table.rows {
                    let mut combined = current_row.clone();
                    combined.extend(sub_row.clone());
                    if let Some(ref on_expr) = join.on {
                        if evaluate_expression(Some(db), on_expr, &temp_all_cols, &combined)? {
                            joined_rows.push(combined);
                            has_match = true;
                        }
                    } else {
                        joined_rows.push(combined);
                        has_match = true;
                    }
                }
                if matches!(join.join_type, JoinType::Left | JoinType::Full) && !has_match {
                    let mut combined = current_row.clone();
                    combined.extend(vec![Value::Null; sub_columns.len()]);
                    joined_rows.push(combined);
                }
            }

            table_names.push(alias.clone());
            table_column_counts.push(sub_columns.len());
            all_columns.extend(sub_columns);
            current_rows = joined_rows;
            continue;
        }

        let join_table = db
            .tables
            .get(&join.table)
            .ok_or_else(|| RustqlError::TableNotFound(join.table.clone()))?;

        let join_table_name = join.table.clone();
        table_names.push(join_table_name.clone());
        table_column_counts.push(join_table.columns.len());

        let mut joined_rows: Vec<Vec<Value>> = Vec::new();
        let mut matched_pairs = HashSet::new();

        let check_join_match = |current_row: &Vec<Value>, join_row: &Vec<Value>| -> bool {
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
                                            db.tables.get(&joins[idx - 1].table).unwrap()
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
                        current_row.get(idx).cloned()
                    } else {
                        join_row.get(idx - current_row.len()).cloned()
                    }
                });
                let right_val = right_col_idx.and_then(|idx| {
                    if idx < current_row.len() {
                        current_row.get(idx).cloned()
                    } else {
                        join_row.get(idx - current_row.len()).cloned()
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

            for (curr_idx, current_row) in current_rows.iter().enumerate() {
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
                        let mut combined = current_row.clone();
                        combined.extend(join_row.clone());
                        joined_rows.push(combined);
                        has_match = true;
                        matched_pairs.insert((curr_idx, ji));
                    }
                }
                if matches!(join.join_type, JoinType::Left | JoinType::Full) && !has_match {
                    let mut combined = current_row.clone();
                    combined.extend(vec![Value::Null; join_table.columns.len()]);
                    joined_rows.push(combined);
                }
            }

            if matches!(join.join_type, JoinType::Right | JoinType::Full) {
                for (ji, join_row) in join_table.rows.iter().enumerate() {
                    let has_match = current_rows
                        .iter()
                        .enumerate()
                        .any(|(curr_idx, _)| matched_pairs.contains(&(curr_idx, ji)));
                    if !has_match {
                        let mut combined =
                            vec![Value::Null; current_rows.first().map_or(0, |r| r.len())];
                        combined.extend(join_row.clone());
                        joined_rows.push(combined);
                    }
                }
            }
        } else {
            match join.join_type {
                JoinType::Inner | JoinType::Left | JoinType::Full => {
                    for (curr_idx, current_row) in current_rows.iter().enumerate() {
                        let mut has_match = false;
                        for (ji, join_row) in join_table.rows.iter().enumerate() {
                            if check_join_match(current_row, join_row) {
                                let mut combined = current_row.clone();
                                combined.extend(join_row.clone());
                                joined_rows.push(combined);
                                has_match = true;
                                matched_pairs.insert((curr_idx, ji));
                            }
                        }
                        if matches!(join.join_type, JoinType::Left | JoinType::Full) && !has_match {
                            let mut combined = current_row.clone();
                            combined.extend(vec![Value::Null; join_table.columns.len()]);
                            joined_rows.push(combined);
                        }
                    }
                }
                JoinType::Cross => {
                    for current_row in current_rows.iter() {
                        for join_row in join_table.rows.iter() {
                            let mut combined = current_row.clone();
                            combined.extend(join_row.clone());
                            joined_rows.push(combined);
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
                                let mut combined = current_row.clone();
                                combined.extend(join_row.clone());
                                joined_rows.push(combined);
                                has_match = true;
                                matched_pairs.insert((curr_idx, ji));
                            }
                        }
                        if !has_match {
                            let mut combined = current_row.clone();
                            combined.extend(vec![Value::Null; join_table.columns.len()]);
                            joined_rows.push(combined);
                        }
                    }
                }
                _ => {}
            }

            if matches!(join.join_type, JoinType::Right | JoinType::Full) {
                for (ji, join_row) in join_table.rows.iter().enumerate() {
                    let mut has_match = false;
                    for (curr_idx, current_row) in current_rows.iter().enumerate() {
                        if check_join_match(current_row, join_row) {
                            has_match = true;
                            if !matches!(join.join_type, JoinType::Full)
                                || !matched_pairs.contains(&(curr_idx, ji))
                            {
                                let mut combined = current_row.clone();
                                combined.extend(join_row.clone());
                                joined_rows.push(combined);
                            }
                        }
                    }
                    if !has_match {
                        let mut combined = vec![Value::Null; current_rows[0].len()];
                        combined.extend(join_row.clone());
                        joined_rows.push(combined);
                    }
                }
            }
        }

        all_columns.extend(join_table.columns.clone());
        current_rows = joined_rows;
    }

    Ok((current_rows, all_columns))
}
