use crate::ast::*;
use crate::database::{Database, Table};
use crate::error::RustqlError;
use std::collections::HashSet;

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

        all_columns.extend(join_table.columns.clone());
        current_rows = joined_rows;
    }

    Ok((current_rows, all_columns))
}
