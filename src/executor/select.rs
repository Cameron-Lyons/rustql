use crate::ast::*;
use crate::database::{Database, Table};
use crate::error::RustqlError;
use std::cmp::Ordering;
use std::collections::HashSet;

use super::aggregate::{
    compute_aggregate, evaluate_having, evaluate_window_functions, execute_select_with_aggregates,
    execute_select_with_grouping,
};
use super::expr::{
    compare_values_for_sort, evaluate_expression, evaluate_select_order_expression,
    evaluate_value_expression, format_value, parse_value_from_string,
};
use super::join::perform_multiple_joins;
use super::{SelectResult, get_database_read, get_database_write};

pub fn execute_select(stmt: SelectStatement) -> Result<String, RustqlError> {
    if let Some((ref subquery, ref alias)) = stmt.from_subquery {
        return execute_select_from_subquery(stmt.clone(), subquery, alias);
    }

    if !stmt.ctes.is_empty() {
        return execute_select_with_ctes(stmt);
    }

    if let Some(ref tf) = stmt.from_function {
        if tf.name == "generate_series" {
            return execute_generate_series(stmt.clone(), tf);
        }
    }

    if stmt.from.is_empty() {
        let result = execute_select_without_from(stmt)?;
        return Ok(format_select_result(&result));
    }

    {
        let db = get_database_read();
        if !db.tables.contains_key(&stmt.from)
            && let Some(view) = db.views.get(&stmt.from)
        {
            let view_sql = view.query_sql.clone();
            let view_name = stmt.from.clone();
            drop(db);
            return execute_select_from_view(stmt, &view_name, &view_sql);
        }
    }

    let db = get_database_read();

    if let Some((ref set_op_type, ref other_stmt)) = stmt.set_op {
        let left_stmt = SelectStatement {
            ctes: Vec::new(),
            distinct: stmt.distinct,
            distinct_on: stmt.distinct_on.clone(),
            columns: stmt.columns.clone(),
            from: stmt.from.clone(),
            from_alias: stmt.from_alias.clone(),
            from_subquery: stmt.from_subquery.clone(),
            from_function: stmt.from_function.clone(),
            joins: stmt.joins.clone(),
            where_clause: stmt.where_clause.clone(),
            group_by: stmt.group_by.clone(),
            having: stmt.having.clone(),
            order_by: stmt.order_by.clone(),
            limit: stmt.limit,
            offset: stmt.offset,
            fetch: stmt.fetch.clone(),
            set_op: None,
        };
        return execute_set_operation(left_stmt, other_stmt.as_ref().clone(), set_op_type, &db);
    }

    if !stmt.joins.is_empty() {
        return execute_select_with_joins(stmt, &db);
    }

    let db_ref: &Database = &db;
    let table = db_ref
        .tables
        .get(&stmt.from)
        .ok_or_else(|| RustqlError::TableNotFound(stmt.from.clone()))?;

    let mut filtered_rows: Vec<&Vec<Value>> = Vec::new();

    let candidate_indices: Option<HashSet<usize>> = if let Some(ref where_expr) = stmt.where_clause
    {
        if let Some(index_usage) = super::ddl::find_index_usage(db_ref, &stmt.from, where_expr) {
            super::ddl::get_indexed_rows(db_ref, table, &index_usage).ok()
        } else {
            None
        }
    } else {
        None
    };

    let rows_to_check: Vec<(usize, &Vec<Value>)> =
        if let Some(ref candidate_set) = candidate_indices {
            table
                .rows
                .iter()
                .enumerate()
                .filter(|(idx, _)| candidate_set.contains(idx))
                .collect()
        } else {
            table.rows.iter().enumerate().collect()
        };

    for (_, row) in rows_to_check {
        let include_row = if let Some(ref where_expr) = stmt.where_clause {
            evaluate_expression(Some(db_ref), where_expr, &table.columns, row)?
        } else {
            true
        };
        if include_row {
            filtered_rows.push(row);
        }
    }

    if stmt.group_by.is_some() {
        return execute_select_with_grouping(stmt, table, filtered_rows);
    }

    let has_aggregate = stmt
        .columns
        .iter()
        .any(|col| matches!(col, Column::Function(_)));

    if has_aggregate {
        return execute_select_with_aggregates(stmt, table, filtered_rows);
    }

    let column_specs: Vec<(String, Column)> = if matches!(stmt.columns[0], Column::All) {
        table
            .columns
            .iter()
            .map(|c| {
                (
                    c.name.clone(),
                    Column::Named {
                        name: c.name.clone(),
                        alias: None,
                    },
                )
            })
            .collect()
    } else {
        stmt.columns
            .iter()
            .map(|col| match col {
                Column::Named { name, alias } => {
                    (alias.clone().unwrap_or_else(|| name.clone()), col.clone())
                }
                Column::Subquery(_) => ("<subquery>".to_string(), col.clone()),
                Column::Function(_) => ("<aggregate>".to_string(), col.clone()),
                Column::Expression { alias, .. } => (
                    alias.clone().unwrap_or_else(|| "<expression>".to_string()),
                    col.clone(),
                ),
                Column::All => unreachable!(),
            })
            .collect()
    };

    let mut result = String::new();
    for (name, _) in &column_specs {
        result.push_str(&format!("{}\t", name));
    }
    result.push('\n');
    result.push_str(&"-".repeat(40));
    result.push('\n');

    let has_window_fn_main = column_specs.iter().any(|(_, col)| {
        matches!(
            col,
            Column::Expression {
                expr: Expression::WindowFunction { .. },
                ..
            }
        )
    });

    let window_rows: Option<Vec<Vec<Value>>> = if has_window_fn_main {
        let mut raw: Vec<Vec<Value>> = filtered_rows.iter().map(|r| (*r).clone()).collect();
        evaluate_window_functions(&mut raw, &table.columns, &stmt.columns)?;
        Some(raw)
    } else {
        None
    };

    let mut outputs: Vec<(Vec<Value>, Vec<Value>)> = Vec::with_capacity(filtered_rows.len());
    for (row_idx, row_ref) in filtered_rows.iter().enumerate() {
        let row = *row_ref;
        let mut projected: Vec<Value> = Vec::with_capacity(column_specs.len());
        let mut wf_offset = table.columns.len();
        for (_, col) in &column_specs {
            let val = match col {
                Column::All => {
                    unreachable!("Column::All should not appear in column_specs")
                }
                Column::Named { name, .. } => {
                    let column_name = if name.contains('.') {
                        name.split('.').next_back().unwrap_or(name)
                    } else {
                        name.as_str()
                    };
                    let idx = table
                        .columns
                        .iter()
                        .position(|c| c.name == column_name)
                        .ok_or_else(|| RustqlError::ColumnNotFound(name.clone()))?;
                    row[idx].clone()
                }
                Column::Subquery(subquery) => {
                    eval_scalar_subquery_with_outer(db_ref, subquery, &table.columns, row)?
                }
                Column::Expression {
                    expr: Expression::WindowFunction { .. },
                    ..
                } => {
                    if let Some(ref wr) = window_rows {
                        let v = wr[row_idx].get(wf_offset).cloned().unwrap_or(Value::Null);
                        wf_offset += 1;
                        v
                    } else {
                        Value::Null
                    }
                }
                Column::Expression { expr, .. } => {
                    evaluate_value_expression(expr, &table.columns, row)?
                }
                Column::Function(_) => {
                    return Err(RustqlError::Internal(
                        "Aggregate functions must be used with GROUP BY or without other columns"
                            .to_string(),
                    ));
                }
            };
            projected.push(val);
        }

        let mut order_values: Vec<Value> = Vec::new();
        if let Some(ref order_by) = stmt.order_by {
            for order_expr in order_by {
                let value = evaluate_select_order_expression(
                    &order_expr.expr,
                    &table.columns,
                    row,
                    &column_specs,
                    &projected,
                    true,
                )?;
                order_values.push(value);
            }
        }

        outputs.push((projected, order_values));
    }

    if let Some(ref order_by) = stmt.order_by {
        outputs.sort_by(|a, b| {
            for (idx, order_expr) in order_by.iter().enumerate() {
                let cmp = compare_values_for_sort(&a.1[idx], &b.1[idx]);
                if cmp != Ordering::Equal {
                    return if order_expr.asc { cmp } else { cmp.reverse() };
                }
            }
            Ordering::Equal
        });
    }

    if let Some(ref distinct_on_exprs) = stmt.distinct_on {
        let mut seen_keys: Vec<Vec<Value>> = Vec::new();
        let mut deduped: Vec<(Vec<Value>, Vec<Value>)> = Vec::new();
        for (projected, order_vals) in outputs {
            let key: Vec<Value> = distinct_on_exprs
                .iter()
                .map(|expr| {
                    if let Expression::Column(name) = expr {
                        for (i, (header, _)) in column_specs.iter().enumerate() {
                            if header == name {
                                return projected[i].clone();
                            }
                        }
                    }
                    evaluate_value_expression(expr, &table.columns, &projected)
                        .unwrap_or(Value::Null)
                })
                .collect();
            if !seen_keys.iter().any(|k| k == &key) {
                seen_keys.push(key);
                deduped.push((projected, order_vals));
            }
        }
        outputs = deduped;
    }

    let offset = stmt.offset.unwrap_or(0);
    let base_limit = stmt.limit.unwrap_or(outputs.len());

    let limit = if let Some(ref fetch) = stmt.fetch {
        if fetch.with_ties && stmt.order_by.is_some() {
            let target = offset + fetch.count;
            if target < outputs.len() {
                let last_included = &outputs[target - 1].1;
                let mut extended = target;
                while extended < outputs.len() && outputs[extended].1 == *last_included {
                    extended += 1;
                }
                extended - offset
            } else {
                fetch.count
            }
        } else {
            fetch.count
        }
    } else {
        base_limit
    };

    use std::collections::BTreeSet;
    let mut seen: BTreeSet<Vec<Value>> = BTreeSet::new();
    let mut emitted = 0usize;
    let mut skipped = 0usize;
    for (projected, _) in outputs {
        if stmt.distinct && !seen.insert(projected.clone()) {
            continue;
        }
        if skipped < offset {
            skipped += 1;
            continue;
        }
        if emitted >= limit {
            break;
        }
        for val in &projected {
            result.push_str(&format!("{}\t", format_value(val)));
        }
        result.push('\n');
        emitted += 1;
    }

    Ok(result)
}

fn execute_set_operation(
    left_stmt: SelectStatement,
    right_stmt: SelectStatement,
    set_op_type: &SetOperation,
    db: &Database,
) -> Result<String, RustqlError> {
    let mut left_stmt_internal = left_stmt;
    left_stmt_internal.set_op = None;
    let mut right_stmt_internal = right_stmt;
    right_stmt_internal.set_op = None;

    let left_result = execute_select_internal(left_stmt_internal, db)?;
    let right_result = execute_select_internal(right_stmt_internal, db)?;

    let mut combined: Vec<Vec<Value>> = Vec::new();

    match set_op_type {
        SetOperation::UnionAll => {
            combined.extend(left_result.rows);
            combined.extend(right_result.rows);
        }
        SetOperation::Union => {
            use std::collections::BTreeSet;
            let mut seen: BTreeSet<Vec<Value>> = BTreeSet::new();
            for row in left_result.rows {
                if seen.insert(row.clone()) {
                    combined.push(row);
                }
            }
            for row in right_result.rows {
                if seen.insert(row.clone()) {
                    combined.push(row);
                }
            }
        }
        SetOperation::Intersect => {
            use std::collections::BTreeSet;
            let right_set: BTreeSet<Vec<Value>> = right_result.rows.into_iter().collect();
            let mut seen: BTreeSet<Vec<Value>> = BTreeSet::new();
            for row in left_result.rows {
                if right_set.contains(&row) && seen.insert(row.clone()) {
                    combined.push(row);
                }
            }
        }
        SetOperation::IntersectAll => {
            use std::collections::BTreeMap;
            let mut right_counts: BTreeMap<Vec<Value>, usize> = BTreeMap::new();
            for row in &right_result.rows {
                *right_counts.entry(row.clone()).or_insert(0) += 1;
            }
            let mut left_counts: BTreeMap<Vec<Value>, usize> = BTreeMap::new();
            for row in &left_result.rows {
                *left_counts.entry(row.clone()).or_insert(0) += 1;
            }
            for (row, left_count) in &left_counts {
                let right_count = right_counts.get(row).copied().unwrap_or(0);
                let emit_count = (*left_count).min(right_count);
                for _ in 0..emit_count {
                    combined.push(row.clone());
                }
            }
        }
        SetOperation::Except => {
            use std::collections::BTreeSet;
            let right_set: BTreeSet<Vec<Value>> = right_result.rows.into_iter().collect();
            let mut seen: BTreeSet<Vec<Value>> = BTreeSet::new();
            for row in left_result.rows {
                if !right_set.contains(&row) && seen.insert(row.clone()) {
                    combined.push(row);
                }
            }
        }
        SetOperation::ExceptAll => {
            use std::collections::BTreeMap;
            let mut right_counts: BTreeMap<Vec<Value>, usize> = BTreeMap::new();
            for row in &right_result.rows {
                *right_counts.entry(row.clone()).or_insert(0) += 1;
            }
            let mut left_counts: BTreeMap<Vec<Value>, usize> = BTreeMap::new();
            let mut left_order: Vec<Vec<Value>> = Vec::new();
            for row in &left_result.rows {
                let entry = left_counts.entry(row.clone()).or_insert(0);
                if *entry == 0 {
                    left_order.push(row.clone());
                }
                *entry += 1;
            }
            for row in &left_order {
                let left_count = left_counts.get(row).copied().unwrap_or(0);
                let right_count = right_counts.get(row).copied().unwrap_or(0);
                let emit_count = left_count.saturating_sub(right_count);
                for _ in 0..emit_count {
                    combined.push(row.clone());
                }
            }
        }
    }

    let mut result = String::new();
    for (idx, header) in left_result.headers.iter().enumerate() {
        if idx > 0 {
            result.push('\t');
        }
        result.push_str(header);
    }
    result.push('\n');
    result.push_str(&"-".repeat(40));
    result.push('\n');

    let offset = 0;
    let limit = combined.len();

    let mut emitted = 0usize;
    let mut skipped = 0usize;
    for row in combined {
        if skipped < offset {
            skipped += 1;
            continue;
        }
        if emitted >= limit {
            break;
        }
        for (idx, val) in row.iter().enumerate() {
            if idx > 0 {
                result.push('\t');
            }
            result.push_str(&format_value(val));
        }
        result.push('\n');
        emitted += 1;
    }

    Ok(result)
}

fn format_select_result(result: &SelectResult) -> String {
    let mut output = String::new();
    for (idx, header) in result.headers.iter().enumerate() {
        if idx > 0 {
            output.push('\t');
        }
        output.push_str(header);
    }
    output.push('\n');
    output.push_str(&"-".repeat(40));
    output.push('\n');

    for row in &result.rows {
        for (idx, val) in row.iter().enumerate() {
            if idx > 0 {
                output.push('\t');
            }
            output.push_str(&format_value(val));
        }
        output.push('\n');
    }
    output
}

fn execute_select_without_from(stmt: SelectStatement) -> Result<SelectResult, RustqlError> {
    let empty_columns: Vec<ColumnDefinition> = Vec::new();
    let empty_row: Vec<Value> = Vec::new();

    let mut headers = Vec::new();
    let mut result_row = Vec::new();

    for col in &stmt.columns {
        match col {
            Column::Named { name, alias } => {
                let col_name = alias.clone().unwrap_or_else(|| name.clone());
                headers.push(col_name);
                result_row.push(Value::Null);
            }
            Column::Expression { expr, alias } => {
                let val = evaluate_value_expression(expr, &empty_columns, &empty_row)?;
                let col_name = alias
                    .clone()
                    .unwrap_or_else(|| format!("column{}", headers.len()));
                headers.push(col_name);
                result_row.push(val);
            }
            Column::Function(agg) => {
                let col_name = agg
                    .alias
                    .clone()
                    .unwrap_or_else(|| format!("{:?}", agg.function));
                headers.push(col_name);
                result_row.push(Value::Null);
            }
            Column::Subquery(_) => {}
            Column::All => {}
        }
    }

    Ok(SelectResult {
        headers,
        rows: vec![result_row],
    })
}

fn execute_generate_series(
    stmt: SelectStatement,
    tf: &TableFunction,
) -> Result<String, RustqlError> {
    let empty_columns: Vec<ColumnDefinition> = Vec::new();
    let empty_row: Vec<Value> = Vec::new();

    let start = match evaluate_value_expression(&tf.args[0], &empty_columns, &empty_row)? {
        Value::Integer(n) => n,
        _ => {
            return Err(RustqlError::TypeMismatch(
                "GENERATE_SERIES arguments must be integers".to_string(),
            ));
        }
    };
    let stop = match evaluate_value_expression(&tf.args[1], &empty_columns, &empty_row)? {
        Value::Integer(n) => n,
        _ => {
            return Err(RustqlError::TypeMismatch(
                "GENERATE_SERIES arguments must be integers".to_string(),
            ));
        }
    };
    let step = if tf.args.len() > 2 {
        match evaluate_value_expression(&tf.args[2], &empty_columns, &empty_row)? {
            Value::Integer(n) => n,
            _ => {
                return Err(RustqlError::TypeMismatch(
                    "GENERATE_SERIES step must be an integer".to_string(),
                ));
            }
        }
    } else if start <= stop {
        1
    } else {
        -1
    };

    if step == 0 {
        return Err(RustqlError::Internal(
            "GENERATE_SERIES step cannot be zero".to_string(),
        ));
    }

    let col_name = tf
        .alias
        .clone()
        .unwrap_or_else(|| "generate_series".to_string());

    let series_col = ColumnDefinition {
        name: col_name.clone(),
        data_type: DataType::Integer,
        nullable: false,
        primary_key: false,
        unique: false,
        default_value: None,
        foreign_key: None,
        check: None,
        auto_increment: false,
    };

    let mut series_rows: Vec<Vec<Value>> = Vec::new();
    let mut current = start;
    if step > 0 {
        while current <= stop {
            series_rows.push(vec![Value::Integer(current)]);
            current += step;
        }
    } else {
        while current >= stop {
            series_rows.push(vec![Value::Integer(current)]);
            current += step;
        }
    }

    let all_columns = vec![series_col];

    let mut filtered_rows: Vec<&Vec<Value>> = Vec::new();
    for row in &series_rows {
        let include = if let Some(ref where_expr) = stmt.where_clause {
            evaluate_expression(None, where_expr, &all_columns, row)?
        } else {
            true
        };
        if include {
            filtered_rows.push(row);
        }
    }

    let headers: Vec<String> = stmt
        .columns
        .iter()
        .map(|col| match col {
            Column::All => col_name.clone(),
            Column::Named { name, alias } => alias.clone().unwrap_or_else(|| name.clone()),
            Column::Expression { alias, .. } => {
                alias.clone().unwrap_or_else(|| "<expr>".to_string())
            }
            _ => "<col>".to_string(),
        })
        .collect();

    let mut result = String::new();
    for h in &headers {
        result.push_str(&format!("{}\t", h));
    }
    result.push('\n');
    result.push_str(&"-".repeat(40));
    result.push('\n');

    let offset = stmt.offset.unwrap_or(0);
    let limit = stmt.limit.unwrap_or(filtered_rows.len());

    for row in filtered_rows.iter().skip(offset).take(limit) {
        for col in &stmt.columns {
            let val = match col {
                Column::All => row[0].clone(),
                Column::Named { name, .. } => {
                    let idx = all_columns
                        .iter()
                        .position(|c| c.name == *name)
                        .unwrap_or(0);
                    row.get(idx).cloned().unwrap_or(Value::Null)
                }
                Column::Expression { expr, .. } => {
                    evaluate_value_expression(expr, &all_columns, row)?
                }
                _ => Value::Null,
            };
            result.push_str(&format!("{}\t", format_value(&val)));
        }
        result.push('\n');
    }

    Ok(result)
}

pub(crate) fn execute_select_internal(
    stmt: SelectStatement,
    db: &Database,
) -> Result<SelectResult, RustqlError> {
    if stmt.from.is_empty() {
        return execute_select_without_from(stmt);
    }

    let (all_rows, all_columns) = if !stmt.joins.is_empty() {
        let main_table = db
            .tables
            .get(&stmt.from)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.from.clone()))?;
        let (joined_rows, all_cols) =
            perform_multiple_joins(db, main_table, &stmt.from, &stmt.joins)?;
        (joined_rows, all_cols)
    } else {
        let table = db
            .tables
            .get(&stmt.from)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.from.clone()))?;
        (table.rows.clone(), table.columns.clone())
    };

    let mut filtered_rows: Vec<&Vec<Value>> = Vec::new();

    if stmt.joins.is_empty() {
        let table = db
            .tables
            .get(&stmt.from)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.from.clone()))?;

        let candidate_indices: Option<HashSet<usize>> = if let Some(ref where_expr) =
            stmt.where_clause
        {
            if let Some(index_usage) = super::ddl::find_index_usage(db, &stmt.from, where_expr) {
                super::ddl::get_indexed_rows(db, table, &index_usage).ok()
            } else {
                None
            }
        } else {
            None
        };

        let rows_to_check: Vec<(usize, &Vec<Value>)> =
            if let Some(ref candidate_set) = candidate_indices {
                table
                    .rows
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| candidate_set.contains(idx))
                    .collect()
            } else {
                table.rows.iter().enumerate().collect()
            };

        for (_, row) in rows_to_check {
            let include_row = if let Some(ref where_expr) = stmt.where_clause {
                evaluate_expression(Some(db), where_expr, &all_columns, row)?
            } else {
                true
            };
            if include_row {
                filtered_rows.push(row);
            }
        }
    } else {
        for row in &all_rows {
            let include_row = if let Some(ref where_expr) = stmt.where_clause {
                evaluate_expression(Some(db), where_expr, &all_columns, row)?
            } else {
                true
            };
            if include_row {
                filtered_rows.push(row);
            }
        }
    }

    if stmt.group_by.is_some() {
        let temp_table = Table {
            columns: all_columns.clone(),
            rows: Vec::new(),
            constraints: vec![],
        };
        let row_refs: Vec<&Vec<Value>> = filtered_rows.to_vec();
        let grouping_result = execute_select_with_grouping(stmt.clone(), &temp_table, row_refs)?;
        let lines: Vec<&str> = grouping_result.lines().collect();
        if lines.len() < 2 {
            return Err(RustqlError::Internal("Invalid grouping result".to_string()));
        }
        let headers: Vec<String> = lines[0]
            .split('\t')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for line in lines.iter().skip(2) {
            if line.trim().is_empty() {
                continue;
            }
            let values: Vec<Value> = line
                .split('\t')
                .filter(|s| !s.is_empty())
                .map(parse_value_from_string)
                .collect();
            rows.push(values);
        }
        return Ok(SelectResult { headers, rows });
    }

    let has_aggregate = stmt
        .columns
        .iter()
        .any(|col| matches!(col, Column::Function(_)));
    if has_aggregate {
        let temp_table = Table {
            columns: all_columns.clone(),
            rows: Vec::new(),
            constraints: vec![],
        };
        let row_refs: Vec<&Vec<Value>> = filtered_rows.to_vec();
        let agg_result = execute_select_with_aggregates(stmt.clone(), &temp_table, row_refs)?;
        let lines: Vec<&str> = agg_result.lines().collect();
        if lines.len() < 2 {
            return Err(RustqlError::Internal(
                "Invalid aggregate result".to_string(),
            ));
        }
        let headers: Vec<String> = lines[0]
            .split('\t')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        let mut rows: Vec<Vec<Value>> = Vec::new();
        if lines.len() > 2 {
            let values: Vec<Value> = lines[2]
                .split('\t')
                .filter(|s| !s.is_empty())
                .map(parse_value_from_string)
                .collect();
            rows.push(values);
        }
        return Ok(SelectResult { headers, rows });
    }

    let headers: Vec<String> = if matches!(stmt.columns[0], Column::All) {
        all_columns.iter().map(|c| c.name.clone()).collect()
    } else {
        stmt.columns
            .iter()
            .map(|col| match col {
                Column::Named { name, alias } => alias.clone().unwrap_or_else(|| name.clone()),
                Column::Function(agg) => agg
                    .alias
                    .clone()
                    .unwrap_or_else(|| format!("{:?}", agg.function)),
                Column::Subquery(_) => "<subquery>".to_string(),
                Column::Expression { alias, .. } => {
                    alias.clone().unwrap_or_else(|| "<expression>".to_string())
                }
                Column::All => unreachable!(),
            })
            .collect()
    };

    let has_window_fn = stmt.columns.iter().any(|col| {
        matches!(
            col,
            Column::Expression {
                expr: Expression::WindowFunction { .. },
                ..
            }
        )
    });

    if has_window_fn {
        let mut raw_rows: Vec<Vec<Value>> = filtered_rows.iter().map(|r| (*r).clone()).collect();
        evaluate_window_functions(&mut raw_rows, &all_columns, &stmt.columns)?;
        let base_len = all_columns.len();
        let mut rows: Vec<Vec<Value>> = Vec::new();
        for raw_row in &raw_rows {
            let mut projected: Vec<Value> = Vec::new();
            let mut wf_offset = base_len;
            for col in &stmt.columns {
                let val = match col {
                    Column::All => unreachable!(),
                    Column::Named { name, .. } => {
                        let column_name = if name.contains('.') {
                            name.split('.').next_back().unwrap_or(name)
                        } else {
                            name.as_str()
                        };
                        let idx = all_columns
                            .iter()
                            .position(|c| c.name == column_name)
                            .ok_or_else(|| RustqlError::ColumnNotFound(name.clone()))?;
                        raw_row[idx].clone()
                    }
                    Column::Expression {
                        expr: Expression::WindowFunction { .. },
                        ..
                    } => {
                        let v = raw_row.get(wf_offset).cloned().unwrap_or(Value::Null);
                        wf_offset += 1;
                        v
                    }
                    Column::Expression { expr, .. } => {
                        evaluate_value_expression(expr, &all_columns, raw_row)?
                    }
                    Column::Function(_) => Value::Null,
                    Column::Subquery(subquery) => {
                        eval_scalar_subquery_with_outer(db, subquery, &all_columns, raw_row)?
                    }
                };
                projected.push(val);
            }
            rows.push(projected);
        }
        return Ok(SelectResult { headers, rows });
    }

    let mut rows: Vec<Vec<Value>> = Vec::new();
    for row_ref in &filtered_rows {
        let row = *row_ref;
        let mut projected: Vec<Value> = Vec::new();

        if matches!(stmt.columns[0], Column::All) {
            projected = row.clone();
        } else {
            for col in &stmt.columns {
                let val = match col {
                    Column::All => unreachable!(),
                    Column::Named { name, .. } => {
                        let column_name = if name.contains('.') {
                            name.split('.').next_back().unwrap_or(name)
                        } else {
                            name.as_str()
                        };
                        let idx = all_columns
                            .iter()
                            .position(|c| c.name == column_name)
                            .ok_or_else(|| RustqlError::ColumnNotFound(name.clone()))?;
                        row[idx].clone()
                    }
                    Column::Function(_) => {
                        return Err(RustqlError::Internal(
                            "Aggregate functions in UNION must use GROUP BY".to_string(),
                        ));
                    }
                    Column::Subquery(subquery) => {
                        eval_scalar_subquery_with_outer(db, subquery, &all_columns, row)?
                    }
                    Column::Expression { expr, .. } => {
                        evaluate_value_expression(expr, &all_columns, row)?
                    }
                };
                projected.push(val);
            }
        }

        rows.push(projected);
    }

    Ok(SelectResult { headers, rows })
}

pub fn eval_subquery_values(
    db: &Database,
    subquery: &SelectStatement,
) -> Result<Vec<Value>, RustqlError> {
    if subquery.columns.len() != 1 {
        return Err(RustqlError::Internal(
            "Subquery in IN must select exactly one column".to_string(),
        ));
    }
    let table = db
        .tables
        .get(&subquery.from)
        .ok_or_else(|| RustqlError::TableNotFound(subquery.from.clone()))?;

    let mut filtered_rows: Vec<&Vec<Value>> = Vec::new();

    if subquery.joins.is_empty() {
        let candidate_indices: Option<HashSet<usize>> = if let Some(ref where_expr) =
            subquery.where_clause
        {
            if let Some(index_usage) = super::ddl::find_index_usage(db, &subquery.from, where_expr)
            {
                super::ddl::get_indexed_rows(db, table, &index_usage).ok()
            } else {
                None
            }
        } else {
            None
        };

        let rows_to_check: Vec<(usize, &Vec<Value>)> =
            if let Some(ref candidate_set) = candidate_indices {
                table
                    .rows
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| candidate_set.contains(idx))
                    .collect()
            } else {
                table.rows.iter().enumerate().collect()
            };

        for (_, row) in rows_to_check {
            let include_row = if let Some(ref where_expr) = subquery.where_clause {
                evaluate_expression(Some(db), where_expr, &table.columns, row)?
            } else {
                true
            };
            if include_row {
                filtered_rows.push(row);
            }
        }
    } else {
        for row in &table.rows {
            let include_row = if let Some(ref where_expr) = subquery.where_clause {
                evaluate_expression(Some(db), where_expr, &table.columns, row)?
            } else {
                true
            };
            if include_row {
                filtered_rows.push(row);
            }
        }
    }

    match &subquery.columns[0] {
        Column::All => Err(RustqlError::Internal(
            "Subquery in IN cannot use *".to_string(),
        )),
        Column::Expression { .. } => Err(RustqlError::Internal(
            "Subquery in IN cannot use expressions".to_string(),
        )),
        Column::Subquery(scalar_subquery) => {
            let mut values = Vec::with_capacity(filtered_rows.len());
            for row in &filtered_rows {
                let scalar_value =
                    eval_scalar_subquery_with_outer(db, scalar_subquery, &table.columns, row)?;
                values.push(scalar_value);
            }
            Ok(values)
        }
        Column::Function(agg) => {
            if let Some(group_by_clause) = &subquery.group_by {
                let gb_exprs = group_by_clause.exprs();
                let mut groups: std::collections::BTreeMap<Vec<Value>, Vec<&Vec<Value>>> =
                    std::collections::BTreeMap::new();
                for row in &filtered_rows {
                    let key: Vec<Value> = gb_exprs
                        .iter()
                        .map(|expr| {
                            evaluate_value_expression(expr, &table.columns, row)
                                .unwrap_or(Value::Null)
                        })
                        .collect();
                    groups.entry(key).or_default().push(*row);
                }
                let mut values = Vec::with_capacity(groups.len());
                for (_k, rows) in groups {
                    if let Some(ref having_expr) = subquery.having
                        && !evaluate_having(having_expr, &subquery.columns, table, &rows)?
                    {
                        continue;
                    }
                    let v =
                        compute_aggregate(&agg.function, &agg.expr, table, &rows, agg.distinct)?;
                    values.push(v);
                }
                Ok(values)
            } else {
                let value = compute_aggregate(
                    &agg.function,
                    &agg.expr,
                    table,
                    &filtered_rows,
                    agg.distinct,
                )?;
                Ok(vec![value])
            }
        }
        Column::Named { name, .. } => {
            if let Some(group_by_clause) = &subquery.group_by {
                let gb_exprs = group_by_clause.exprs();
                let mut groups: std::collections::BTreeMap<Vec<Value>, Vec<&Vec<Value>>> =
                    std::collections::BTreeMap::new();
                for row in &filtered_rows {
                    let key: Vec<Value> = gb_exprs
                        .iter()
                        .map(|expr| {
                            evaluate_value_expression(expr, &table.columns, row)
                                .unwrap_or(Value::Null)
                        })
                        .collect();
                    groups.entry(key).or_default().push(*row);
                }

                let named_idx = table
                    .columns
                    .iter()
                    .position(|c| &c.name == name)
                    .ok_or_else(|| RustqlError::ColumnNotFound(name.clone()))?;
                let is_in_group_by = gb_exprs.iter().any(|expr| {
                    matches!(expr, Expression::Column(col_name) if col_name == name
                        || col_name.split('.').next_back() == Some(name.as_str()))
                });
                if !is_in_group_by {
                    return Err(RustqlError::Internal(format!(
                        "Column '{}' must appear in GROUP BY clause",
                        name
                    )));
                }
                let mut values = Vec::with_capacity(groups.len());
                for (_k, rows) in groups {
                    values.push(rows[0][named_idx].clone());
                }
                Ok(values)
            } else {
                let idx = table
                    .columns
                    .iter()
                    .position(|c| &c.name == name)
                    .ok_or_else(|| RustqlError::ColumnNotFound(name.clone()))?;
                let mut values = Vec::with_capacity(filtered_rows.len());
                for row in filtered_rows {
                    values.push(row[idx].clone());
                }
                Ok(values)
            }
        }
    }
}

pub fn eval_subquery_exists_with_outer(
    db: &Database,
    subquery: &SelectStatement,
    outer_columns: &[ColumnDefinition],
    outer_row: &[Value],
) -> Result<bool, RustqlError> {
    if !subquery.joins.is_empty() {
        return eval_subquery_exists_with_joins(db, subquery, outer_columns, outer_row);
    }

    let table = db
        .tables
        .get(&subquery.from)
        .ok_or_else(|| RustqlError::TableNotFound(subquery.from.clone()))?;

    let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
    combined_columns.extend(table.columns.clone());

    for inner_row in &table.rows {
        let mut combined_row: Vec<Value> = outer_row.to_vec();
        combined_row.extend(inner_row.clone());

        let include_row = if let Some(ref where_expr) = subquery.where_clause {
            evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
        } else {
            true
        };
        if include_row {
            return Ok(true);
        }
    }
    Ok(false)
}

fn eval_subquery_exists_with_joins(
    db: &Database,
    subquery: &SelectStatement,
    outer_columns: &[ColumnDefinition],
    outer_row: &[Value],
) -> Result<bool, RustqlError> {
    let main_table = db
        .tables
        .get(&subquery.from)
        .ok_or_else(|| RustqlError::TableNotFound(subquery.from.clone()))?;

    let (joined_rows, all_subquery_columns) =
        perform_multiple_joins(db, main_table, &subquery.from, &subquery.joins)?;

    let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
    combined_columns.extend(all_subquery_columns.clone());

    for sub_row in joined_rows {
        let mut combined_row: Vec<Value> = outer_row.to_vec();
        combined_row.extend(sub_row);

        let include_row = if let Some(ref where_expr) = subquery.where_clause {
            evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
        } else {
            return Ok(true);
        };
        if include_row {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn eval_scalar_subquery_with_outer(
    db: &Database,
    subquery: &SelectStatement,
    outer_columns: &[ColumnDefinition],
    outer_row: &[Value],
) -> Result<Value, RustqlError> {
    if subquery.columns.len() != 1 {
        return Err(RustqlError::Internal(
            "Scalar subquery must select exactly one column".to_string(),
        ));
    }

    if !subquery.joins.is_empty() {
        let main_table = db
            .tables
            .get(&subquery.from)
            .ok_or_else(|| RustqlError::TableNotFound(subquery.from.clone()))?;

        let (joined_rows, all_subquery_columns) =
            perform_multiple_joins(db, main_table, &subquery.from, &subquery.joins)?;

        let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
        combined_columns.extend(all_subquery_columns.clone());
        let mut candidate_rows: Vec<Vec<Value>> = Vec::new();
        for sub_row in joined_rows {
            let mut combined_row: Vec<Value> = outer_row.to_vec();
            combined_row.extend(sub_row);
            let include_row = if let Some(ref where_expr) = subquery.where_clause {
                evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
            } else {
                true
            };
            if include_row {
                candidate_rows.push(combined_row);
            }
        }

        let apply_order_and_slice = |rows: &mut Vec<Vec<Value>>| -> Result<(), RustqlError> {
            if let Some(order_by) = &subquery.order_by {
                rows.sort_by(|a, b| {
                    for ob in order_by {
                        let va = evaluate_value_expression(&ob.expr, &combined_columns, a)
                            .unwrap_or(Value::Null);
                        let vb = evaluate_value_expression(&ob.expr, &combined_columns, b)
                            .unwrap_or(Value::Null);
                        let ord = compare_values_for_sort(&va, &vb);
                        if ord != std::cmp::Ordering::Equal {
                            return if ob.asc { ord } else { ord.reverse() };
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
            let start = subquery.offset.unwrap_or(0);
            let end = if let Some(limit) = subquery.limit {
                start.saturating_add(limit)
            } else {
                rows.len()
            };
            let end = end.min(rows.len());
            if start >= rows.len() {
                rows.clear();
            } else {
                rows.drain(0..start);
                rows.truncate(end - start);
            }
            Ok(())
        };
        apply_order_and_slice(&mut candidate_rows)?;

        return match &subquery.columns[0] {
            Column::All => Err(RustqlError::Internal(
                "Scalar subquery cannot use *".to_string(),
            )),
            Column::Expression { .. } => Err(RustqlError::Internal(
                "Scalar subquery cannot use expressions".to_string(),
            )),
            Column::Named { name, .. } => {
                let col_idx = combined_columns
                    .iter()
                    .position(|c| {
                        c.name
                            == (if name.contains('.') {
                                name.split('.').next_back().unwrap_or(name)
                            } else {
                                name
                            })
                    })
                    .ok_or_else(|| RustqlError::ColumnNotFound(name.clone()))?;
                if candidate_rows.is_empty() {
                    Ok(Value::Null)
                } else if candidate_rows.len() == 1 {
                    Ok(candidate_rows[0][col_idx].clone())
                } else {
                    Err(RustqlError::Internal(
                        "Scalar subquery returned more than one row".to_string(),
                    ))
                }
            }
            Column::Subquery(nested) => {
                let mut results = Vec::new();
                for combined_row in candidate_rows {
                    let v = eval_scalar_subquery_with_outer(
                        db,
                        nested,
                        &combined_columns,
                        &combined_row,
                    )?;
                    results.push(v);
                }
                if results.is_empty() {
                    Ok(Value::Null)
                } else if results.len() == 1 {
                    Ok(results[0].clone())
                } else {
                    Err(RustqlError::Internal(
                        "Scalar subquery returned more than one row".to_string(),
                    ))
                }
            }
            Column::Function(agg) => {
                let outer_col_count = outer_columns.len();

                let mut subquery_rows: Vec<Vec<Value>> = Vec::new();
                for combined_row in &candidate_rows {
                    subquery_rows.push(combined_row[outer_col_count..].to_vec());
                }

                let filtered_rows: Vec<&Vec<Value>> = subquery_rows.iter().collect();

                let temp_table = Table {
                    columns: all_subquery_columns.clone(),
                    rows: subquery_rows.clone(),
                    constraints: vec![],
                };

                compute_aggregate(
                    &agg.function,
                    &agg.expr,
                    &temp_table,
                    &filtered_rows,
                    agg.distinct,
                )
            }
        };
    }

    let table = db
        .tables
        .get(&subquery.from)
        .ok_or_else(|| RustqlError::TableNotFound(subquery.from.clone()))?;

    let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
    combined_columns.extend(table.columns.clone());

    let apply_order_and_slice = |rows: &mut Vec<Vec<Value>>| -> Result<(), RustqlError> {
        if let Some(order_by) = &subquery.order_by {
            rows.sort_by(|a, b| {
                for ob in order_by {
                    let va = evaluate_value_expression(&ob.expr, &combined_columns, a)
                        .unwrap_or(Value::Null);
                    let vb = evaluate_value_expression(&ob.expr, &combined_columns, b)
                        .unwrap_or(Value::Null);
                    let ord = compare_values_for_sort(&va, &vb);
                    if ord != std::cmp::Ordering::Equal {
                        return if ob.asc { ord } else { ord.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        let start = subquery.offset.unwrap_or(0);
        let end = if let Some(limit) = subquery.limit {
            start.saturating_add(limit)
        } else {
            rows.len()
        };
        let end = end.min(rows.len());
        if start >= rows.len() {
            rows.clear();
        } else {
            rows.drain(0..start);
            rows.truncate(end - start);
        }
        Ok(())
    };

    if let Column::Function(agg) = &subquery.columns[0] {
        let mut filtered_rows: Vec<&Vec<Value>> = Vec::new();
        for inner_row in &table.rows {
            let mut combined_row: Vec<Value> = outer_row.to_vec();
            combined_row.extend(inner_row.clone());

            let include_row = if let Some(ref where_expr) = subquery.where_clause {
                evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
            } else {
                true
            };
            if include_row {
                filtered_rows.push(inner_row);
            }
        }
        return compute_aggregate(
            &agg.function,
            &agg.expr,
            table,
            &filtered_rows,
            agg.distinct,
        );
    }

    if let Column::Subquery(nested_subquery) = &subquery.columns[0] {
        let mut candidate_rows: Vec<Vec<Value>> = Vec::new();
        for inner_row in &table.rows {
            let mut combined_row: Vec<Value> = outer_row.to_vec();
            combined_row.extend(inner_row.clone());

            let include_row = if let Some(ref where_expr) = subquery.where_clause {
                evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
            } else {
                true
            };
            if include_row {
                candidate_rows.push(combined_row);
            }
        }

        apply_order_and_slice(&mut candidate_rows)?;

        let mut results = Vec::new();
        for combined_row in candidate_rows {
            let nested_result = eval_scalar_subquery_with_outer(
                db,
                nested_subquery,
                &combined_columns,
                &combined_row,
            )?;
            results.push(nested_result);
        }

        match results.len() {
            0 => Ok(Value::Null),
            1 => Ok(results[0].clone()),
            _ => Err(RustqlError::Internal(
                "Scalar subquery returned more than one row".to_string(),
            )),
        }
    } else {
        let mut candidate_rows: Vec<Vec<Value>> = Vec::new();
        for inner_row in &table.rows {
            let mut combined_row: Vec<Value> = outer_row.to_vec();
            combined_row.extend(inner_row.clone());

            let include_row = if let Some(ref where_expr) = subquery.where_clause {
                evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
            } else {
                true
            };
            if include_row {
                candidate_rows.push(combined_row);
            }
        }

        apply_order_and_slice(&mut candidate_rows)?;

        let mut results = Vec::new();
        for combined_row in &candidate_rows {
            let val = match &subquery.columns[0] {
                Column::All => {
                    return Err(RustqlError::Internal(
                        "Scalar subquery cannot use *".to_string(),
                    ));
                }
                Column::Expression { .. } => {
                    return Err(RustqlError::Internal(
                        "Scalar subquery cannot use expressions".to_string(),
                    ));
                }
                Column::Function(_) => {
                    return Err(RustqlError::Internal(
                        "Scalar subquery cannot use aggregate functions".to_string(),
                    ));
                }
                Column::Subquery(_) => {
                    return Err(RustqlError::Internal(
                        "Scalar subquery cannot use nested subqueries".to_string(),
                    ));
                }
                Column::Named { name, .. } => {
                    let col_idx = combined_columns
                        .iter()
                        .position(|c| {
                            c.name
                                == (if name.contains('.') {
                                    name.split('.').next_back().unwrap_or(name)
                                } else {
                                    name
                                })
                        })
                        .ok_or_else(|| RustqlError::ColumnNotFound(name.clone()))?;
                    combined_row[col_idx].clone()
                }
            };
            results.push(val);
        }

        match results.len() {
            0 => Ok(Value::Null),
            1 => Ok(results[0].clone()),
            _ => Err(RustqlError::Internal(
                "Scalar subquery returned more than one row".to_string(),
            )),
        }
    }
}

fn execute_select_with_joins(stmt: SelectStatement, db: &Database) -> Result<String, RustqlError> {
    let main_table = db
        .tables
        .get(&stmt.from)
        .ok_or_else(|| RustqlError::TableNotFound(stmt.from.clone()))?;

    let (joined_rows, all_columns) =
        perform_multiple_joins(db, main_table, &stmt.from, &stmt.joins)?;

    let mut filtered_rows: Vec<Vec<Value>> = Vec::new();
    let db_ref: &Database = db;
    for row in &joined_rows {
        let include_row = if let Some(ref where_expr) = stmt.where_clause {
            evaluate_expression(Some(db_ref), where_expr, &all_columns, row)?
        } else {
            true
        };
        if include_row {
            filtered_rows.push(row.clone());
        }
    }

    let has_aggregate = stmt
        .columns
        .iter()
        .any(|col| matches!(col, Column::Function(_)));

    if stmt.group_by.is_some() {
        let temp_table = Table {
            columns: all_columns.clone(),
            rows: Vec::new(),
            constraints: vec![],
        };
        let row_refs: Vec<&Vec<Value>> = filtered_rows.iter().collect();
        return execute_select_with_grouping(stmt, &temp_table, row_refs);
    }

    if has_aggregate {
        let temp_table = Table {
            columns: all_columns.clone(),
            rows: Vec::new(),
            constraints: vec![],
        };
        let row_refs: Vec<&Vec<Value>> = filtered_rows.iter().collect();
        return execute_select_with_aggregates(stmt, &temp_table, row_refs);
    }

    if let Some(ref order_by) = stmt.order_by {
        filtered_rows.sort_by(|a, b| {
            for order_expr in order_by {
                let a_val = evaluate_value_expression(&order_expr.expr, &all_columns, a)
                    .unwrap_or(Value::Null);
                let b_val = evaluate_value_expression(&order_expr.expr, &all_columns, b)
                    .unwrap_or(Value::Null);
                let cmp = compare_values_for_sort(&a_val, &b_val);
                if cmp != Ordering::Equal {
                    return if order_expr.asc { cmp } else { cmp.reverse() };
                }
            }
            Ordering::Equal
        });
    }

    let column_specs: Vec<(String, usize)> = match &stmt.columns[0] {
        Column::All => all_columns
            .iter()
            .enumerate()
            .map(|(idx, col)| (col.name.clone(), idx))
            .collect(),
        Column::Named { .. } => {
            let mut specs = Vec::new();
            for col in &stmt.columns {
                match col {
                    Column::Named { name, alias } => {
                        let target_name = if name.contains('.') {
                            name.split('.').next_back().unwrap_or(name)
                        } else {
                            name
                        };
                        let idx = all_columns
                            .iter()
                            .position(|c| c.name == target_name)
                            .ok_or_else(|| RustqlError::ColumnNotFound(name.clone()))?;
                        let header = alias.clone().unwrap_or_else(|| name.clone());
                        specs.push((header, idx));
                    }
                    _ => return Err(RustqlError::TypeMismatch("Invalid column type".to_string())),
                }
            }
            specs
        }
        _ => return Err(RustqlError::TypeMismatch("Invalid column type".to_string())),
    };

    let mut result = String::new();
    for (header, _) in &column_specs {
        result.push_str(&format!("{}\t", header));
    }
    result.push('\n');
    result.push_str(&"-".repeat(40));
    result.push('\n');

    let offset = stmt.offset.unwrap_or(0);
    let limit = stmt.limit.unwrap_or(filtered_rows.len());

    use std::collections::BTreeSet;
    let mut seen: BTreeSet<Vec<Value>> = BTreeSet::new();
    let mut skipped = 0usize;
    let mut emitted = 0usize;
    for row in &filtered_rows {
        let projected: Vec<Value> = column_specs
            .iter()
            .map(|(_, idx)| row[*idx].clone())
            .collect();
        if stmt.distinct && !seen.insert(projected.clone()) {
            continue;
        }
        if skipped < offset {
            skipped += 1;
            continue;
        }
        if emitted >= limit {
            break;
        }
        for val in &projected {
            result.push_str(&format!("{}\t", format_value(val)));
        }
        result.push('\n');
        emitted += 1;
    }

    Ok(result)
}

fn execute_select_from_subquery(
    stmt: SelectStatement,
    subquery: &SelectStatement,
    alias: &str,
) -> Result<String, RustqlError> {
    let subquery_result = {
        let db = get_database_read();
        execute_select_internal(subquery.clone(), &db)?
    };

    let columns: Vec<ColumnDefinition> = subquery_result
        .headers
        .iter()
        .enumerate()
        .map(|(idx, name)| {
            let data_type = subquery_result
                .rows
                .first()
                .and_then(|row| row.get(idx))
                .map(|val| match val {
                    Value::Integer(_) => DataType::Integer,
                    Value::Float(_) => DataType::Float,
                    Value::Boolean(_) => DataType::Boolean,
                    Value::Date(_) => DataType::Date,
                    Value::Time(_) => DataType::Time,
                    Value::DateTime(_) => DataType::DateTime,
                    _ => DataType::Text,
                })
                .unwrap_or(DataType::Text);
            ColumnDefinition {
                name: name.clone(),
                data_type,
                nullable: true,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
            }
        })
        .collect();

    {
        let mut db = get_database_write();
        db.tables.insert(
            alias.to_string(),
            Table {
                columns,
                rows: subquery_result.rows,
                constraints: vec![],
            },
        );
    }

    let mut new_stmt = stmt;
    new_stmt.from = alias.to_string();
    new_stmt.from_subquery = None;

    let result = execute_select(new_stmt);

    {
        let mut db = get_database_write();
        db.tables.remove(alias);
    }

    result
}

fn execute_select_from_view(
    stmt: SelectStatement,
    view_name: &str,
    view_sql: &str,
) -> Result<String, RustqlError> {
    let tokens = crate::lexer::tokenize(view_sql)?;
    let view_stmt = crate::parser::parse(tokens)?;
    let view_select = match view_stmt {
        crate::ast::Statement::Select(s) => s,
        _ => {
            return Err(RustqlError::Internal(
                "View definition is not a SELECT statement".to_string(),
            ));
        }
    };

    let view_result = {
        let db = get_database_read();
        execute_select_internal(view_select, &db)?
    };

    let columns: Vec<ColumnDefinition> = view_result
        .headers
        .iter()
        .enumerate()
        .map(|(idx, name)| {
            let data_type = view_result
                .rows
                .first()
                .and_then(|row| row.get(idx))
                .map(|val| match val {
                    Value::Integer(_) => DataType::Integer,
                    Value::Float(_) => DataType::Float,
                    Value::Boolean(_) => DataType::Boolean,
                    Value::Date(_) => DataType::Date,
                    Value::Time(_) => DataType::Time,
                    Value::DateTime(_) => DataType::DateTime,
                    _ => DataType::Text,
                })
                .unwrap_or(DataType::Text);
            ColumnDefinition {
                name: name.clone(),
                data_type,
                nullable: true,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
            }
        })
        .collect();

    {
        let mut db = get_database_write();
        db.tables.insert(
            view_name.to_string(),
            Table {
                columns,
                rows: view_result.rows,
                constraints: vec![],
            },
        );
    }

    let result = execute_select(stmt);

    {
        let mut db = get_database_write();
        db.tables.remove(view_name);
    }

    result
}

fn execute_select_with_ctes(mut stmt: SelectStatement) -> Result<String, RustqlError> {
    let ctes = std::mem::take(&mut stmt.ctes);
    let mut db = get_database_write();

    let mut temp_table_names: Vec<String> = Vec::new();
    for cte in &ctes {
        if cte.recursive {
            let cte_query = &cte.query;
            if let Some((ref set_op_type, ref recursive_part)) = cte_query.set_op {
                if !matches!(set_op_type, SetOperation::UnionAll | SetOperation::Union) {
                    return Err(RustqlError::Internal(
                        "Recursive CTE requires UNION or UNION ALL".to_string(),
                    ));
                }
                let is_union_all = matches!(set_op_type, SetOperation::UnionAll);

                let mut base_stmt = cte_query.clone();
                base_stmt.set_op = None;
                let base_result = execute_select_internal(base_stmt, &db)?;

                let columns: Vec<ColumnDefinition> = base_result
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
                    })
                    .collect();

                let mut all_rows: Vec<Vec<Value>> = base_result.rows.clone();
                let mut working_rows: Vec<Vec<Value>> = base_result.rows;
                let mut seen: Option<std::collections::BTreeSet<Vec<Value>>> = if is_union_all {
                    None
                } else {
                    let mut s = std::collections::BTreeSet::new();
                    for row in &all_rows {
                        s.insert(row.clone());
                    }
                    Some(s)
                };

                const MAX_ITERATIONS: usize = 1000;
                for _ in 0..MAX_ITERATIONS {
                    if working_rows.is_empty() {
                        break;
                    }

                    db.tables.insert(
                        cte.name.clone(),
                        Table {
                            columns: columns.clone(),
                            rows: working_rows,
                            constraints: vec![],
                        },
                    );

                    let recursive_result = execute_select_internal(*recursive_part.clone(), &db)?;

                    let mut new_rows: Vec<Vec<Value>> = Vec::new();
                    for row in recursive_result.rows {
                        if let Some(ref mut seen_set) = seen {
                            if seen_set.insert(row.clone()) {
                                new_rows.push(row);
                            }
                        } else {
                            new_rows.push(row);
                        }
                    }

                    if new_rows.is_empty() {
                        break;
                    }

                    all_rows.extend(new_rows.clone());
                    working_rows = new_rows;
                }

                db.tables.insert(
                    cte.name.clone(),
                    Table {
                        columns,
                        rows: all_rows,
                        constraints: vec![],
                    },
                );
            } else {
                let cte_result = execute_select_internal(cte.query.clone(), &db)?;
                let columns: Vec<ColumnDefinition> = cte_result
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
                    })
                    .collect();
                db.tables.insert(
                    cte.name.clone(),
                    Table {
                        columns,
                        rows: cte_result.rows,
                        constraints: vec![],
                    },
                );
            }
        } else {
            let cte_result = execute_select_internal(cte.query.clone(), &db)?;
            let columns: Vec<ColumnDefinition> = cte_result
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
                })
                .collect();
            db.tables.insert(
                cte.name.clone(),
                Table {
                    columns,
                    rows: cte_result.rows,
                    constraints: vec![],
                },
            );
        }
        temp_table_names.push(cte.name.clone());
    }

    drop(db);

    let result = execute_select(stmt);

    let mut db = get_database_write();
    for name in temp_table_names {
        db.tables.remove(&name);
    }

    result
}
