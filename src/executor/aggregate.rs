use crate::ast::*;
use crate::database::Table;
use crate::error::RustqlError;
use std::cmp::Ordering;
use std::collections::BTreeMap;

use super::expr::{
    apply_arithmetic, compare_values_for_sort, evaluate_value_expression, format_value,
};

fn remember_distinct(seen: &mut Vec<Value>, val: &Value) -> bool {
    if seen.iter().any(|existing| existing == val) {
        false
    } else {
        seen.push(val.clone());
        true
    }
}

pub fn format_aggregate_header(agg: &AggregateFunction) -> String {
    if let Some(alias) = &agg.alias {
        return alias.clone();
    }

    let func_name = match agg.function {
        AggregateFunctionType::Count => "Count",
        AggregateFunctionType::Sum => "Sum",
        AggregateFunctionType::Avg => "Avg",
        AggregateFunctionType::Min => "Min",
        AggregateFunctionType::Max => "Max",
        AggregateFunctionType::Stddev => "Stddev",
        AggregateFunctionType::Variance => "Variance",
        AggregateFunctionType::GroupConcat => "GroupConcat",
        AggregateFunctionType::BoolAnd => "BoolAnd",
        AggregateFunctionType::BoolOr => "BoolOr",
        AggregateFunctionType::Median => "Median",
        AggregateFunctionType::Mode => "Mode",
        AggregateFunctionType::PercentileCont => "PercentileCont",
        AggregateFunctionType::PercentileDisc => "PercentileDisc",
    };

    let distinct_str = if agg.distinct { "DISTINCT " } else { "" };

    let expr_str = match &*agg.expr {
        Expression::Column(name) if name == "*" => "*".to_string(),
        Expression::Column(name) => name.clone(),
        _ => "*".to_string(),
    };

    format!("{}({}{})", func_name, distinct_str, expr_str)
}

pub fn compute_aggregate_with_options(
    func: &AggregateFunctionType,
    expr: &Expression,
    table: &Table,
    rows: &[&Vec<Value>],
    distinct: bool,
    separator: Option<&str>,
    percentile: Option<f64>,
) -> Result<Value, RustqlError> {
    compute_aggregate_inner(func, expr, table, rows, distinct, separator, percentile)
}

pub fn compute_aggregate(
    func: &AggregateFunctionType,
    expr: &Expression,
    table: &Table,
    rows: &[&Vec<Value>],
    distinct: bool,
) -> Result<Value, RustqlError> {
    compute_aggregate_inner(func, expr, table, rows, distinct, None, None)
}

fn compute_aggregate_inner(
    func: &AggregateFunctionType,
    expr: &Expression,
    table: &Table,
    rows: &[&Vec<Value>],
    distinct: bool,
    separator: Option<&str>,
    percentile: Option<f64>,
) -> Result<Value, RustqlError> {
    match func {
        AggregateFunctionType::Count => {
            if matches!(expr, Expression::Column(name) if name == "*") {
                if distinct {
                    return Err(RustqlError::AggregateError(
                        "COUNT(DISTINCT *) is not supported".to_string(),
                    ));
                }
                Ok(Value::Integer(rows.len() as i64))
            } else {
                let mut count = 0;
                let mut seen: Vec<Value> = Vec::new();
                for row in rows {
                    let val = evaluate_value_expression(expr, &table.columns, row)?;
                    if matches!(&val, Value::Null) {
                        continue;
                    }
                    if distinct && !remember_distinct(&mut seen, &val) {
                        continue;
                    }
                    count += 1;
                }
                Ok(Value::Integer(count))
            }
        }
        AggregateFunctionType::Sum => {
            let mut sum = 0.0;
            let mut has_value = false;
            let mut seen: Vec<Value> = Vec::new();
            for row in rows {
                let val = evaluate_value_expression(expr, &table.columns, row)?;
                if matches!(&val, Value::Null) {
                    continue;
                }
                if distinct && !remember_distinct(&mut seen, &val) {
                    continue;
                }
                match &val {
                    Value::Integer(n) => {
                        sum += *n as f64;
                        has_value = true;
                    }
                    Value::Float(f) => {
                        sum += *f;
                        has_value = true;
                    }
                    _ => {
                        return Err(RustqlError::AggregateError(
                            "SUM requires numeric values".to_string(),
                        ));
                    }
                };
            }
            if has_value {
                Ok(Value::Float(sum))
            } else {
                Ok(Value::Null)
            }
        }
        AggregateFunctionType::Avg => {
            let mut sum = 0.0;
            let mut count = 0;
            let mut seen: Vec<Value> = Vec::new();
            for row in rows {
                let val = evaluate_value_expression(expr, &table.columns, row)?;
                if matches!(&val, Value::Null) {
                    continue;
                }
                if distinct && !remember_distinct(&mut seen, &val) {
                    continue;
                }
                match &val {
                    Value::Integer(n) => {
                        sum += *n as f64;
                        count += 1;
                    }
                    Value::Float(f) => {
                        sum += *f;
                        count += 1;
                    }
                    _ => {
                        return Err(RustqlError::AggregateError(
                            "AVG requires numeric values".to_string(),
                        ));
                    }
                };
            }
            if count > 0 {
                Ok(Value::Float(sum / count as f64))
            } else {
                Ok(Value::Null)
            }
        }
        AggregateFunctionType::Min => {
            let mut min_val: Option<Value> = None;
            let mut seen: Vec<Value> = Vec::new();
            for row in rows {
                let val = evaluate_value_expression(expr, &table.columns, row)?;
                if matches!(&val, Value::Null) {
                    continue;
                }
                if distinct && !remember_distinct(&mut seen, &val) {
                    continue;
                }
                min_val = Some(match min_val {
                    None => val,
                    Some(ref current) => {
                        if compare_values_for_sort(&val, current) == Ordering::Less {
                            val
                        } else {
                            current.clone()
                        }
                    }
                });
            }
            Ok(min_val.unwrap_or(Value::Null))
        }
        AggregateFunctionType::Max => {
            let mut max_val: Option<Value> = None;
            let mut seen: Vec<Value> = Vec::new();
            for row in rows {
                let val = evaluate_value_expression(expr, &table.columns, row)?;
                if matches!(&val, Value::Null) {
                    continue;
                }
                if distinct && !remember_distinct(&mut seen, &val) {
                    continue;
                }
                max_val = Some(match max_val {
                    None => val,
                    Some(ref current) => {
                        if compare_values_for_sort(&val, current) == Ordering::Greater {
                            val
                        } else {
                            current.clone()
                        }
                    }
                });
            }
            Ok(max_val.unwrap_or(Value::Null))
        }
        AggregateFunctionType::Variance => {
            let mut values: Vec<f64> = Vec::new();
            let mut seen: Vec<Value> = Vec::new();
            for row in rows {
                let val = evaluate_value_expression(expr, &table.columns, row)?;
                if matches!(&val, Value::Null) {
                    continue;
                }
                if distinct && !remember_distinct(&mut seen, &val) {
                    continue;
                }
                match &val {
                    Value::Integer(n) => values.push(*n as f64),
                    Value::Float(f) => values.push(*f),
                    _ => {
                        return Err(RustqlError::AggregateError(
                            "VARIANCE requires numeric values".to_string(),
                        ));
                    }
                }
            }
            if values.is_empty() {
                return Ok(Value::Null);
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            Ok(Value::Float(variance))
        }
        AggregateFunctionType::Stddev => {
            let mut values: Vec<f64> = Vec::new();
            let mut seen: Vec<Value> = Vec::new();
            for row in rows {
                let val = evaluate_value_expression(expr, &table.columns, row)?;
                if matches!(&val, Value::Null) {
                    continue;
                }
                if distinct && !remember_distinct(&mut seen, &val) {
                    continue;
                }
                match &val {
                    Value::Integer(n) => values.push(*n as f64),
                    Value::Float(f) => values.push(*f),
                    _ => {
                        return Err(RustqlError::AggregateError(
                            "STDDEV requires numeric values".to_string(),
                        ));
                    }
                }
            }
            if values.is_empty() {
                return Ok(Value::Null);
            }
            let mean = values.iter().sum::<f64>() / values.len() as f64;
            let variance =
                values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
            Ok(Value::Float(variance.sqrt()))
        }
        AggregateFunctionType::GroupConcat => {
            let sep = separator.unwrap_or(",");
            let mut parts: Vec<String> = Vec::new();
            let mut seen_vals: Vec<Value> = Vec::new();
            for row in rows {
                let val = evaluate_value_expression(expr, &table.columns, row)?;
                if matches!(&val, Value::Null) {
                    continue;
                }
                if distinct && !remember_distinct(&mut seen_vals, &val) {
                    continue;
                }
                parts.push(format_value(&val));
            }
            if parts.is_empty() {
                Ok(Value::Null)
            } else {
                Ok(Value::Text(parts.join(sep)))
            }
        }
        AggregateFunctionType::BoolAnd => {
            let mut result = true;
            let mut has_value = false;
            for row in rows {
                let val = evaluate_value_expression(expr, &table.columns, row)?;
                if matches!(&val, Value::Null) {
                    continue;
                }
                has_value = true;
                match &val {
                    Value::Boolean(b) => {
                        if !b {
                            result = false;
                        }
                    }
                    Value::Integer(n) => {
                        if *n == 0 {
                            result = false;
                        }
                    }
                    _ => {
                        return Err(RustqlError::AggregateError(
                            "BOOL_AND requires boolean or integer values".to_string(),
                        ));
                    }
                }
            }
            if has_value {
                Ok(Value::Boolean(result))
            } else {
                Ok(Value::Null)
            }
        }
        AggregateFunctionType::BoolOr => {
            let mut result = false;
            let mut has_value = false;
            for row in rows {
                let val = evaluate_value_expression(expr, &table.columns, row)?;
                if matches!(&val, Value::Null) {
                    continue;
                }
                has_value = true;
                match &val {
                    Value::Boolean(b) => {
                        if *b {
                            result = true;
                        }
                    }
                    Value::Integer(n) => {
                        if *n != 0 {
                            result = true;
                        }
                    }
                    _ => {
                        return Err(RustqlError::AggregateError(
                            "BOOL_OR requires boolean or integer values".to_string(),
                        ));
                    }
                }
            }
            if has_value {
                Ok(Value::Boolean(result))
            } else {
                Ok(Value::Null)
            }
        }
        AggregateFunctionType::Median => {
            let mut values: Vec<f64> = Vec::new();
            let mut seen_vals: Vec<Value> = Vec::new();
            for row in rows {
                let val = evaluate_value_expression(expr, &table.columns, row)?;
                if matches!(&val, Value::Null) {
                    continue;
                }
                if distinct && !remember_distinct(&mut seen_vals, &val) {
                    continue;
                }
                match &val {
                    Value::Integer(n) => values.push(*n as f64),
                    Value::Float(f) => values.push(*f),
                    _ => {
                        return Err(RustqlError::AggregateError(
                            "MEDIAN requires numeric values".to_string(),
                        ));
                    }
                }
            }
            if values.is_empty() {
                return Ok(Value::Null);
            }
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
            let mid = values.len() / 2;
            if values.len().is_multiple_of(2) {
                Ok(Value::Float((values[mid - 1] + values[mid]) / 2.0))
            } else {
                Ok(Value::Float(values[mid]))
            }
        }
        AggregateFunctionType::Mode => {
            let mut counts: Vec<(Value, usize)> = Vec::new();
            let mut seen_vals: Vec<Value> = Vec::new();
            for row in rows {
                let val = evaluate_value_expression(expr, &table.columns, row)?;
                if matches!(&val, Value::Null) {
                    continue;
                }
                if distinct && !remember_distinct(&mut seen_vals, &val) {
                    continue;
                }
                if let Some(entry) = counts.iter_mut().find(|(v, _)| v == &val) {
                    entry.1 += 1;
                } else {
                    counts.push((val, 1));
                }
            }
            if counts.is_empty() {
                return Ok(Value::Null);
            }
            counts.sort_by(|a, b| b.1.cmp(&a.1));
            Ok(counts[0].0.clone())
        }
        AggregateFunctionType::PercentileCont => {
            let frac = percentile.unwrap_or(0.5);
            let mut values: Vec<f64> = Vec::new();
            let mut seen_vals: Vec<Value> = Vec::new();
            for row in rows {
                let val = evaluate_value_expression(expr, &table.columns, row)?;
                if matches!(&val, Value::Null) {
                    continue;
                }
                if distinct && !remember_distinct(&mut seen_vals, &val) {
                    continue;
                }
                match &val {
                    Value::Integer(n) => values.push(*n as f64),
                    Value::Float(f) => values.push(*f),
                    _ => {
                        return Err(RustqlError::AggregateError(
                            "PERCENTILE_CONT requires numeric values".to_string(),
                        ));
                    }
                }
            }
            if values.is_empty() {
                return Ok(Value::Null);
            }
            values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
            let n = values.len();
            if n == 1 {
                return Ok(Value::Float(values[0]));
            }
            let pos = frac * (n - 1) as f64;
            let lower = pos.floor() as usize;
            let upper = pos.ceil() as usize;
            if lower == upper {
                Ok(Value::Float(values[lower]))
            } else {
                let weight = pos - lower as f64;
                Ok(Value::Float(
                    values[lower] * (1.0 - weight) + values[upper] * weight,
                ))
            }
        }
        AggregateFunctionType::PercentileDisc => {
            let frac = percentile.unwrap_or(0.5);
            let mut values: Vec<Value> = Vec::new();
            let mut seen_vals: Vec<Value> = Vec::new();
            for row in rows {
                let val = evaluate_value_expression(expr, &table.columns, row)?;
                if matches!(&val, Value::Null) {
                    continue;
                }
                if distinct && !remember_distinct(&mut seen_vals, &val) {
                    continue;
                }
                values.push(val);
            }
            if values.is_empty() {
                return Ok(Value::Null);
            }
            values.sort();
            let idx = ((frac * values.len() as f64).ceil() as usize).saturating_sub(1);
            let idx = idx.min(values.len() - 1);
            Ok(values[idx].clone())
        }
    }
}

pub fn evaluate_having(
    expr: &Expression,
    _columns: &[Column],
    table: &Table,
    rows: &[&Vec<Value>],
) -> Result<bool, RustqlError> {
    match expr {
        Expression::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(evaluate_having(left, _columns, table, rows)?
                && evaluate_having(right, _columns, table, rows)?),
            BinaryOperator::Or => Ok(evaluate_having(left, _columns, table, rows)?
                || evaluate_having(right, _columns, table, rows)?),
            _ => {
                let left_val = evaluate_having_value(left, _columns, table, rows)?;
                let right_val = evaluate_having_value(right, _columns, table, rows)?;
                super::expr::compare_values(&left_val, op, &right_val)
            }
        },
        Expression::IsNull { expr, not } => {
            let value = evaluate_having_value(expr, _columns, table, rows)?;
            let is_null = matches!(value, Value::Null);
            Ok(if *not { !is_null } else { is_null })
        }
        Expression::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => Ok(!evaluate_having(expr, _columns, table, rows)?),
            _ => Err(RustqlError::Internal(
                "Unsupported unary operation in HAVING clause".to_string(),
            )),
        },
        _ => Err(RustqlError::Internal(
            "Invalid expression in HAVING clause".to_string(),
        )),
    }
}

fn evaluate_having_value(
    expr: &Expression,
    _columns: &[Column],
    table: &Table,
    rows: &[&Vec<Value>],
) -> Result<Value, RustqlError> {
    match expr {
        Expression::Function(agg) => compute_aggregate_with_options(
            &agg.function,
            &agg.expr,
            table,
            rows,
            agg.distinct,
            agg.separator.as_deref(),
            agg.percentile,
        ),
        Expression::Value(val) => Ok(val.clone()),
        Expression::Column(name) => {
            if !rows.is_empty() {
                let normalized = if name.contains('.') {
                    name.split('.').next_back().unwrap_or(name)
                } else {
                    name.as_str()
                };
                if let Some(idx) = table.columns.iter().position(|c| c.name == normalized) {
                    Ok(rows[0][idx].clone())
                } else {
                    Err(RustqlError::ColumnNotFound(format!(
                        "{} (HAVING clause)",
                        name
                    )))
                }
            } else {
                Err(RustqlError::Internal(
                    "No rows in group for HAVING clause".to_string(),
                ))
            }
        }
        _ => Err(RustqlError::Internal(
            "Complex expressions not yet supported in HAVING".to_string(),
        )),
    }
}

pub fn execute_select_with_aggregates(
    stmt: SelectStatement,
    table: &Table,
    rows: Vec<&Vec<Value>>,
) -> Result<String, RustqlError> {
    let mut result = String::new();
    for col in &stmt.columns {
        match col {
            Column::Function(agg) => {
                let name = format_aggregate_header(agg);
                result.push_str(&format!("{}\t", name));
            }
            Column::Named { name, alias } => {
                let header = alias.clone().unwrap_or_else(|| name.clone());
                result.push_str(&format!("{}\t", header));
            }
            Column::Expression { alias, .. } => {
                let header = alias.clone().unwrap_or_else(|| "<expression>".to_string());
                result.push_str(&format!("{}\t", header));
            }
            _ => {}
        }
    }
    result.push('\n');
    result.push_str(&"-".repeat(40));
    result.push('\n');

    for col in &stmt.columns {
        match col {
            Column::Function(agg) => {
                let value = compute_aggregate_with_options(
                    &agg.function,
                    &agg.expr,
                    table,
                    &rows,
                    agg.distinct,
                    agg.separator.as_deref(),
                    agg.percentile,
                )?;
                result.push_str(&format!("{}\t", format_value(&value)));
            }
            _ => {
                return Err(RustqlError::Internal(
                    "Cannot mix aggregate and non-aggregate columns without GROUP BY".to_string(),
                ));
            }
        }
    }
    result.push('\n');
    Ok(result)
}

pub fn execute_select_with_grouping(
    stmt: SelectStatement,
    table: &Table,
    rows: Vec<&Vec<Value>>,
) -> Result<String, RustqlError> {
    let raw_group_by = stmt.group_by.as_ref().unwrap();
    let mut group_by_info: Vec<(Expression, Option<(String, usize)>)> =
        Vec::with_capacity(raw_group_by.len());
    for expr in raw_group_by {
        let col_info = if let Expression::Column(col_name) = expr {
            let normalized = if col_name.contains('.') {
                col_name
                    .split('.')
                    .next_back()
                    .unwrap_or(col_name)
                    .to_string()
            } else {
                col_name.clone()
            };
            table
                .columns
                .iter()
                .position(|c| c.name == normalized)
                .map(|idx| (normalized, idx))
        } else {
            None
        };
        group_by_info.push((expr.clone(), col_info));
    }

    let mut groups: BTreeMap<Vec<Value>, Vec<&Vec<Value>>> = BTreeMap::new();
    for row in rows {
        let key: Vec<Value> = group_by_info
            .iter()
            .map(|(expr, col_info)| {
                if let Some((_, idx)) = col_info {
                    row[*idx].clone()
                } else {
                    evaluate_value_expression(expr, &table.columns, row).unwrap_or(Value::Null)
                }
            })
            .collect();
        groups.entry(key).or_default().push(row);
    }

    let group_by_normalized_with_indices: Vec<(String, usize)> = group_by_info
        .iter()
        .filter_map(|(_, col_info)| col_info.clone())
        .collect();

    let mut column_specs: Vec<(String, Column)> = Vec::new();
    for col in &stmt.columns {
        match col {
            Column::Function(agg) => {
                let header = format_aggregate_header(agg);
                column_specs.push((header, col.clone()));
            }
            Column::Named { name, alias } => {
                let header = alias.clone().unwrap_or_else(|| name.clone());
                column_specs.push((header, col.clone()));
            }
            Column::Expression { alias, .. } => {
                let header = alias.clone().unwrap_or_else(|| "<expression>".to_string());
                column_specs.push((header, col.clone()));
            }
            _ => {}
        }
    }

    let mut result = String::new();
    for (header, _) in &column_specs {
        result.push_str(&format!("{}\t", header));
    }
    result.push('\n');
    result.push_str(&"-".repeat(40));
    result.push('\n');

    let mut grouped_outputs: Vec<(Vec<Value>, Vec<Value>)> = Vec::new();

    for (_group_key, group_rows) in groups {
        if let Some(ref having_expr) = stmt.having {
            let should_include = evaluate_having(having_expr, &stmt.columns, table, &group_rows)?;
            if !should_include {
                continue;
            }
        }
        let mut projected_row: Vec<Value> = Vec::with_capacity(column_specs.len());
        for (_, col_spec) in &column_specs {
            match col_spec {
                Column::Function(agg) => {
                    let value = compute_aggregate_with_options(
                        &agg.function,
                        &agg.expr,
                        table,
                        &group_rows,
                        agg.distinct,
                        agg.separator.as_deref(),
                        agg.percentile,
                    )?;
                    projected_row.push(value);
                }
                Column::Named { name, .. } => {
                    let column_name = if name.contains('.') {
                        name.split('.').next_back().unwrap_or(name)
                    } else {
                        name.as_str()
                    };
                    if let Some((_, group_idx)) = group_by_normalized_with_indices
                        .iter()
                        .find(|(normalized, _)| normalized == column_name)
                    {
                        projected_row.push(group_rows[0][*group_idx].clone());
                    } else {
                        return Err(RustqlError::Internal(format!(
                            "Column '{}' must appear in GROUP BY clause",
                            name
                        )));
                    }
                }
                Column::Expression { expr, .. } => {
                    let value = evaluate_group_order_expression(
                        expr,
                        table,
                        &group_rows,
                        &column_specs,
                        &projected_row,
                        &group_by_normalized_with_indices,
                        false,
                    )?;
                    projected_row.push(value);
                }
                _ => {}
            }
        }

        let mut order_values: Vec<Value> = Vec::new();
        if let Some(ref order_by) = stmt.order_by {
            for order_expr in order_by {
                let value = evaluate_group_order_expression(
                    &order_expr.expr,
                    table,
                    &group_rows,
                    &column_specs,
                    &projected_row,
                    &group_by_normalized_with_indices,
                    true,
                )?;
                order_values.push(value);
            }
        }

        grouped_outputs.push((projected_row, order_values));
    }

    if let Some(ref order_by) = stmt.order_by {
        grouped_outputs.sort_by(|a, b| {
            for (idx, order_expr) in order_by.iter().enumerate() {
                let cmp = compare_values_for_sort(&a.1[idx], &b.1[idx]);
                if cmp != Ordering::Equal {
                    return if order_expr.asc { cmp } else { cmp.reverse() };
                }
            }
            Ordering::Equal
        });
    }

    let offset = stmt.offset.unwrap_or(0);
    let limit = stmt.limit.unwrap_or(grouped_outputs.len());

    use std::collections::BTreeSet;
    let mut seen: BTreeSet<Vec<Value>> = BTreeSet::new();
    let mut skipped = 0usize;
    let mut emitted = 0usize;

    for (row_values, _) in grouped_outputs {
        if stmt.distinct && !seen.insert(row_values.clone()) {
            continue;
        }
        if skipped < offset {
            skipped += 1;
            continue;
        }
        if emitted >= limit {
            break;
        }
        for val in &row_values {
            result.push_str(&format!("{}\t", format_value(val)));
        }
        result.push('\n');
        emitted += 1;
    }

    Ok(result)
}

pub fn evaluate_group_order_expression(
    expr: &Expression,
    table: &Table,
    group_rows: &[&Vec<Value>],
    column_specs: &[(String, Column)],
    projected_row: &[Value],
    group_by_indices: &[(String, usize)],
    allow_ordinal: bool,
) -> Result<Value, RustqlError> {
    match expr {
        Expression::Column(name) => {
            for (idx, (header, col_spec)) in column_specs.iter().enumerate() {
                if header == name {
                    return Ok(projected_row[idx].clone());
                }
                if let Column::Named {
                    name: original_name,
                    alias,
                } = col_spec
                    && (alias.as_ref().map(|a| a == name).unwrap_or(false)
                        || original_name == name
                        || original_name
                            .split('.')
                            .next_back()
                            .map(|n| n == name)
                            .unwrap_or(false))
                {
                    return Ok(projected_row[idx].clone());
                }
                if let Column::Function(agg) = col_spec {
                    let default_alias = agg
                        .alias
                        .clone()
                        .unwrap_or_else(|| format!("{:?}(*)", agg.function));
                    if default_alias == *name {
                        return Ok(projected_row[idx].clone());
                    }
                }
            }

            let normalized = if name.contains('.') {
                name.split('.').next_back().unwrap_or(name)
            } else {
                name.as_str()
            };

            if let Some((_, idx)) = group_by_indices
                .iter()
                .find(|(normalized_name, _)| normalized_name == normalized)
                && let Some(first_row) = group_rows.first()
            {
                return Ok(first_row[*idx].clone());
            }

            Err(RustqlError::ColumnNotFound(format!(
                "{} (ORDER BY grouped)",
                name
            )))
        }
        Expression::Function(agg) => compute_aggregate_with_options(
            &agg.function,
            &agg.expr,
            table,
            group_rows,
            agg.distinct,
            agg.separator.as_deref(),
            agg.percentile,
        ),
        Expression::Value(val) => {
            if allow_ordinal
                && let Value::Integer(ord) = val
                && *ord >= 1
                && (*ord as usize) <= projected_row.len()
            {
                return Ok(projected_row[*ord as usize - 1].clone());
            }
            Ok(val.clone())
        }
        Expression::BinaryOp { left, op, right } => match op {
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide => {
                let left_val = evaluate_group_order_expression(
                    left,
                    table,
                    group_rows,
                    column_specs,
                    projected_row,
                    group_by_indices,
                    false,
                )?;
                let right_val = evaluate_group_order_expression(
                    right,
                    table,
                    group_rows,
                    column_specs,
                    projected_row,
                    group_by_indices,
                    false,
                )?;
                apply_arithmetic(&left_val, &right_val, op)
            }
            _ => Err(RustqlError::Internal(
                "Unsupported operator in ORDER BY for grouped results".to_string(),
            )),
        },
        _ => Err(RustqlError::Internal(
            "Unsupported expression in ORDER BY for grouped results".to_string(),
        )),
    }
}

fn resolve_frame_bounds(
    frame: &Option<WindowFrame>,
    has_order_by: bool,
    partition_len: usize,
    current_pos: usize,
) -> (usize, usize) {
    match frame {
        Some(f) => {
            let start = match f.start {
                WindowFrameBound::UnboundedPreceding => 0,
                WindowFrameBound::Preceding(n) => current_pos.saturating_sub(n),
                WindowFrameBound::CurrentRow => current_pos,
                WindowFrameBound::Following(n) => {
                    (current_pos + n).min(partition_len.saturating_sub(1))
                }
                WindowFrameBound::UnboundedFollowing => partition_len.saturating_sub(1),
            };
            let end = match f.end {
                WindowFrameBound::UnboundedPreceding => 0,
                WindowFrameBound::Preceding(n) => current_pos.saturating_sub(n),
                WindowFrameBound::CurrentRow => current_pos,
                WindowFrameBound::Following(n) => {
                    (current_pos + n).min(partition_len.saturating_sub(1))
                }
                WindowFrameBound::UnboundedFollowing => partition_len.saturating_sub(1),
            };
            (start, end)
        }
        None => {
            if has_order_by {
                (0, current_pos)
            } else {
                (0, partition_len.saturating_sub(1))
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn compute_windowed_aggregate(
    agg_type: &AggregateFunctionType,
    rows: &[Vec<Value>],
    sorted_indices: &[usize],
    columns: &[ColumnDefinition],
    args: &[Expression],
    frame: &Option<WindowFrame>,
    has_order_by: bool,
    pos: usize,
) -> Value {
    let (frame_start, frame_end) =
        resolve_frame_bounds(frame, has_order_by, sorted_indices.len(), pos);

    let default_expr = Expression::Column("*".to_string());
    let expr = args.first().unwrap_or(&default_expr);

    let mut values: Vec<Value> = Vec::new();
    for &row_idx in &sorted_indices[frame_start..=frame_end] {
        let val = evaluate_value_expression(expr, columns, &rows[row_idx]).unwrap_or(Value::Null);
        values.push(val);
    }

    match agg_type {
        AggregateFunctionType::Count => {
            if matches!(expr, Expression::Column(name) if name == "*") {
                Value::Integer(values.len() as i64)
            } else {
                let count = values.iter().filter(|v| !matches!(v, Value::Null)).count();
                Value::Integer(count as i64)
            }
        }
        AggregateFunctionType::Sum => {
            let mut sum = 0.0;
            let mut has_value = false;
            for val in &values {
                match val {
                    Value::Integer(n) => {
                        sum += *n as f64;
                        has_value = true;
                    }
                    Value::Float(f) => {
                        sum += *f;
                        has_value = true;
                    }
                    _ => {}
                }
            }
            if has_value {
                Value::Float(sum)
            } else {
                Value::Null
            }
        }
        AggregateFunctionType::Avg => {
            let mut sum = 0.0;
            let mut count = 0;
            for val in &values {
                match val {
                    Value::Integer(n) => {
                        sum += *n as f64;
                        count += 1;
                    }
                    Value::Float(f) => {
                        sum += *f;
                        count += 1;
                    }
                    _ => {}
                }
            }
            if count > 0 {
                Value::Float(sum / count as f64)
            } else {
                Value::Null
            }
        }
        AggregateFunctionType::Min => {
            let mut min_val: Option<Value> = None;
            for val in values {
                if matches!(&val, Value::Null) {
                    continue;
                }
                min_val = Some(match min_val {
                    None => val,
                    Some(ref current) => {
                        if compare_values_for_sort(&val, current) == Ordering::Less {
                            val
                        } else {
                            current.clone()
                        }
                    }
                });
            }
            min_val.unwrap_or(Value::Null)
        }
        AggregateFunctionType::Max => {
            let mut max_val: Option<Value> = None;
            for val in values {
                if matches!(&val, Value::Null) {
                    continue;
                }
                max_val = Some(match max_val {
                    None => val,
                    Some(ref current) => {
                        if compare_values_for_sort(&val, current) == Ordering::Greater {
                            val
                        } else {
                            current.clone()
                        }
                    }
                });
            }
            max_val.unwrap_or(Value::Null)
        }
        AggregateFunctionType::Variance => {
            let mut nums: Vec<f64> = Vec::new();
            for val in &values {
                match val {
                    Value::Integer(n) => nums.push(*n as f64),
                    Value::Float(f) => nums.push(*f),
                    _ => {}
                }
            }
            if nums.is_empty() {
                return Value::Null;
            }
            let mean = nums.iter().sum::<f64>() / nums.len() as f64;
            let variance = nums.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / nums.len() as f64;
            Value::Float(variance)
        }
        AggregateFunctionType::Stddev => {
            let mut nums: Vec<f64> = Vec::new();
            for val in &values {
                match val {
                    Value::Integer(n) => nums.push(*n as f64),
                    Value::Float(f) => nums.push(*f),
                    _ => {}
                }
            }
            if nums.is_empty() {
                return Value::Null;
            }
            let mean = nums.iter().sum::<f64>() / nums.len() as f64;
            let variance = nums.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / nums.len() as f64;
            Value::Float(variance.sqrt())
        }
        AggregateFunctionType::GroupConcat => {
            let parts: Vec<String> = values
                .iter()
                .filter(|v| !matches!(v, Value::Null))
                .map(format_value)
                .collect();
            if parts.is_empty() {
                Value::Null
            } else {
                Value::Text(parts.join(","))
            }
        }
        AggregateFunctionType::BoolAnd => {
            let mut result = true;
            let mut has_value = false;
            for val in &values {
                match val {
                    Value::Null => continue,
                    Value::Boolean(b) => {
                        has_value = true;
                        if !b {
                            result = false;
                        }
                    }
                    Value::Integer(n) => {
                        has_value = true;
                        if *n == 0 {
                            result = false;
                        }
                    }
                    _ => {}
                }
            }
            if has_value {
                Value::Boolean(result)
            } else {
                Value::Null
            }
        }
        AggregateFunctionType::BoolOr => {
            let mut result = false;
            let mut has_value = false;
            for val in &values {
                match val {
                    Value::Null => continue,
                    Value::Boolean(b) => {
                        has_value = true;
                        if *b {
                            result = true;
                        }
                    }
                    Value::Integer(n) => {
                        has_value = true;
                        if *n != 0 {
                            result = true;
                        }
                    }
                    _ => {}
                }
            }
            if has_value {
                Value::Boolean(result)
            } else {
                Value::Null
            }
        }
        AggregateFunctionType::Median => {
            let mut nums: Vec<f64> = Vec::new();
            for val in &values {
                match val {
                    Value::Integer(n) => nums.push(*n as f64),
                    Value::Float(f) => nums.push(*f),
                    _ => {}
                }
            }
            if nums.is_empty() {
                return Value::Null;
            }
            nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
            let mid = nums.len() / 2;
            if nums.len().is_multiple_of(2) {
                Value::Float((nums[mid - 1] + nums[mid]) / 2.0)
            } else {
                Value::Float(nums[mid])
            }
        }
        AggregateFunctionType::Mode => {
            let mut counts: Vec<(Value, usize)> = Vec::new();
            for val in values {
                if matches!(&val, Value::Null) {
                    continue;
                }
                if let Some(entry) = counts.iter_mut().find(|(v, _)| *v == val) {
                    entry.1 += 1;
                } else {
                    counts.push((val, 1));
                }
            }
            if counts.is_empty() {
                return Value::Null;
            }
            counts.sort_by(|a, b| b.1.cmp(&a.1));
            counts[0].0.clone()
        }
        AggregateFunctionType::PercentileCont => {
            let mut nums: Vec<f64> = Vec::new();
            for val in &values {
                match val {
                    Value::Integer(n) => nums.push(*n as f64),
                    Value::Float(f) => nums.push(*f),
                    _ => {}
                }
            }
            if nums.is_empty() {
                return Value::Null;
            }
            nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
            let frac = 0.5;
            let n = nums.len();
            if n == 1 {
                return Value::Float(nums[0]);
            }
            let p = frac * (n - 1) as f64;
            let lower = p.floor() as usize;
            let upper = p.ceil() as usize;
            if lower == upper {
                Value::Float(nums[lower])
            } else {
                let w = p - lower as f64;
                Value::Float(nums[lower] * (1.0 - w) + nums[upper] * w)
            }
        }
        AggregateFunctionType::PercentileDisc => {
            let mut sorted_vals: Vec<Value> = values
                .into_iter()
                .filter(|v| !matches!(v, Value::Null))
                .collect();
            if sorted_vals.is_empty() {
                return Value::Null;
            }
            sorted_vals.sort();
            let frac = 0.5;
            let idx = ((frac * sorted_vals.len() as f64).ceil() as usize).saturating_sub(1);
            let idx = idx.min(sorted_vals.len() - 1);
            sorted_vals[idx].clone()
        }
    }
}

pub fn evaluate_window_functions(
    rows: &mut [Vec<Value>],
    columns: &[ColumnDefinition],
    select_columns: &[Column],
) -> Result<(), RustqlError> {
    for col in select_columns {
        if let Column::Expression {
            expr:
                Expression::WindowFunction {
                    function,
                    args,
                    partition_by,
                    order_by,
                    frame,
                },
            ..
        } = col
        {
            let mut partition_groups: BTreeMap<Vec<Value>, Vec<usize>> = BTreeMap::new();
            for (idx, row) in rows.iter().enumerate() {
                let key: Vec<Value> = partition_by
                    .iter()
                    .map(|expr| {
                        evaluate_value_expression(expr, columns, row).unwrap_or(Value::Null)
                    })
                    .collect();
                partition_groups.entry(key).or_default().push(idx);
            }

            for indices in partition_groups.values() {
                let mut sorted_indices = indices.clone();
                sorted_indices.sort_by(|&a, &b| {
                    for ob in order_by {
                        let va = evaluate_value_expression(&ob.expr, columns, &rows[a])
                            .unwrap_or(Value::Null);
                        let vb = evaluate_value_expression(&ob.expr, columns, &rows[b])
                            .unwrap_or(Value::Null);
                        let cmp = compare_values_for_sort(&va, &vb);
                        if cmp != Ordering::Equal {
                            return if ob.asc { cmp } else { cmp.reverse() };
                        }
                    }
                    Ordering::Equal
                });

                match function {
                    WindowFunctionType::RowNumber => {
                        for (rank, &idx) in sorted_indices.iter().enumerate() {
                            rows[idx].push(Value::Integer(rank as i64 + 1));
                        }
                    }
                    WindowFunctionType::Rank => {
                        let mut current_rank = 1i64;
                        for (i, &idx) in sorted_indices.iter().enumerate() {
                            if i > 0 {
                                let prev_idx = sorted_indices[i - 1];
                                let same = order_by.iter().all(|ob| {
                                    let va = evaluate_value_expression(
                                        &ob.expr,
                                        columns,
                                        &rows[prev_idx],
                                    )
                                    .unwrap_or(Value::Null);
                                    let vb =
                                        evaluate_value_expression(&ob.expr, columns, &rows[idx])
                                            .unwrap_or(Value::Null);
                                    va == vb
                                });
                                if !same {
                                    current_rank = i as i64 + 1;
                                }
                            }
                            rows[idx].push(Value::Integer(current_rank));
                        }
                    }
                    WindowFunctionType::DenseRank => {
                        let mut current_rank = 1i64;
                        for (i, &idx) in sorted_indices.iter().enumerate() {
                            if i > 0 {
                                let prev_idx = sorted_indices[i - 1];
                                let same = order_by.iter().all(|ob| {
                                    let va = evaluate_value_expression(
                                        &ob.expr,
                                        columns,
                                        &rows[prev_idx],
                                    )
                                    .unwrap_or(Value::Null);
                                    let vb =
                                        evaluate_value_expression(&ob.expr, columns, &rows[idx])
                                            .unwrap_or(Value::Null);
                                    va == vb
                                });
                                if !same {
                                    current_rank += 1;
                                }
                            }
                            rows[idx].push(Value::Integer(current_rank));
                        }
                    }
                    WindowFunctionType::Aggregate(agg_type) => {
                        let has_order_by = !order_by.is_empty();
                        for (pos, &idx) in sorted_indices.iter().enumerate() {
                            let val = compute_windowed_aggregate(
                                agg_type,
                                rows,
                                &sorted_indices,
                                columns,
                                args,
                                frame,
                                has_order_by,
                                pos,
                            );
                            rows[idx].push(val);
                        }
                    }
                    WindowFunctionType::Lag => {
                        let offset = if args.len() > 1 {
                            match evaluate_value_expression(
                                &args[1],
                                columns,
                                &rows[sorted_indices[0]],
                            ) {
                                Ok(Value::Integer(n)) => n as usize,
                                _ => 1,
                            }
                        } else {
                            1
                        };
                        let default_val = if args.len() > 2 {
                            evaluate_value_expression(&args[2], columns, &rows[sorted_indices[0]])
                                .unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        };
                        for (i, &idx) in sorted_indices.iter().enumerate() {
                            if i >= offset {
                                let source_idx = sorted_indices[i - offset];
                                let val = if !args.is_empty() {
                                    evaluate_value_expression(&args[0], columns, &rows[source_idx])
                                        .unwrap_or(Value::Null)
                                } else {
                                    Value::Null
                                };
                                rows[idx].push(val);
                            } else {
                                rows[idx].push(default_val.clone());
                            }
                        }
                    }
                    WindowFunctionType::Lead => {
                        let offset = if args.len() > 1 {
                            match evaluate_value_expression(
                                &args[1],
                                columns,
                                &rows[sorted_indices[0]],
                            ) {
                                Ok(Value::Integer(n)) => n as usize,
                                _ => 1,
                            }
                        } else {
                            1
                        };
                        let default_val = if args.len() > 2 {
                            evaluate_value_expression(&args[2], columns, &rows[sorted_indices[0]])
                                .unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        };
                        let len = sorted_indices.len();
                        for (i, &idx) in sorted_indices.iter().enumerate() {
                            if i + offset < len {
                                let source_idx = sorted_indices[i + offset];
                                let val = if !args.is_empty() {
                                    evaluate_value_expression(&args[0], columns, &rows[source_idx])
                                        .unwrap_or(Value::Null)
                                } else {
                                    Value::Null
                                };
                                rows[idx].push(val);
                            } else {
                                rows[idx].push(default_val.clone());
                            }
                        }
                    }
                    WindowFunctionType::Ntile => {
                        let n = if !args.is_empty() {
                            match evaluate_value_expression(
                                &args[0],
                                columns,
                                &rows[sorted_indices[0]],
                            ) {
                                Ok(Value::Integer(v)) => v.max(1) as usize,
                                _ => 1,
                            }
                        } else {
                            1
                        };
                        let total = sorted_indices.len();
                        for (i, &idx) in sorted_indices.iter().enumerate() {
                            let bucket = (i * n / total) + 1;
                            rows[idx].push(Value::Integer(bucket as i64));
                        }
                    }
                    WindowFunctionType::FirstValue => {
                        let has_order_by = !order_by.is_empty();
                        for (pos, &idx) in sorted_indices.iter().enumerate() {
                            let (frame_start, _frame_end) = resolve_frame_bounds(
                                frame,
                                has_order_by,
                                sorted_indices.len(),
                                pos,
                            );
                            let source_idx = sorted_indices[frame_start];
                            let val = if !args.is_empty() {
                                evaluate_value_expression(&args[0], columns, &rows[source_idx])
                                    .unwrap_or(Value::Null)
                            } else {
                                Value::Null
                            };
                            rows[idx].push(val);
                        }
                    }
                    WindowFunctionType::LastValue => {
                        let has_order_by = !order_by.is_empty();
                        for (pos, &idx) in sorted_indices.iter().enumerate() {
                            let (_frame_start, frame_end) = resolve_frame_bounds(
                                frame,
                                has_order_by,
                                sorted_indices.len(),
                                pos,
                            );
                            let source_idx = sorted_indices[frame_end];
                            let val = if !args.is_empty() {
                                evaluate_value_expression(&args[0], columns, &rows[source_idx])
                                    .unwrap_or(Value::Null)
                            } else {
                                Value::Null
                            };
                            rows[idx].push(val);
                        }
                    }
                    WindowFunctionType::NthValue => {
                        let n = if args.len() > 1 {
                            match evaluate_value_expression(
                                &args[1],
                                columns,
                                &rows[sorted_indices[0]],
                            ) {
                                Ok(Value::Integer(v)) => v.max(1) as usize,
                                _ => 1,
                            }
                        } else {
                            1
                        };
                        let has_order_by = !order_by.is_empty();
                        for (pos, &idx) in sorted_indices.iter().enumerate() {
                            let (frame_start, frame_end) = resolve_frame_bounds(
                                frame,
                                has_order_by,
                                sorted_indices.len(),
                                pos,
                            );
                            let frame_len = frame_end - frame_start + 1;
                            if n <= frame_len {
                                let source_idx = sorted_indices[frame_start + n - 1];
                                let val = if !args.is_empty() {
                                    evaluate_value_expression(&args[0], columns, &rows[source_idx])
                                        .unwrap_or(Value::Null)
                                } else {
                                    Value::Null
                                };
                                rows[idx].push(val);
                            } else {
                                rows[idx].push(Value::Null);
                            }
                        }
                    }
                    WindowFunctionType::PercentRank => {
                        let total = sorted_indices.len();
                        if total <= 1 {
                            for &idx in &sorted_indices {
                                rows[idx].push(Value::Float(0.0));
                            }
                        } else {
                            let mut current_rank = 1i64;
                            for (i, &idx) in sorted_indices.iter().enumerate() {
                                if i > 0 {
                                    let prev_idx = sorted_indices[i - 1];
                                    let same = order_by.iter().all(|ob| {
                                        let va = evaluate_value_expression(
                                            &ob.expr,
                                            columns,
                                            &rows[prev_idx],
                                        )
                                        .unwrap_or(Value::Null);
                                        let vb = evaluate_value_expression(
                                            &ob.expr, columns, &rows[idx],
                                        )
                                        .unwrap_or(Value::Null);
                                        va == vb
                                    });
                                    if !same {
                                        current_rank = i as i64 + 1;
                                    }
                                }
                                let pct = (current_rank - 1) as f64 / (total - 1) as f64;
                                rows[idx].push(Value::Float(pct));
                            }
                        }
                    }
                    WindowFunctionType::CumeDist => {
                        let total = sorted_indices.len();
                        let mut i = 0;
                        while i < sorted_indices.len() {
                            let mut j = i + 1;
                            while j < sorted_indices.len() {
                                let same = order_by.iter().all(|ob| {
                                    let va = evaluate_value_expression(
                                        &ob.expr,
                                        columns,
                                        &rows[sorted_indices[i]],
                                    )
                                    .unwrap_or(Value::Null);
                                    let vb = evaluate_value_expression(
                                        &ob.expr,
                                        columns,
                                        &rows[sorted_indices[j]],
                                    )
                                    .unwrap_or(Value::Null);
                                    va == vb
                                });
                                if !same {
                                    break;
                                }
                                j += 1;
                            }
                            let cd = j as f64 / total as f64;
                            for k in i..j {
                                rows[sorted_indices[k]].push(Value::Float(cd));
                            }
                            i = j;
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
