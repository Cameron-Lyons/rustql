use crate::ast::*;
use crate::error::RustqlError;
use std::cmp::Ordering;
use std::collections::BTreeMap;

use super::expr::{compare_values_for_sort, evaluate_value_expression};

pub(crate) const DEFAULT_PERCENTILE_FRACTION: f64 = 0.5;

pub(crate) fn format_aggregate_header(agg: &AggregateFunction) -> String {
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
    rows: &[&Vec<Value>],
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
        let val = evaluate_value_expression(expr, columns, rows[row_idx]).unwrap_or(Value::Null);
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
            let nums = numeric_values(&values);
            if nums.is_empty() {
                Value::Null
            } else {
                Value::Float(nums.iter().sum())
            }
        }
        AggregateFunctionType::Avg => {
            let nums = numeric_values(&values);
            if nums.is_empty() {
                Value::Null
            } else {
                Value::Float(nums.iter().sum::<f64>() / nums.len() as f64)
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
            let nums = numeric_values(&values);
            if nums.is_empty() {
                Value::Null
            } else {
                Value::Float(variance(&nums))
            }
        }
        AggregateFunctionType::Stddev => {
            let nums = numeric_values(&values);
            if nums.is_empty() {
                Value::Null
            } else {
                Value::Float(variance(&nums).sqrt())
            }
        }
        AggregateFunctionType::GroupConcat => {
            let parts: Vec<String> = values
                .iter()
                .filter(|v| !matches!(v, Value::Null))
                .map(ToString::to_string)
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
            let mut nums = numeric_values(&values);
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
            let mut counts: BTreeMap<Value, (usize, usize)> = BTreeMap::new();
            let mut seen_order = 0usize;
            for val in values {
                if matches!(&val, Value::Null) {
                    continue;
                }
                let entry = counts.entry(val).or_insert((0, seen_order));
                entry.0 += 1;
                seen_order += 1;
            }
            if counts.is_empty() {
                return Value::Null;
            }
            let mut best: Option<(Value, usize, usize)> = None;
            for (val, (count, first_seen)) in counts {
                match best {
                    None => best = Some((val, count, first_seen)),
                    Some((_, best_count, best_seen)) => {
                        if count > best_count || (count == best_count && first_seen < best_seen) {
                            best = Some((val, count, first_seen));
                        }
                    }
                }
            }
            best.map(|(val, _, _)| val).unwrap_or(Value::Null)
        }
        AggregateFunctionType::PercentileCont => {
            let mut nums = numeric_values(&values);
            if nums.is_empty() {
                return Value::Null;
            }
            nums.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
            let frac = DEFAULT_PERCENTILE_FRACTION;
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
            let frac = DEFAULT_PERCENTILE_FRACTION;
            let idx = ((frac * sorted_vals.len() as f64).ceil() as usize).saturating_sub(1);
            let idx = idx.min(sorted_vals.len() - 1);
            sorted_vals[idx].clone()
        }
    }
}

fn numeric_values(values: &[Value]) -> Vec<f64> {
    values
        .iter()
        .filter_map(|value| match value {
            Value::Integer(n) => Some(*n as f64),
            Value::Float(f) => Some(*f),
            _ => None,
        })
        .collect()
}

fn variance(nums: &[f64]) -> f64 {
    let mean = nums.iter().sum::<f64>() / nums.len() as f64;
    nums.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / nums.len() as f64
}

fn evaluate_window_function_outputs(
    rows: &[&Vec<Value>],
    columns: &[ColumnDefinition],
    select_columns: &[Column],
) -> Result<Vec<Vec<Value>>, RustqlError> {
    let mut outputs = vec![Vec::new(); rows.len()];

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
            let order_values: Vec<Vec<Value>> = rows
                .iter()
                .map(|row| {
                    order_by
                        .iter()
                        .map(|ob| {
                            evaluate_value_expression(&ob.expr, columns, row).unwrap_or(Value::Null)
                        })
                        .collect()
                })
                .collect();
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
                sorted_indices
                    .sort_by(|&a, &b| compare_window_order_rows(&order_values, order_by, a, b));

                match function {
                    WindowFunctionType::RowNumber => {
                        for (rank, &idx) in sorted_indices.iter().enumerate() {
                            outputs[idx].push(Value::Integer(rank as i64 + 1));
                        }
                    }
                    WindowFunctionType::Rank => {
                        let mut current_rank = 1i64;
                        for (i, &idx) in sorted_indices.iter().enumerate() {
                            if i > 0 {
                                let prev_idx = sorted_indices[i - 1];
                                if !window_order_values_equal(&order_values, prev_idx, idx) {
                                    current_rank = i as i64 + 1;
                                }
                            }
                            outputs[idx].push(Value::Integer(current_rank));
                        }
                    }
                    WindowFunctionType::DenseRank => {
                        let mut current_rank = 1i64;
                        for (i, &idx) in sorted_indices.iter().enumerate() {
                            if i > 0 {
                                let prev_idx = sorted_indices[i - 1];
                                if !window_order_values_equal(&order_values, prev_idx, idx) {
                                    current_rank += 1;
                                }
                            }
                            outputs[idx].push(Value::Integer(current_rank));
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
                            outputs[idx].push(val);
                        }
                    }
                    WindowFunctionType::Lag => {
                        let offset = if args.len() > 1 {
                            match evaluate_value_expression(
                                &args[1],
                                columns,
                                rows[sorted_indices[0]],
                            ) {
                                Ok(Value::Integer(n)) => n as usize,
                                _ => 1,
                            }
                        } else {
                            1
                        };
                        let default_val = if args.len() > 2 {
                            evaluate_value_expression(&args[2], columns, rows[sorted_indices[0]])
                                .unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        };
                        for (i, &idx) in sorted_indices.iter().enumerate() {
                            if i >= offset {
                                let source_idx = sorted_indices[i - offset];
                                let val = if !args.is_empty() {
                                    evaluate_value_expression(&args[0], columns, rows[source_idx])
                                        .unwrap_or(Value::Null)
                                } else {
                                    Value::Null
                                };
                                outputs[idx].push(val);
                            } else {
                                outputs[idx].push(default_val.clone());
                            }
                        }
                    }
                    WindowFunctionType::Lead => {
                        let offset = if args.len() > 1 {
                            match evaluate_value_expression(
                                &args[1],
                                columns,
                                rows[sorted_indices[0]],
                            ) {
                                Ok(Value::Integer(n)) => n as usize,
                                _ => 1,
                            }
                        } else {
                            1
                        };
                        let default_val = if args.len() > 2 {
                            evaluate_value_expression(&args[2], columns, rows[sorted_indices[0]])
                                .unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        };
                        let len = sorted_indices.len();
                        for (i, &idx) in sorted_indices.iter().enumerate() {
                            if i + offset < len {
                                let source_idx = sorted_indices[i + offset];
                                let val = if !args.is_empty() {
                                    evaluate_value_expression(&args[0], columns, rows[source_idx])
                                        .unwrap_or(Value::Null)
                                } else {
                                    Value::Null
                                };
                                outputs[idx].push(val);
                            } else {
                                outputs[idx].push(default_val.clone());
                            }
                        }
                    }
                    WindowFunctionType::Ntile => {
                        let n = if !args.is_empty() {
                            match evaluate_value_expression(
                                &args[0],
                                columns,
                                rows[sorted_indices[0]],
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
                            outputs[idx].push(Value::Integer(bucket as i64));
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
                                evaluate_value_expression(&args[0], columns, rows[source_idx])
                                    .unwrap_or(Value::Null)
                            } else {
                                Value::Null
                            };
                            outputs[idx].push(val);
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
                                evaluate_value_expression(&args[0], columns, rows[source_idx])
                                    .unwrap_or(Value::Null)
                            } else {
                                Value::Null
                            };
                            outputs[idx].push(val);
                        }
                    }
                    WindowFunctionType::NthValue => {
                        let n = if args.len() > 1 {
                            match evaluate_value_expression(
                                &args[1],
                                columns,
                                rows[sorted_indices[0]],
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
                                    evaluate_value_expression(&args[0], columns, rows[source_idx])
                                        .unwrap_or(Value::Null)
                                } else {
                                    Value::Null
                                };
                                outputs[idx].push(val);
                            } else {
                                outputs[idx].push(Value::Null);
                            }
                        }
                    }
                    WindowFunctionType::PercentRank => {
                        let total = sorted_indices.len();
                        if total <= 1 {
                            for &idx in &sorted_indices {
                                outputs[idx].push(Value::Float(0.0));
                            }
                        } else {
                            let mut current_rank = 1i64;
                            for (i, &idx) in sorted_indices.iter().enumerate() {
                                if i > 0 {
                                    let prev_idx = sorted_indices[i - 1];
                                    if !window_order_values_equal(&order_values, prev_idx, idx) {
                                        current_rank = i as i64 + 1;
                                    }
                                }
                                let pct = (current_rank - 1) as f64 / (total - 1) as f64;
                                outputs[idx].push(Value::Float(pct));
                            }
                        }
                    }
                    WindowFunctionType::CumeDist => {
                        let total = sorted_indices.len();
                        let mut i = 0;
                        while i < sorted_indices.len() {
                            let mut j = i + 1;
                            while j < sorted_indices.len() {
                                if !window_order_values_equal(
                                    &order_values,
                                    sorted_indices[i],
                                    sorted_indices[j],
                                ) {
                                    break;
                                }
                                j += 1;
                            }
                            let cd = j as f64 / total as f64;
                            for k in i..j {
                                outputs[sorted_indices[k]].push(Value::Float(cd));
                            }
                            i = j;
                        }
                    }
                }
            }
        }
    }
    Ok(outputs)
}

fn compare_window_order_rows(
    order_values: &[Vec<Value>],
    order_by: &[OrderByExpr],
    left_row: usize,
    right_row: usize,
) -> Ordering {
    for (idx, order_expr) in order_by.iter().enumerate() {
        let cmp =
            compare_values_for_sort(&order_values[left_row][idx], &order_values[right_row][idx]);
        if cmp != Ordering::Equal {
            return if order_expr.asc { cmp } else { cmp.reverse() };
        }
    }
    Ordering::Equal
}

fn window_order_values_equal(
    order_values: &[Vec<Value>],
    left_row: usize,
    right_row: usize,
) -> bool {
    order_values[left_row] == order_values[right_row]
}

pub(crate) fn evaluate_window_functions(
    rows: &mut [Vec<Value>],
    columns: &[ColumnDefinition],
    select_columns: &[Column],
) -> Result<(), RustqlError> {
    let row_refs: Vec<&Vec<Value>> = rows.iter().collect();
    let outputs = evaluate_window_function_outputs(&row_refs, columns, select_columns)?;
    for (row, extra_values) in rows.iter_mut().zip(outputs) {
        row.extend(extra_values);
    }
    Ok(())
}
