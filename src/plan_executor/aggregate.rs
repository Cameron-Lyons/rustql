use super::*;

struct PreparedAggregateInput {
    count_star: bool,
    filtered_row_count: usize,
    values: Vec<Value>,
}

struct HavingContext<'a> {
    result_columns: &'a [ColumnDefinition],
    result_row: &'a [Value],
    input_columns: &'a [ColumnDefinition],
    selected_aggregates: &'a [AggregateFunction],
    aggregate_values: &'a [Value],
    group_rows: &'a [&'a [Value]],
}

impl<'a> PlanExecutor<'a> {
    pub(super) fn execute_aggregate(
        &self,
        input: ExecutionResult,
        group_by: &[Expression],
        grouping_sets: Option<&[Vec<Expression>]>,
        aggregates: &[AggregateFunction],
        having: Option<&Expression>,
    ) -> Result<ExecutionResult, RustqlError> {
        let column_defs: Vec<ColumnDefinition> = input
            .columns
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

        let group_by_names: Vec<String> = group_by
            .iter()
            .map(|expr| match expr {
                Expression::Column(name) => name.clone(),
                _ => format!("{:?}", expr),
            })
            .collect();
        let aggregate_names: Vec<String> = aggregates
            .iter()
            .map(|agg| format!("{:?}", agg.function))
            .collect();

        let result_column_defs: Vec<ColumnDefinition> = group_by_names
            .iter()
            .chain(aggregate_names.iter())
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

        let mut result_rows = Vec::new();
        let build_groups = |exprs: &[Expression]| -> BTreeMap<Vec<Value>, Vec<&[Value]>> {
            let mut groups: BTreeMap<Vec<Value>, Vec<&[Value]>> = BTreeMap::new();
            for row in &input.rows {
                let key: Vec<Value> = exprs
                    .iter()
                    .map(|expr| {
                        self.evaluate_value_expression(expr, &column_defs, row)
                            .unwrap_or(Value::Null)
                    })
                    .collect();
                groups.entry(key).or_default().push(row.as_slice());
            }

            if groups.is_empty() && exprs.is_empty() {
                groups.insert(Vec::new(), Vec::new());
            }

            groups
        };

        if let Some(grouping_sets) = grouping_sets {
            for set in grouping_sets {
                let groups = build_groups(set);

                for group_rows in groups.into_values() {
                    let mut result_row = Vec::with_capacity(group_by.len() + aggregates.len());

                    for group_expr in group_by {
                        if set.iter().any(|active_expr| active_expr == group_expr) {
                            let value = group_rows
                                .first()
                                .map(|row| {
                                    self.evaluate_value_expression(group_expr, &column_defs, row)
                                })
                                .transpose()?
                                .unwrap_or(Value::Null);
                            result_row.push(value);
                        } else {
                            result_row.push(Value::Null);
                        }
                    }

                    let aggregate_values =
                        self.compute_group_aggregate_values(aggregates, &group_rows, &column_defs)?;
                    result_row.extend(aggregate_values.iter().cloned());

                    if let Some(having_expr) = having {
                        let having_context = HavingContext {
                            result_columns: &result_column_defs,
                            result_row: &result_row,
                            input_columns: &column_defs,
                            selected_aggregates: aggregates,
                            aggregate_values: &aggregate_values,
                            group_rows: &group_rows,
                        };
                        let include = self.evaluate_having(having_expr, &having_context)?;

                        if !include {
                            continue;
                        }
                    }

                    result_rows.push(result_row);
                }
            }
        } else {
            let groups = build_groups(group_by);

            for (group_key, group_rows) in groups {
                let mut result_row = group_key.clone();
                let aggregate_values =
                    self.compute_group_aggregate_values(aggregates, &group_rows, &column_defs)?;
                result_row.extend(aggregate_values.iter().cloned());

                if let Some(having_expr) = having {
                    let having_context = HavingContext {
                        result_columns: &result_column_defs,
                        result_row: &result_row,
                        input_columns: &column_defs,
                        selected_aggregates: aggregates,
                        aggregate_values: &aggregate_values,
                        group_rows: &group_rows,
                    };
                    let include = self.evaluate_having(having_expr, &having_context)?;

                    if !include {
                        continue;
                    }
                }

                result_rows.push(result_row);
            }
        }

        Ok(ExecutionResult {
            columns: group_by_names.into_iter().chain(aggregate_names).collect(),
            rows: result_rows,
        })
    }

    fn compute_group_aggregate_values(
        &self,
        aggregates: &[AggregateFunction],
        rows: &[&[Value]],
        columns: &[ColumnDefinition],
    ) -> Result<Vec<Value>, RustqlError> {
        let mut shared_inputs = vec![0usize; aggregates.len()];
        for (idx, aggregate) in aggregates.iter().enumerate() {
            shared_inputs[idx] = aggregates[..idx]
                .iter()
                .position(|candidate| aggregate_input_signature_matches(candidate, aggregate))
                .unwrap_or(idx);
        }

        let mut prepared_inputs: Vec<Option<PreparedAggregateInput>> = Vec::new();
        prepared_inputs.resize_with(aggregates.len(), || None);

        let mut values = Vec::with_capacity(aggregates.len());
        for (idx, aggregate) in aggregates.iter().enumerate() {
            let input_idx = shared_inputs[idx];
            if prepared_inputs[input_idx].is_none() {
                prepared_inputs[input_idx] =
                    Some(self.prepare_aggregate_input(&aggregates[input_idx], rows, columns)?);
            }
            let prepared_input = prepared_inputs[input_idx].as_ref().ok_or_else(|| {
                RustqlError::Internal("Aggregate input was not prepared".to_string())
            })?;
            values.push(self.compute_aggregate_from_input(aggregate, prepared_input)?);
        }

        Ok(values)
    }

    fn prepare_aggregate_input(
        &self,
        agg: &AggregateFunction,
        rows: &[&[Value]],
        columns: &[ColumnDefinition],
    ) -> Result<PreparedAggregateInput, RustqlError> {
        use std::collections::BTreeSet;

        let count_star = matches!(agg.expr.as_ref(), Expression::Column(name) if name == "*")
            && matches!(agg.function, AggregateFunctionType::Count);

        if count_star && agg.distinct {
            return Err(RustqlError::AggregateError(
                "COUNT(DISTINCT *) is not supported".to_string(),
            ));
        }

        let mut filtered_row_count = 0usize;
        let mut values = Vec::new();
        let mut seen = agg.distinct.then(BTreeSet::new);

        for row in rows {
            if let Some(filter_expr) = agg.filter.as_deref()
                && !self.evaluate_expression(filter_expr, columns, row)?
            {
                continue;
            }

            filtered_row_count += 1;
            if count_star {
                continue;
            }

            let value = self.evaluate_value_expression(&agg.expr, columns, row)?;
            if matches!(value, Value::Null) {
                continue;
            }

            if let Some(seen) = seen.as_mut()
                && !seen.insert(value.clone())
            {
                continue;
            }

            values.push(value);
        }

        Ok(PreparedAggregateInput {
            count_star,
            filtered_row_count,
            values,
        })
    }

    fn compute_aggregate_from_input(
        &self,
        agg: &AggregateFunction,
        input: &PreparedAggregateInput,
    ) -> Result<Value, RustqlError> {
        match agg.function {
            AggregateFunctionType::Count => {
                if input.count_star {
                    Ok(Value::Integer(input.filtered_row_count as i64))
                } else {
                    Ok(Value::Integer(input.values.len() as i64))
                }
            }
            AggregateFunctionType::Sum => {
                let mut sum = 0.0f64;
                let mut has_value = false;
                for value in &input.values {
                    match value {
                        Value::Integer(i) => {
                            sum += *i as f64;
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
                    }
                }
                if has_value {
                    Ok(Value::Float(sum))
                } else {
                    Ok(Value::Null)
                }
            }
            AggregateFunctionType::Avg => {
                let mut sum = 0.0f64;
                let mut count = 0i64;
                for value in &input.values {
                    match value {
                        Value::Integer(i) => {
                            sum += *i as f64;
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
                    }
                }
                if count > 0 {
                    Ok(Value::Float(sum / count as f64))
                } else {
                    Ok(Value::Null)
                }
            }
            AggregateFunctionType::Min => {
                let mut min_val: Option<Value> = None;
                for value in &input.values {
                    min_val = Some(match min_val {
                        None => value.clone(),
                        Some(current) => {
                            if compare_values_same_type(value, &current) == Ordering::Less {
                                value.clone()
                            } else {
                                current
                            }
                        }
                    });
                }
                Ok(min_val.unwrap_or(Value::Null))
            }
            AggregateFunctionType::Max => {
                let mut max_val: Option<Value> = None;
                for value in &input.values {
                    max_val = Some(match max_val {
                        None => value.clone(),
                        Some(current) => {
                            if compare_values_same_type(value, &current) == Ordering::Greater {
                                value.clone()
                            } else {
                                current
                            }
                        }
                    });
                }
                Ok(max_val.unwrap_or(Value::Null))
            }
            AggregateFunctionType::Variance => {
                let values = numeric_values(&input.values, "VARIANCE requires numeric values")?;
                if values.is_empty() {
                    return Ok(Value::Null);
                }
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance =
                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
                Ok(Value::Float(variance))
            }
            AggregateFunctionType::Stddev => {
                let values = numeric_values(&input.values, "STDDEV requires numeric values")?;
                if values.is_empty() {
                    return Ok(Value::Null);
                }
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance =
                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
                Ok(Value::Float(variance.sqrt()))
            }
            AggregateFunctionType::GroupConcat => {
                let sep = agg.separator.as_deref().unwrap_or(",");
                let parts: Vec<String> = input.values.iter().map(format_value_string).collect();
                if parts.is_empty() {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Text(parts.join(sep)))
                }
            }
            AggregateFunctionType::BoolAnd => {
                let mut result = true;
                let mut has_value = false;
                for value in &input.values {
                    has_value = true;
                    match value {
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
                for value in &input.values {
                    has_value = true;
                    match value {
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
                let mut values = numeric_values(&input.values, "MEDIAN requires numeric values")?;
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
                let mut counts: BTreeMap<Value, (usize, usize)> = BTreeMap::new();
                for (position, value) in input.values.iter().cloned().enumerate() {
                    let entry = counts.entry(value).or_insert((0, position));
                    entry.0 += 1;
                }
                if counts.is_empty() {
                    return Ok(Value::Null);
                }
                let mode = counts
                    .into_iter()
                    .max_by(|a, b| a.1.0.cmp(&b.1.0).then_with(|| b.1.1.cmp(&a.1.1)))
                    .map(|(value, _)| value)
                    .unwrap_or(Value::Null);
                Ok(mode)
            }
            AggregateFunctionType::PercentileCont => {
                let frac = agg.percentile.unwrap_or(DEFAULT_PERCENTILE_FRACTION);
                let mut values =
                    numeric_values(&input.values, "PERCENTILE_CONT requires numeric values")?;
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
                    let w = pos - lower as f64;
                    Ok(Value::Float(values[lower] * (1.0 - w) + values[upper] * w))
                }
            }
            AggregateFunctionType::PercentileDisc => {
                let frac = agg.percentile.unwrap_or(DEFAULT_PERCENTILE_FRACTION);
                let mut values = input.values.clone();
                if values.is_empty() {
                    return Ok(Value::Null);
                }
                values.sort();
                let idx = ((frac * values.len() as f64).ceil() as usize).saturating_sub(1);
                Ok(values[idx.min(values.len() - 1)].clone())
            }
        }
    }

    fn compute_aggregate(
        &self,
        agg: &AggregateFunction,
        rows: &[&[Value]],
        columns: &[ColumnDefinition],
    ) -> Result<Value, RustqlError> {
        let prepared = self.prepare_aggregate_input(agg, rows, columns)?;
        self.compute_aggregate_from_input(agg, &prepared)
    }

    fn evaluate_having(
        &self,
        expr: &Expression,
        context: &HavingContext<'_>,
    ) -> Result<bool, RustqlError> {
        match expr {
            Expression::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And => Ok(self.evaluate_having(left, context)?
                        && self.evaluate_having(right, context)?),
                    BinaryOperator::Or => Ok(self.evaluate_having(left, context)?
                        || self.evaluate_having(right, context)?),
                    _ => {
                        let left_val = self.evaluate_having_value(left, context)?;
                        let right_val = self.evaluate_having_value(right, context)?;
                        compare_values(&left_val, op, &right_val)
                    }
                }
            }
            Expression::IsNull { expr, not } => {
                let value = self.evaluate_having_value(expr, context)?;
                let is_null = matches!(value, Value::Null);
                Ok(if *not { !is_null } else { is_null })
            }
            Expression::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => Ok(!self.evaluate_having(expr, context)?),
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
        &self,
        expr: &Expression,
        context: &HavingContext<'_>,
    ) -> Result<Value, RustqlError> {
        match expr {
            Expression::Function(agg) => {
                if let Some(idx) = context
                    .selected_aggregates
                    .iter()
                    .position(|candidate| candidate == agg)
                {
                    Ok(context
                        .aggregate_values
                        .get(idx)
                        .cloned()
                        .unwrap_or(Value::Null))
                } else {
                    self.compute_aggregate(agg, context.group_rows, context.input_columns)
                }
            }
            Expression::Value(val) => Ok(val.clone()),
            Expression::Column(name) => {
                let col_name = if name.contains('.') {
                    name.split('.').next_back().unwrap_or(name)
                } else {
                    name.as_str()
                };
                let idx = context
                    .result_columns
                    .iter()
                    .position(|c| column_names_match(&c.name, col_name))
                    .ok_or_else(|| format!("Column '{}' not found in HAVING clause", name))?;
                Ok(context.result_row.get(idx).cloned().unwrap_or(Value::Null))
            }
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_having_value(left, context)?;
                let right_val = self.evaluate_having_value(right, context)?;
                match op {
                    BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide => apply_arithmetic(&left_val, &right_val, op),
                    _ => Err(RustqlError::Internal(
                        "Only arithmetic operators are supported in HAVING expressions".to_string(),
                    )),
                }
            }
            _ => Err(RustqlError::Internal(
                "Complex expressions not yet supported in HAVING".to_string(),
            )),
        }
    }
}

fn aggregate_input_signature_matches(left: &AggregateFunction, right: &AggregateFunction) -> bool {
    left.expr == right.expr && left.distinct == right.distinct && left.filter == right.filter
}

fn numeric_values(values: &[Value], error: &str) -> Result<Vec<f64>, RustqlError> {
    values
        .iter()
        .map(|value| match value {
            Value::Integer(i) => Ok(*i as f64),
            Value::Float(f) => Ok(*f),
            _ => Err(RustqlError::AggregateError(error.to_string())),
        })
        .collect()
}

fn format_value_string(value: &Value) -> String {
    match value {
        Value::Integer(n) => n.to_string(),
        Value::Float(f) => format!("{}", f),
        Value::Text(s) => s.clone(),
        Value::Boolean(b) => b.to_string(),
        Value::Date(d) => d.clone(),
        Value::Time(t) => t.clone(),
        Value::DateTime(dt) => dt.clone(),
        Value::Null => "NULL".to_string(),
    }
}
