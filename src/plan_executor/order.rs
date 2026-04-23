use super::*;

impl<'a> PlanExecutor<'a> {
    pub(super) fn execute_sort(
        &self,
        input: ExecutionResult,
        order_by: &[OrderByExpr],
    ) -> Result<ExecutionResult, RustqlError> {
        let column_defs = column_definitions_from_names(&input.columns);
        let mut keyed_rows: Vec<(Vec<Value>, Vec<Value>)> = input
            .rows
            .into_iter()
            .map(|row| {
                let keys = order_by
                    .iter()
                    .map(|order_expr| {
                        self.get_sort_value(&order_expr.expr, &input.columns, &column_defs, &row)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((keys, row))
            })
            .collect::<Result<Vec<_>, RustqlError>>()?;

        keyed_rows.sort_by(|(a_keys, _), (b_keys, _)| {
            for (idx, order_expr) in order_by.iter().enumerate() {
                let a_val = a_keys.get(idx).unwrap_or(&Value::Null);
                let b_val = b_keys.get(idx).unwrap_or(&Value::Null);
                let cmp = compare_values_for_sort(a_val, b_val);
                if cmp != Ordering::Equal {
                    return if order_expr.asc { cmp } else { cmp.reverse() };
                }
            }
            Ordering::Equal
        });

        let rows = keyed_rows.into_iter().map(|(_, row)| row).collect();

        Ok(ExecutionResult {
            columns: input.columns,
            rows,
        })
    }

    pub(super) fn execute_limit(
        &self,
        input: ExecutionResult,
        limit: usize,
        offset: usize,
        with_ties: bool,
        order_by: &[OrderByExpr],
    ) -> Result<ExecutionResult, RustqlError> {
        let mut rows = input.rows;

        if offset < rows.len() {
            rows = rows.split_off(offset);
        } else {
            rows.clear();
        }

        if rows.len() > limit {
            let limit_with_ties = if with_ties && !order_by.is_empty() && limit > 0 {
                let column_defs = column_definitions_from_names(&input.columns);
                let boundary_values = self.extract_order_values(
                    order_by,
                    &input.columns,
                    &column_defs,
                    &rows[limit - 1],
                )?;
                let mut extended_limit = limit;
                while extended_limit < rows.len() {
                    let values = self.extract_order_values(
                        order_by,
                        &input.columns,
                        &column_defs,
                        &rows[extended_limit],
                    )?;
                    if values != boundary_values {
                        break;
                    }
                    extended_limit += 1;
                }
                extended_limit
            } else {
                limit
            };

            rows.truncate(limit_with_ties);
        }

        Ok(ExecutionResult {
            columns: input.columns,
            rows,
        })
    }

    pub(super) fn execute_distinct_on(
        &self,
        input: ExecutionResult,
        distinct_on: &[Expression],
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

        let mut seen = BTreeSet::new();
        let mut rows = Vec::new();

        for row in input.rows {
            let key: Vec<Value> = distinct_on
                .iter()
                .map(|expr| {
                    self.evaluate_distinct_on_value(expr, &input.columns, &column_defs, &row)
                })
                .collect::<Result<_, _>>()?;

            if seen.insert(key) {
                rows.push(row);
            }
        }

        Ok(ExecutionResult {
            columns: input.columns,
            rows,
        })
    }

    fn get_sort_value(
        &self,
        expr: &Expression,
        columns: &[String],
        column_defs: &[ColumnDefinition],
        row: &[Value],
    ) -> Result<Value, RustqlError> {
        match expr {
            Expression::Column(col) => self.lookup_sort_column_value(columns, row, col),
            Expression::Function(agg) => self.lookup_sort_aggregate_value(columns, row, agg),
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.get_sort_value(left, columns, column_defs, row)?;
                let right_val = self.get_sort_value(right, columns, column_defs, row)?;
                match op {
                    BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide => apply_arithmetic(&left_val, &right_val, op),
                    BinaryOperator::Concat => Ok(Value::Text(format!(
                        "{}{}",
                        format_value(&left_val),
                        format_value(&right_val)
                    ))),
                    _ => Err(RustqlError::Internal(
                        "Unsupported operator in ORDER BY".to_string(),
                    )),
                }
            }
            Expression::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } => match self.get_sort_value(expr, columns, column_defs, row)? {
                Value::Integer(value) => Ok(Value::Integer(-value)),
                Value::Float(value) => Ok(Value::Float(-value)),
                _ => Err(RustqlError::Internal(
                    "Unary minus only supported for numeric ORDER BY values".to_string(),
                )),
            },
            Expression::ScalarFunction { .. } | Expression::Cast { .. } => {
                let materialized = self.materialize_sort_expression(expr, columns, row)?;
                self.evaluate_value_expression(&materialized, column_defs, row)
            }
            Expression::Value(value) => Ok(value.clone()),
            _ => self.evaluate_value_expression(expr, column_defs, row),
        }
    }

    fn lookup_sort_column_value(
        &self,
        columns: &[String],
        row: &[Value],
        column_name: &str,
    ) -> Result<Value, RustqlError> {
        find_result_column_index(columns, column_name)
            .map(|idx| row.get(idx).cloned().unwrap_or(Value::Null))
            .ok_or_else(|| RustqlError::ColumnNotFound(column_name.to_string()))
    }

    fn lookup_sort_aggregate_value(
        &self,
        columns: &[String],
        row: &[Value],
        agg: &AggregateFunction,
    ) -> Result<Value, RustqlError> {
        find_aggregate_result_column_index(columns, agg)
            .map(|idx| row.get(idx).cloned().unwrap_or(Value::Null))
            .ok_or_else(|| {
                RustqlError::ColumnNotFound(format!("{} (ORDER BY)", format_aggregate_header(agg)))
            })
    }

    fn materialize_sort_expression(
        &self,
        expr: &Expression,
        columns: &[String],
        row: &[Value],
    ) -> Result<Expression, RustqlError> {
        match expr {
            Expression::Column(column_name) => Ok(Expression::Value(
                self.lookup_sort_column_value(columns, row, column_name)?,
            )),
            Expression::Function(agg) => Ok(Expression::Value(
                self.lookup_sort_aggregate_value(columns, row, agg)?,
            )),
            Expression::BinaryOp { left, op, right } => Ok(Expression::BinaryOp {
                left: Box::new(self.materialize_sort_expression(left, columns, row)?),
                op: op.clone(),
                right: Box::new(self.materialize_sort_expression(right, columns, row)?),
            }),
            Expression::UnaryOp { op, expr } => Ok(Expression::UnaryOp {
                op: op.clone(),
                expr: Box::new(self.materialize_sort_expression(expr, columns, row)?),
            }),
            Expression::ScalarFunction { name, args } => Ok(Expression::ScalarFunction {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| self.materialize_sort_expression(arg, columns, row))
                    .collect::<Result<_, _>>()?,
            }),
            Expression::Cast { expr, data_type } => Ok(Expression::Cast {
                expr: Box::new(self.materialize_sort_expression(expr, columns, row)?),
                data_type: data_type.clone(),
            }),
            _ => Ok(expr.clone()),
        }
    }

    fn evaluate_distinct_on_value(
        &self,
        expr: &Expression,
        columns: &[String],
        column_defs: &[ColumnDefinition],
        row: &[Value],
    ) -> Result<Value, RustqlError> {
        if let Expression::Column(column_name) = expr
            && let Some(idx) = find_result_column_index(columns, column_name)
        {
            return Ok(row.get(idx).cloned().unwrap_or(Value::Null));
        }

        self.evaluate_value_expression(expr, column_defs, row)
    }

    fn extract_order_values(
        &self,
        order_by: &[OrderByExpr],
        columns: &[String],
        column_defs: &[ColumnDefinition],
        row: &[Value],
    ) -> Result<Vec<Value>, RustqlError> {
        order_by
            .iter()
            .map(|order_expr| self.get_sort_value(&order_expr.expr, columns, column_defs, row))
            .collect()
    }
}
