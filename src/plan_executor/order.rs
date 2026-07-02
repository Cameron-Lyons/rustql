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
                let cmp = compare_order_values(a_val, b_val, order_expr);
                if cmp != Ordering::Equal {
                    return cmp;
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

        if offset >= rows.len() {
            rows.clear();
        } else if offset > 0 {
            rows.drain(..offset);
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
                    if !order_values_equal(&values, &boundary_values) {
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

        let mut seen = SqlRowSet::new();
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
        let materialized = self.materialize_sort_expression(expr, columns, row)?;
        self.evaluate_value_expression(&materialized, column_defs, row)
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
            Expression::In { left, values } => Ok(Expression::In {
                left: Box::new(self.materialize_sort_expression(left, columns, row)?),
                values: values
                    .iter()
                    .map(|value| self.materialize_sort_expression(value, columns, row))
                    .collect::<Result<Vec<_>, _>>()?,
            }),
            Expression::IsNull { expr, not } => Ok(Expression::IsNull {
                expr: Box::new(self.materialize_sort_expression(expr, columns, row)?),
                not: *not,
            }),
            Expression::Any { left, op, subquery } => Ok(Expression::Any {
                left: Box::new(self.materialize_sort_expression(left, columns, row)?),
                op: op.clone(),
                subquery: subquery.clone(),
            }),
            Expression::All { left, op, subquery } => Ok(Expression::All {
                left: Box::new(self.materialize_sort_expression(left, columns, row)?),
                op: op.clone(),
                subquery: subquery.clone(),
            }),
            Expression::Case {
                operand,
                when_clauses,
                else_clause,
            } => Ok(Expression::Case {
                operand: operand
                    .as_ref()
                    .map(|expr| self.materialize_sort_expression(expr, columns, row))
                    .transpose()?
                    .map(Box::new),
                when_clauses: when_clauses
                    .iter()
                    .map(|(when_expr, then_expr)| {
                        Ok((
                            self.materialize_sort_expression(when_expr, columns, row)?,
                            self.materialize_sort_expression(then_expr, columns, row)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, RustqlError>>()?,
                else_clause: else_clause
                    .as_ref()
                    .map(|expr| self.materialize_sort_expression(expr, columns, row))
                    .transpose()?
                    .map(Box::new),
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
            Expression::IsDistinctFrom { left, right, not } => Ok(Expression::IsDistinctFrom {
                left: Box::new(self.materialize_sort_expression(left, columns, row)?),
                right: Box::new(self.materialize_sort_expression(right, columns, row)?),
                not: *not,
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

fn order_values_equal(left: &[Value], right: &[Value]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| compare_values_for_sort(left, right) == Ordering::Equal)
}
