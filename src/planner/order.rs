use super::*;

impl<'a> QueryPlanner<'a> {
    pub(super) fn resolve_order_by_aliases(
        &self,
        stmt: &SelectStatement,
        order_by: &[OrderByExpr],
    ) -> Result<Vec<OrderByExpr>, RustqlError> {
        order_by
            .iter()
            .map(|item| {
                Ok(OrderByExpr {
                    expr: self.resolve_order_by_alias(stmt, &item.expr)?,
                    asc: item.asc,
                    nulls_first: item.nulls_first,
                })
            })
            .collect()
    }

    pub(super) fn resolve_order_by_alias(
        &self,
        stmt: &SelectStatement,
        expr: &Expression,
    ) -> Result<Expression, RustqlError> {
        self.resolve_order_by_expression(stmt, expr, true)
    }

    pub(super) fn resolve_order_by_expression(
        &self,
        stmt: &SelectStatement,
        expr: &Expression,
        allow_ordinal: bool,
    ) -> Result<Expression, RustqlError> {
        if allow_ordinal && let Expression::Value(Value::Integer(position)) = expr {
            if *position < 1 || *position as usize > self.select_ordinal_count(stmt)? {
                return Err(RustqlError::ParseError(format!(
                    "ORDER BY position {} is not in select list",
                    position
                )));
            }

            if let Some(resolved) = self.select_ordinal_expression(stmt, *position) {
                return Ok(resolved);
            }
        }

        if let Expression::Column(name) = expr
            && let Some(resolved) = self.select_alias_expression(stmt, name)
        {
            return Ok(resolved);
        }

        Ok(match expr {
            Expression::BinaryOp { left, op, right } => Expression::BinaryOp {
                left: Box::new(self.resolve_order_by_expression(stmt, left, false)?),
                op: op.clone(),
                right: Box::new(self.resolve_order_by_expression(stmt, right, false)?),
            },
            Expression::UnaryOp { op, expr } => Expression::UnaryOp {
                op: op.clone(),
                expr: Box::new(self.resolve_order_by_expression(stmt, expr, false)?),
            },
            Expression::In { left, values } => Expression::In {
                left: Box::new(self.resolve_order_by_expression(stmt, left, false)?),
                values: values
                    .iter()
                    .map(|value| self.resolve_order_by_expression(stmt, value, false))
                    .collect::<Result<Vec<_>, _>>()?,
            },
            Expression::IsNull { expr, not } => Expression::IsNull {
                expr: Box::new(self.resolve_order_by_expression(stmt, expr, false)?),
                not: *not,
            },
            Expression::Any { left, op, subquery } => Expression::Any {
                left: Box::new(self.resolve_order_by_expression(stmt, left, false)?),
                op: op.clone(),
                subquery: subquery.clone(),
            },
            Expression::All { left, op, subquery } => Expression::All {
                left: Box::new(self.resolve_order_by_expression(stmt, left, false)?),
                op: op.clone(),
                subquery: subquery.clone(),
            },
            Expression::ScalarFunction { name, args } => Expression::ScalarFunction {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| self.resolve_order_by_expression(stmt, arg, false))
                    .collect::<Result<Vec<_>, _>>()?,
            },
            Expression::Cast { expr, data_type } => Expression::Cast {
                expr: Box::new(self.resolve_order_by_expression(stmt, expr, false)?),
                data_type: data_type.clone(),
            },
            Expression::Case {
                operand,
                when_clauses,
                else_clause,
            } => Expression::Case {
                operand: operand
                    .as_ref()
                    .map(|expr| {
                        self.resolve_order_by_expression(stmt, expr, false)
                            .map(Box::new)
                    })
                    .transpose()?,
                when_clauses: when_clauses
                    .iter()
                    .map(|(condition, result)| {
                        Ok((
                            self.resolve_order_by_expression(stmt, condition, false)?,
                            self.resolve_order_by_expression(stmt, result, false)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, RustqlError>>()?,
                else_clause: else_clause
                    .as_ref()
                    .map(|expr| {
                        self.resolve_order_by_expression(stmt, expr, false)
                            .map(Box::new)
                    })
                    .transpose()?,
            },
            Expression::IsDistinctFrom { left, right, not } => Expression::IsDistinctFrom {
                left: Box::new(self.resolve_order_by_expression(stmt, left, false)?),
                right: Box::new(self.resolve_order_by_expression(stmt, right, false)?),
                not: *not,
            },
            _ => expr.clone(),
        })
    }

    pub(super) fn select_ordinal_expression(
        &self,
        stmt: &SelectStatement,
        position: i64,
    ) -> Option<Expression> {
        if position < 1 || matches!(stmt.columns.first(), Some(Column::All)) {
            return None;
        }

        match stmt.columns.get(position as usize - 1)? {
            Column::Named { name, .. } => Some(Expression::Column(name.clone())),
            Column::Expression { expr, .. } => Some(expr.clone()),
            Column::Function(agg) => Some(Expression::Function(agg.clone())),
            Column::All | Column::Subquery(_) => None,
        }
    }

    fn select_ordinal_count(&self, stmt: &SelectStatement) -> Result<usize, RustqlError> {
        if matches!(stmt.columns.first(), Some(Column::All)) {
            return Ok(self.infer_select_output_columns(stmt)?.len());
        }

        Ok(stmt.columns.len())
    }

    pub(super) fn select_alias_expression(
        &self,
        stmt: &SelectStatement,
        name: &str,
    ) -> Option<Expression> {
        stmt.columns.iter().find_map(|column| match column {
            Column::Named {
                name: column_name,
                alias: Some(alias),
            } if alias == name => Some(Expression::Column(column_name.clone())),
            Column::Expression {
                expr,
                alias: Some(alias),
            } if alias == name => Some(expr.clone()),
            Column::Function(agg) if agg.alias.as_deref() == Some(name) => {
                Some(Expression::Function(agg.clone()))
            }
            _ => None,
        })
    }
}
