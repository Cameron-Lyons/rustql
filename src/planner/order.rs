use super::*;

impl<'a> QueryPlanner<'a> {
    pub(super) fn resolve_order_by_aliases(
        &self,
        stmt: &SelectStatement,
        order_by: &[OrderByExpr],
    ) -> Vec<OrderByExpr> {
        order_by
            .iter()
            .map(|item| OrderByExpr {
                expr: self.resolve_order_by_alias(stmt, &item.expr),
                asc: item.asc,
            })
            .collect()
    }

    pub(super) fn resolve_order_by_alias(
        &self,
        stmt: &SelectStatement,
        expr: &Expression,
    ) -> Expression {
        self.resolve_order_by_expression(stmt, expr, true)
    }

    pub(super) fn resolve_order_by_expression(
        &self,
        stmt: &SelectStatement,
        expr: &Expression,
        allow_ordinal: bool,
    ) -> Expression {
        if allow_ordinal
            && let Expression::Value(Value::Integer(position)) = expr
            && let Some(resolved) = self.select_ordinal_expression(stmt, *position)
        {
            return resolved;
        }

        if let Expression::Column(name) = expr
            && let Some(resolved) = self.select_alias_expression(stmt, name)
        {
            return resolved;
        }

        match expr {
            Expression::BinaryOp { left, op, right } => Expression::BinaryOp {
                left: Box::new(self.resolve_order_by_expression(stmt, left, false)),
                op: op.clone(),
                right: Box::new(self.resolve_order_by_expression(stmt, right, false)),
            },
            Expression::UnaryOp { op, expr } => Expression::UnaryOp {
                op: op.clone(),
                expr: Box::new(self.resolve_order_by_expression(stmt, expr, false)),
            },
            Expression::ScalarFunction { name, args } => Expression::ScalarFunction {
                name: name.clone(),
                args: args
                    .iter()
                    .map(|arg| self.resolve_order_by_expression(stmt, arg, false))
                    .collect(),
            },
            Expression::Cast { expr, data_type } => Expression::Cast {
                expr: Box::new(self.resolve_order_by_expression(stmt, expr, false)),
                data_type: data_type.clone(),
            },
            Expression::Case {
                operand,
                when_clauses,
                else_clause,
            } => Expression::Case {
                operand: operand
                    .as_ref()
                    .map(|expr| Box::new(self.resolve_order_by_expression(stmt, expr, false))),
                when_clauses: when_clauses
                    .iter()
                    .map(|(condition, result)| {
                        (
                            self.resolve_order_by_expression(stmt, condition, false),
                            self.resolve_order_by_expression(stmt, result, false),
                        )
                    })
                    .collect(),
                else_clause: else_clause
                    .as_ref()
                    .map(|expr| Box::new(self.resolve_order_by_expression(stmt, expr, false))),
            },
            Expression::IsDistinctFrom { left, right, not } => Expression::IsDistinctFrom {
                left: Box::new(self.resolve_order_by_expression(stmt, left, false)),
                right: Box::new(self.resolve_order_by_expression(stmt, right, false)),
                not: *not,
            },
            _ => expr.clone(),
        }
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
