use super::*;

impl<'a> QueryPlanner<'a> {
    pub(super) fn qualified_column_definitions(
        &self,
        columns: &[ColumnDefinition],
        label: &str,
    ) -> Vec<ColumnDefinition> {
        columns
            .iter()
            .map(|column| {
                let mut qualified = column.clone();
                qualified.name = format!("{}.{}", label, column.name);
                qualified
            })
            .collect()
    }

    pub(super) fn qualified_column_names(
        &self,
        columns: &[ColumnDefinition],
        label: &str,
    ) -> Vec<String> {
        columns
            .iter()
            .map(|column| format!("{}.{}", label, column.name))
            .collect()
    }

    pub(super) fn unqualified_column_name<'b>(&self, name: &'b str) -> &'b str {
        name.split('.').next_back().unwrap_or(name)
    }

    pub(super) fn extract_conjuncts(&self, expr: &Expression) -> Vec<Expression> {
        match expr {
            Expression::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let mut result = self.extract_conjuncts(left);
                result.extend(self.extract_conjuncts(right));
                result
            }
            other => vec![other.clone()],
        }
    }

    pub(super) fn referenced_tables(&self, expr: &Expression) -> HashSet<String> {
        let mut tables = HashSet::new();
        self.collect_table_refs(expr, &mut tables);
        tables
    }

    pub(super) fn collect_table_refs(&self, expr: &Expression, tables: &mut HashSet<String>) {
        match expr {
            Expression::Column(name) => {
                if let Some(dot_pos) = name.find('.') {
                    tables.insert(name[..dot_pos].to_string());
                }
            }
            Expression::BinaryOp { left, right, .. } => {
                self.collect_table_refs(left, tables);
                self.collect_table_refs(right, tables);
            }
            Expression::UnaryOp { expr, .. } => {
                self.collect_table_refs(expr, tables);
            }
            Expression::In { left, .. } => {
                self.collect_table_refs(left, tables);
            }
            Expression::IsNull { expr, .. } => {
                self.collect_table_refs(expr, tables);
            }
            Expression::Function(agg) => {
                self.collect_table_refs(&agg.expr, tables);
            }
            Expression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    self.collect_table_refs(op, tables);
                }
                for (cond, result) in when_clauses {
                    self.collect_table_refs(cond, tables);
                    self.collect_table_refs(result, tables);
                }
                if let Some(el) = else_clause {
                    self.collect_table_refs(el, tables);
                }
            }
            Expression::ScalarFunction { args, .. } => {
                for arg in args {
                    self.collect_table_refs(arg, tables);
                }
            }
            Expression::WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for arg in args {
                    self.collect_table_refs(arg, tables);
                }
                for expr in partition_by {
                    self.collect_table_refs(expr, tables);
                }
                for ob in order_by {
                    self.collect_table_refs(&ob.expr, tables);
                }
            }
            Expression::Cast { expr, .. } => {
                self.collect_table_refs(expr, tables);
            }
            Expression::Any { left, .. } | Expression::All { left, .. } => {
                self.collect_table_refs(left, tables);
            }
            Expression::IsDistinctFrom { left, right, .. } => {
                self.collect_table_refs(left, tables);
                self.collect_table_refs(right, tables);
            }
            Expression::Subquery(_) | Expression::Exists(_) | Expression::Value(_) => {}
        }
    }

    pub(super) fn base_column_names(
        &self,
        stmt: &SelectStatement,
    ) -> Result<HashSet<String>, RustqlError> {
        Ok(self
            .infer_base_source_columns(stmt)?
            .into_iter()
            .map(|column| self.unqualified_column_name(&column.name).to_string())
            .collect())
    }

    pub(super) fn unqualified_columns_resolve_to_base(
        &self,
        expr: &Expression,
        base_column_names: &HashSet<String>,
    ) -> bool {
        let mut columns = Vec::new();
        self.collect_unqualified_column_refs(expr, &mut columns);
        columns
            .iter()
            .all(|column| column == "*" || base_column_names.contains(column))
    }

    pub(super) fn collect_unqualified_column_refs(
        &self,
        expr: &Expression,
        columns: &mut Vec<String>,
    ) {
        match expr {
            Expression::Column(name) => {
                if !name.contains('.') {
                    columns.push(name.clone());
                }
            }
            Expression::BinaryOp { left, right, .. } => {
                self.collect_unqualified_column_refs(left, columns);
                self.collect_unqualified_column_refs(right, columns);
            }
            Expression::UnaryOp { expr, .. } | Expression::IsNull { expr, .. } => {
                self.collect_unqualified_column_refs(expr, columns);
            }
            Expression::In { left, .. } => {
                self.collect_unqualified_column_refs(left, columns);
            }
            Expression::Function(agg) => {
                self.collect_unqualified_column_refs(&agg.expr, columns);
                if let Some(filter) = agg.filter.as_deref() {
                    self.collect_unqualified_column_refs(filter, columns);
                }
            }
            Expression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(operand) = operand {
                    self.collect_unqualified_column_refs(operand, columns);
                }
                for (condition, result) in when_clauses {
                    self.collect_unqualified_column_refs(condition, columns);
                    self.collect_unqualified_column_refs(result, columns);
                }
                if let Some(else_clause) = else_clause {
                    self.collect_unqualified_column_refs(else_clause, columns);
                }
            }
            Expression::ScalarFunction { args, .. } => {
                for arg in args {
                    self.collect_unqualified_column_refs(arg, columns);
                }
            }
            Expression::WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for arg in args {
                    self.collect_unqualified_column_refs(arg, columns);
                }
                for expr in partition_by {
                    self.collect_unqualified_column_refs(expr, columns);
                }
                for order_expr in order_by {
                    self.collect_unqualified_column_refs(&order_expr.expr, columns);
                }
            }
            Expression::Cast { expr, .. } => {
                self.collect_unqualified_column_refs(expr, columns);
            }
            Expression::Any { left, .. } | Expression::All { left, .. } => {
                self.collect_unqualified_column_refs(left, columns);
            }
            Expression::IsDistinctFrom { left, right, .. } => {
                self.collect_unqualified_column_refs(left, columns);
                self.collect_unqualified_column_refs(right, columns);
            }
            Expression::Subquery(_) | Expression::Exists(_) | Expression::Value(_) => {}
        }
    }

    pub(super) fn combine_conjuncts(&self, exprs: Vec<Expression>) -> Option<Expression> {
        let mut iter = exprs.into_iter();
        let first = iter.next()?;
        Some(iter.fold(first, |acc, e| Expression::BinaryOp {
            left: Box::new(acc),
            op: BinaryOperator::And,
            right: Box::new(e),
        }))
    }
}
