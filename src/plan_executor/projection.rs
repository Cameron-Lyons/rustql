use super::*;

impl<'a> PlanExecutor<'a> {
    pub(super) fn apply_projection(
        &self,
        result: &ExecutionResult,
        select_stmt: &SelectStatement,
    ) -> Result<ExecutionResult, RustqlError> {
        let column_defs: Vec<ColumnDefinition> = result
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

        let column_specs: Vec<(String, Column)> =
            if matches!(select_stmt.columns.first(), Some(Column::All)) {
                result
                    .columns
                    .iter()
                    .map(|c| {
                        (
                            c.clone(),
                            Column::Named {
                                name: c.clone(),
                                alias: None,
                            },
                        )
                    })
                    .collect()
            } else {
                select_stmt
                    .columns
                    .iter()
                    .map(|col| match col {
                        Column::Named { name, alias } => {
                            Ok((alias.clone().unwrap_or_else(|| name.clone()), col.clone()))
                        }
                        Column::Expression { alias, .. } => Ok((
                            alias.clone().unwrap_or_else(|| "<expression>".to_string()),
                            col.clone(),
                        )),
                        Column::Function(agg) => Ok((
                            crate::executor::aggregate::format_aggregate_header(agg),
                            col.clone(),
                        )),
                        Column::Subquery(_) => Ok(("<subquery>".to_string(), col.clone())),
                        Column::All => Err(RustqlError::Internal(
                            "Wildcard projection must be expanded before plan projection"
                                .to_string(),
                        )),
                    })
                    .collect::<Result<Vec<_>, RustqlError>>()?
            };

        let aggregate_count = select_stmt
            .columns
            .iter()
            .filter(|col| matches!(col, Column::Function(_)))
            .count();
        let has_window_functions = select_stmt.columns.iter().any(|column| {
            matches!(
                column,
                Column::Expression {
                    expr: Expression::WindowFunction { .. },
                    ..
                }
            )
        });

        for (_, col) in &column_specs {
            if let Column::Named { name, .. } = col {
                find_result_column_index(&result.columns, name)
                    .ok_or_else(|| RustqlError::ColumnNotFound(name.to_string()))?;
            }
        }

        let window_rows = if has_window_functions {
            let mut rows = result.rows.clone();
            crate::executor::aggregate::evaluate_window_functions(
                &mut rows,
                &column_defs,
                &select_stmt.columns,
            )?;
            Some(rows)
        } else {
            None
        };

        let scalar_outer_columns = scalar_outer_scope_columns(&result.columns, select_stmt);
        let mut projected_rows = Vec::new();
        for (row_idx, row) in result.rows.iter().enumerate() {
            let mut projected_row = Vec::new();
            let mut aggregate_offset = result.columns.len().saturating_sub(aggregate_count);
            let mut window_offset = result.columns.len();
            for (_, col) in &column_specs {
                let val = match col {
                    Column::All => {
                        return Err(RustqlError::Internal(
                            "Wildcard projection must be expanded before plan projection"
                                .to_string(),
                        ));
                    }
                    Column::Named { name, .. } => {
                        let idx = find_result_column_index(&result.columns, name)
                            .ok_or_else(|| RustqlError::ColumnNotFound(name.to_string()))?;
                        row.get(idx).cloned().unwrap_or(Value::Null)
                    }
                    Column::Expression { expr, .. } => match expr {
                        Expression::WindowFunction { .. } => window_rows
                            .as_ref()
                            .and_then(|rows| rows.get(row_idx))
                            .and_then(|window_row| window_row.get(window_offset))
                            .cloned()
                            .inspect(|_| {
                                window_offset += 1;
                            })
                            .unwrap_or(Value::Null),
                        _ => self.evaluate_value_expression(expr, &column_defs, row)?,
                    },
                    Column::Function(_) => {
                        let value = row.get(aggregate_offset).cloned().unwrap_or(Value::Null);
                        aggregate_offset += 1;
                        value
                    }
                    Column::Subquery(subquery) => {
                        self.evaluate_scalar_subquery(subquery, &scalar_outer_columns, row)?
                    }
                };
                projected_row.push(val);
            }
            projected_rows.push(projected_row);
        }

        let projected_columns: Vec<String> =
            column_specs.iter().map(|(name, _)| name.clone()).collect();

        Ok(ExecutionResult {
            columns: projected_columns,
            rows: projected_rows,
        })
    }

    fn evaluate_scalar_subquery(
        &self,
        subquery: &SelectStatement,
        outer_columns: &[ColumnDefinition],
        outer_row: &[Value],
    ) -> Result<Value, RustqlError> {
        evaluate_planned_scalar_subquery_with_outer(self.db, subquery, outer_columns, outer_row)
    }

    pub(super) fn apply_distinct(
        &self,
        input: ExecutionResult,
    ) -> Result<ExecutionResult, RustqlError> {
        use std::collections::BTreeSet;
        let mut seen = BTreeSet::new();
        let mut unique_rows = Vec::new();

        for row in input.rows {
            if seen.insert(row.clone()) {
                unique_rows.push(row);
            }
        }

        Ok(ExecutionResult {
            columns: input.columns,
            rows: unique_rows,
        })
    }
}
