use super::*;

enum ProjectionSpec<'a> {
    Named {
        output_name: String,
        source_index: usize,
    },
    Expression {
        output_name: String,
        expr: &'a Expression,
    },
    Window {
        output_name: String,
        source_index: usize,
    },
    Function {
        output_name: String,
        source_index: usize,
    },
    Subquery {
        output_name: String,
        subquery: &'a SelectStatement,
    },
}

impl ProjectionSpec<'_> {
    fn output_name(&self) -> &str {
        match self {
            ProjectionSpec::Named { output_name, .. }
            | ProjectionSpec::Expression { output_name, .. }
            | ProjectionSpec::Window { output_name, .. }
            | ProjectionSpec::Function { output_name, .. }
            | ProjectionSpec::Subquery { output_name, .. } => output_name,
        }
    }
}

impl<'a> PlanExecutor<'a> {
    pub(super) fn apply_projection(
        &self,
        result: &ExecutionResult,
        select_stmt: &SelectStatement,
    ) -> Result<ExecutionResult, RustqlError> {
        let column_defs = column_definitions_from_names(&result.columns);
        let aggregate_count = select_stmt
            .columns
            .iter()
            .filter(|col| matches!(col, Column::Function(_)))
            .count();
        let aggregate_base = result.columns.len().saturating_sub(aggregate_count);
        let mut aggregate_offset = 0usize;
        let mut window_offset = 0usize;

        let projection_specs: Vec<ProjectionSpec<'_>> =
            if matches!(select_stmt.columns.first(), Some(Column::All)) {
                result
                    .columns
                    .iter()
                    .enumerate()
                    .map(|(source_index, output_name)| ProjectionSpec::Named {
                        output_name: output_name.clone(),
                        source_index,
                    })
                    .collect()
            } else {
                select_stmt
                    .columns
                    .iter()
                    .map(|col| match col {
                        Column::Named { name, alias } => {
                            let source_index = find_result_column_index(&result.columns, name)
                                .ok_or_else(|| RustqlError::ColumnNotFound(name.to_string()))?;
                            Ok(ProjectionSpec::Named {
                                output_name: alias.clone().unwrap_or_else(|| name.clone()),
                                source_index,
                            })
                        }
                        Column::Expression { expr, alias } => {
                            let output_name =
                                alias.clone().unwrap_or_else(|| "<expression>".to_string());
                            if matches!(expr, Expression::WindowFunction { .. }) {
                                let source_index = result.columns.len() + window_offset;
                                window_offset += 1;
                                Ok(ProjectionSpec::Window {
                                    output_name,
                                    source_index,
                                })
                            } else {
                                Ok(ProjectionSpec::Expression { output_name, expr })
                            }
                        }
                        Column::Function(agg) => {
                            let source_index = aggregate_base + aggregate_offset;
                            aggregate_offset += 1;
                            Ok(ProjectionSpec::Function {
                                output_name: crate::executor::aggregate::format_aggregate_header(
                                    agg,
                                ),
                                source_index,
                            })
                        }
                        Column::Subquery(subquery) => Ok(ProjectionSpec::Subquery {
                            output_name: "<subquery>".to_string(),
                            subquery,
                        }),
                        Column::All => Err(RustqlError::Internal(
                            "Wildcard projection must be expanded before plan projection"
                                .to_string(),
                        )),
                    })
                    .collect::<Result<Vec<_>, RustqlError>>()?
            };

        let has_window_functions = projection_specs
            .iter()
            .any(|spec| matches!(spec, ProjectionSpec::Window { .. }));
        let has_scalar_subqueries = projection_specs
            .iter()
            .any(|spec| matches!(spec, ProjectionSpec::Subquery { .. }));

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

        let scalar_outer_columns = if has_scalar_subqueries {
            scalar_outer_scope_columns(&result.columns, select_stmt)
        } else {
            Vec::new()
        };

        let mut projected_rows = Vec::with_capacity(result.rows.len());
        for (row_idx, row) in result.rows.iter().enumerate() {
            let mut projected_row = Vec::with_capacity(projection_specs.len());
            for spec in &projection_specs {
                let val = match spec {
                    ProjectionSpec::Named { source_index, .. }
                    | ProjectionSpec::Function { source_index, .. } => {
                        row.get(*source_index).cloned().unwrap_or(Value::Null)
                    }
                    ProjectionSpec::Window { source_index, .. } => window_rows
                        .as_ref()
                        .and_then(|rows| rows.get(row_idx))
                        .and_then(|window_row| window_row.get(*source_index))
                        .cloned()
                        .unwrap_or(Value::Null),
                    ProjectionSpec::Expression { expr, .. } => {
                        self.evaluate_value_expression(expr, &column_defs, row)?
                    }
                    ProjectionSpec::Subquery { subquery, .. } => {
                        self.evaluate_scalar_subquery(subquery, &scalar_outer_columns, row)?
                    }
                };
                projected_row.push(val);
            }
            projected_rows.push(projected_row);
        }

        let projected_columns: Vec<String> = projection_specs
            .iter()
            .map(|spec| spec.output_name().to_string())
            .collect();

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
