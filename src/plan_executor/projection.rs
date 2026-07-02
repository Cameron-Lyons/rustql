use super::*;

enum ProjectionValue {
    InputColumn(usize),
    Expression(Expression),
    AggregateColumn(usize),
    WindowColumn(usize),
    ScalarSubquery(Box<SelectStatement>),
}

struct ProjectionSpec {
    output_name: String,
    value: ProjectionValue,
}

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

        let projection_specs =
            build_projection_specs(&result.columns, &select_stmt.columns, aggregate_count)?;
        let scalar_outer_columns = scalar_outer_scope_columns(&result.columns, select_stmt);
        let mut projected_rows = Vec::with_capacity(result.rows.len());
        for (row_idx, row) in result.rows.iter().enumerate() {
            let mut projected_row = Vec::with_capacity(projection_specs.len());
            for spec in &projection_specs {
                let val = match &spec.value {
                    ProjectionValue::InputColumn(idx) | ProjectionValue::AggregateColumn(idx) => {
                        row.get(*idx).cloned().unwrap_or(Value::Null)
                    }
                    ProjectionValue::WindowColumn(idx) => window_rows
                        .as_ref()
                        .and_then(|rows| rows.get(row_idx))
                        .and_then(|window_row| window_row.get(*idx))
                        .cloned()
                        .unwrap_or(Value::Null),
                    ProjectionValue::Expression(expr) => {
                        self.evaluate_value_expression(expr, &column_defs, row)?
                    }
                    ProjectionValue::ScalarSubquery(subquery) => self.evaluate_scalar_subquery(
                        subquery.as_ref(),
                        &scalar_outer_columns,
                        row,
                    )?,
                };
                projected_row.push(val);
            }
            projected_rows.push(projected_row);
        }

        let projected_columns: Vec<String> = projection_specs
            .iter()
            .map(|spec| spec.output_name.clone())
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
        let mut seen = SqlRowSet::new();
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

fn build_projection_specs(
    result_columns: &[String],
    select_columns: &[Column],
    aggregate_count: usize,
) -> Result<Vec<ProjectionSpec>, RustqlError> {
    if matches!(select_columns.first(), Some(Column::All)) {
        return result_columns
            .iter()
            .enumerate()
            .map(|(idx, name)| {
                Ok(ProjectionSpec {
                    output_name: name.clone(),
                    value: ProjectionValue::InputColumn(idx),
                })
            })
            .collect();
    }

    let mut aggregate_offset = result_columns.len().saturating_sub(aggregate_count);
    let mut window_offset = result_columns.len();
    select_columns
        .iter()
        .map(|col| match col {
            Column::Named { name, alias } => {
                let idx = find_result_column_index(result_columns, name)
                    .ok_or_else(|| RustqlError::ColumnNotFound(name.to_string()))?;
                Ok(ProjectionSpec {
                    output_name: alias.clone().unwrap_or_else(|| name.clone()),
                    value: ProjectionValue::InputColumn(idx),
                })
            }
            Column::Expression { expr, alias } => {
                let value = if matches!(expr, Expression::WindowFunction { .. }) {
                    let idx = window_offset;
                    window_offset += 1;
                    ProjectionValue::WindowColumn(idx)
                } else {
                    ProjectionValue::Expression(expr.clone())
                };
                Ok(ProjectionSpec {
                    output_name: alias.clone().unwrap_or_else(|| "<expression>".to_string()),
                    value,
                })
            }
            Column::Function(agg) => {
                let idx = aggregate_offset;
                aggregate_offset += 1;
                Ok(ProjectionSpec {
                    output_name: crate::executor::aggregate::format_aggregate_header(agg),
                    value: ProjectionValue::AggregateColumn(idx),
                })
            }
            Column::Subquery(subquery) => Ok(ProjectionSpec {
                output_name: "<subquery>".to_string(),
                value: ProjectionValue::ScalarSubquery(subquery.clone()),
            }),
            Column::All => Err(RustqlError::Internal(
                "Wildcard projection must be expanded before plan projection".to_string(),
            )),
        })
        .collect()
}
