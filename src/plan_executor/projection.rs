use super::*;

struct ProjectionSpec<'a> {
    header: String,
    value: ProjectionValue<'a>,
}

enum ProjectionValue<'a> {
    Named { idx: usize },
    Expression { expr: &'a Expression },
    Window { offset: usize },
    Function { offset: usize },
    Subquery { subquery: &'a SelectStatement },
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

        let projection_specs = projection_specs(
            select_stmt,
            &result.columns,
            aggregate_count,
            has_window_functions,
        )?;

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
        let mut projected_rows = Vec::with_capacity(result.rows.len());
        for (row_idx, row) in result.rows.iter().enumerate() {
            let mut projected_row = Vec::with_capacity(projection_specs.len());
            for spec in &projection_specs {
                let val = match &spec.value {
                    ProjectionValue::Named { idx } => row.get(*idx).cloned().unwrap_or(Value::Null),
                    ProjectionValue::Expression { expr } => {
                        self.evaluate_value_expression(expr, &column_defs, row)?
                    }
                    ProjectionValue::Window { offset } => window_rows
                        .as_ref()
                        .and_then(|rows| rows.get(row_idx))
                        .and_then(|window_row| window_row.get(*offset))
                        .cloned()
                        .unwrap_or(Value::Null),
                    ProjectionValue::Function { offset } => {
                        row.get(*offset).cloned().unwrap_or(Value::Null)
                    }
                    ProjectionValue::Subquery { subquery } => {
                        self.evaluate_scalar_subquery(subquery, &scalar_outer_columns, row)?
                    }
                };
                projected_row.push(val);
            }
            projected_rows.push(projected_row);
        }

        let projected_columns: Vec<String> = projection_specs
            .iter()
            .map(|spec| spec.header.clone())
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
        let mut unique_rows = Vec::with_capacity(input.rows.len());

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

fn projection_specs<'a>(
    select_stmt: &'a SelectStatement,
    result_columns: &[String],
    aggregate_count: usize,
    has_window_functions: bool,
) -> Result<Vec<ProjectionSpec<'a>>, RustqlError> {
    if matches!(select_stmt.columns.first(), Some(Column::All)) {
        return result_columns
            .iter()
            .enumerate()
            .map(|(idx, header)| {
                Ok(ProjectionSpec {
                    header: header.clone(),
                    value: ProjectionValue::Named { idx },
                })
            })
            .collect();
    }

    let mut aggregate_offset = result_columns.len().saturating_sub(aggregate_count);
    let mut window_offset = result_columns.len();
    let mut specs = Vec::with_capacity(select_stmt.columns.len());

    for col in &select_stmt.columns {
        let spec = match col {
            Column::Named { name, alias } => {
                let idx = find_result_column_index(result_columns, name)
                    .ok_or_else(|| RustqlError::ColumnNotFound(name.to_string()))?;
                ProjectionSpec {
                    header: alias.clone().unwrap_or_else(|| name.clone()),
                    value: ProjectionValue::Named { idx },
                }
            }
            Column::Expression { expr, alias } => {
                let header = alias.clone().unwrap_or_else(|| "<expression>".to_string());
                if has_window_functions && matches!(expr, Expression::WindowFunction { .. }) {
                    let offset = window_offset;
                    window_offset += 1;
                    ProjectionSpec {
                        header,
                        value: ProjectionValue::Window { offset },
                    }
                } else {
                    ProjectionSpec {
                        header,
                        value: ProjectionValue::Expression { expr },
                    }
                }
            }
            Column::Function(agg) => {
                let offset = aggregate_offset;
                aggregate_offset += 1;
                ProjectionSpec {
                    header: crate::executor::aggregate::format_aggregate_header(agg),
                    value: ProjectionValue::Function { offset },
                }
            }
            Column::Subquery(subquery) => ProjectionSpec {
                header: "<subquery>".to_string(),
                value: ProjectionValue::Subquery { subquery },
            },
            Column::All => {
                return Err(RustqlError::Internal(
                    "Wildcard projection must be expanded before plan projection".to_string(),
                ));
            }
        };
        specs.push(spec);
    }

    Ok(specs)
}
