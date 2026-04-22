use super::*;

impl<'a> PlanExecutor<'a> {
    pub(super) fn execute_filter(
        &self,
        input: ExecutionResult,
        condition: &Expression,
    ) -> Result<ExecutionResult, RustqlError> {
        let mut filtered_rows = Vec::new();
        let columns = column_definitions_from_names(&input.columns);

        for row in &input.rows {
            let include = self.evaluate_expression(condition, &columns, row)?;
            if include {
                filtered_rows.push(row.clone());
            }
        }

        Ok(ExecutionResult {
            columns: input.columns,
            rows: filtered_rows,
        })
    }
}
