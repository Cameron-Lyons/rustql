use super::*;

pub fn execute_select(
    context: &ExecutionContext,
    mut stmt: SelectStatement,
) -> Result<QueryResult, RustqlError> {
    resolve_window_definitions(&mut stmt);

    let db = get_database_read(context);
    Ok(rows_result(execute_select_internal(
        Some(context),
        stmt,
        &db,
    )?))
}

pub(crate) fn plan_select_for_execution(
    db: &dyn DatabaseCatalog,
    stmt: &SelectStatement,
) -> Result<planner::PlanNode, RustqlError> {
    planner::plan_query(db, stmt)
}

pub(crate) fn explain_select(
    context: &ExecutionContext,
    mut stmt: SelectStatement,
) -> Result<planner::PlanNode, RustqlError> {
    resolve_window_definitions(&mut stmt);

    let db = get_database_read(context);
    plan_select_for_execution(&db, &stmt)
}

pub(crate) fn execute_select_internal(
    _context: Option<&ExecutionContext>,
    stmt: SelectStatement,
    db: &dyn DatabaseCatalog,
) -> Result<SelectResult, RustqlError> {
    let plan = planner::plan_query(db, &stmt)?;
    let execution = PlanExecutor::new(db).execute(&plan, &stmt)?;
    Ok(SelectResult {
        headers: execution.columns,
        rows: execution.rows,
    })
}
