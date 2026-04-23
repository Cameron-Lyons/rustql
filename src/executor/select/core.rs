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

pub(crate) fn explain_select(
    context: &ExecutionContext,
    mut stmt: SelectStatement,
) -> Result<planner::PlanNode, RustqlError> {
    resolve_window_definitions(&mut stmt);

    let db = get_database_read(context);
    let bound = crate::binder::bind_select(&*db, &stmt)?;
    planner::plan_bound_query(&*db, &bound)
}

pub(crate) fn execute_select_internal(
    _context: Option<&ExecutionContext>,
    stmt: SelectStatement,
    db: &dyn DatabaseCatalog,
) -> Result<SelectResult, RustqlError> {
    let bound = crate::binder::bind_select(db, &stmt)?;
    let plan = planner::plan_bound_query(db, &bound)?;
    let execution = PlanExecutor::new(db).execute(&plan, &bound.statement)?;
    Ok(SelectResult {
        headers: execution.columns,
        rows: execution.rows,
    })
}
