use super::*;

pub fn execute_select(
    context: &ExecutionContext,
    mut stmt: SelectStatement,
) -> Result<QueryResult, RustqlError> {
    resolve_window_definitions(&mut stmt);

    {
        let db = get_database_read(context);
        if select_requires_source_materialization(&db, &stmt) {
            drop(db);
            return execute_select_with_materialized_sources(context, stmt);
        }
    }

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
    if !select_requires_source_materialization(&db, &stmt) {
        return plan_select_for_execution(&db, &stmt);
    }
    drop(db);

    let mut db = get_database_write(context);
    plan_select_in_db(Some(context), stmt, &mut db)
}

pub(super) fn execute_select_with_materialized_sources(
    context: &ExecutionContext,
    stmt: SelectStatement,
) -> Result<QueryResult, RustqlError> {
    let mut db = get_database_write(context);
    let result = execute_select_in_db(Some(context), stmt, &mut db)?;
    Ok(rows_result(result))
}

pub(super) fn execute_select_in_db(
    context: Option<&ExecutionContext>,
    mut stmt: SelectStatement,
    db: &mut Database,
) -> Result<SelectResult, RustqlError> {
    resolve_window_definitions(&mut stmt);

    if !select_requires_source_materialization(db, &stmt) {
        let plan = plan_select_for_execution(db, &stmt)?;
        let execution = PlanExecutor::new(db).execute(&plan, &stmt)?;
        return Ok(SelectResult {
            headers: execution.columns,
            rows: execution.rows,
        });
    }

    let mut temp_scope = TempTableScope::default();
    let result = (|| {
        let rewritten = rewrite_select_sources_for_scope(context, stmt, db, &mut temp_scope)?;
        execute_select_internal(context, rewritten, db)
    })();
    temp_scope.cleanup(db);
    result
}

pub(super) fn plan_select_in_db(
    context: Option<&ExecutionContext>,
    mut stmt: SelectStatement,
    db: &mut Database,
) -> Result<planner::PlanNode, RustqlError> {
    resolve_window_definitions(&mut stmt);

    let mut temp_scope = TempTableScope::default();
    let result = (|| {
        let rewritten = rewrite_select_sources_for_scope(context, stmt, db, &mut temp_scope)?;
        plan_select_for_execution(db, &rewritten)
    })();
    temp_scope.cleanup(db);
    result
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
