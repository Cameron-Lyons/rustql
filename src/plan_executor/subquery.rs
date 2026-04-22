use super::*;

pub(super) fn execute_planned_select(
    db: &dyn DatabaseCatalog,
    stmt: &SelectStatement,
) -> Result<ExecutionResult, RustqlError> {
    let plan = planner::plan_query(db, stmt)?;
    PlanExecutor::new(db).execute(&plan, stmt)
}

pub(crate) fn evaluate_planned_scalar_subquery_with_outer(
    db: &dyn DatabaseCatalog,
    subquery: &SelectStatement,
    outer_columns: &[ColumnDefinition],
    outer_row: &[Value],
) -> Result<Value, RustqlError> {
    if subquery.columns.len() != 1 {
        return Err(RustqlError::Internal(
            "Scalar subquery must select exactly one column".to_string(),
        ));
    }

    let result = execute_scoped_select(db, subquery, outer_columns, outer_row)?;
    if result.columns.len() != 1 {
        return Err(RustqlError::Internal(
            "Scalar subquery must return exactly one column".to_string(),
        ));
    }

    match result.rows.len() {
        0 => Ok(Value::Null),
        1 => result.rows[0].first().cloned().ok_or_else(|| {
            RustqlError::Internal("Scalar subquery must return exactly one column".to_string())
        }),
        _ => Err(RustqlError::Internal(
            "Scalar subquery returned more than one row".to_string(),
        )),
    }
}

pub(crate) fn evaluate_planned_subquery_values_with_outer(
    db: &dyn DatabaseCatalog,
    subquery: &SelectStatement,
    outer_columns: &[ColumnDefinition],
    outer_row: &[Value],
) -> Result<Vec<Value>, RustqlError> {
    if subquery.columns.len() != 1 {
        return Err(RustqlError::Internal(
            "Subquery in IN must select exactly one column".to_string(),
        ));
    }

    let result = execute_scoped_select(db, subquery, outer_columns, outer_row)?;
    if result.columns.len() != 1 {
        return Err(RustqlError::Internal(
            "Subquery in IN must return exactly one column".to_string(),
        ));
    }

    Ok(result
        .rows
        .into_iter()
        .map(|row| row.first().cloned().unwrap_or(Value::Null))
        .collect())
}

pub(crate) fn evaluate_planned_subquery_exists_with_outer(
    db: &dyn DatabaseCatalog,
    subquery: &SelectStatement,
    outer_columns: &[ColumnDefinition],
    outer_row: &[Value],
) -> Result<bool, RustqlError> {
    Ok(
        !execute_scoped_select(db, subquery, outer_columns, outer_row)?
            .rows
            .is_empty(),
    )
}

fn execute_scoped_select(
    db: &dyn DatabaseCatalog,
    subquery: &SelectStatement,
    outer_columns: &[ColumnDefinition],
    outer_row: &[Value],
) -> Result<ExecutionResult, RustqlError> {
    let local_columns = subquery_local_column_names(db, subquery);
    let needs_outer_scope = !outer_columns.is_empty()
        && subquery_needs_outer_scope(subquery, &local_columns, outer_columns);
    if !needs_outer_scope {
        return execute_planned_select(db, subquery);
    }

    let temp_table_name = "__lateral_outer_scalar".to_string();
    let (scoped_outer_columns, outer_column_mappings) = scoped_outer_columns(outer_columns);
    let rewritten_subquery = scoped_subquery_with_outer_scope(
        subquery,
        &temp_table_name,
        &outer_column_mappings,
        &local_columns,
    );
    let mut scoped_db = ScopedDatabase::new(db, temp_table_name, scoped_outer_columns);
    scoped_db.update_temp_row(outer_row);
    execute_planned_select(&scoped_db, &rewritten_subquery)
}

fn subquery_needs_outer_scope(
    subquery: &SelectStatement,
    local_columns: &HashSet<String>,
    outer_columns: &[ColumnDefinition],
) -> bool {
    subquery_expression_refs(subquery)
        .into_iter()
        .any(|expr| expression_needs_outer_scope(expr, local_columns, outer_columns))
}

fn subquery_expression_refs(subquery: &SelectStatement) -> Vec<&Expression> {
    let mut expressions = Vec::new();

    for column in &subquery.columns {
        match column {
            Column::Named { name, .. } => {
                // Treat SELECT-list names as local unless another expression proves correlation.
                let _ = name;
            }
            Column::Function(aggregate) => {
                expressions.push(aggregate.expr.as_ref());
                if let Some(filter) = aggregate.filter.as_deref() {
                    expressions.push(filter);
                }
            }
            Column::Expression { expr, .. } => expressions.push(expr),
            Column::All | Column::Subquery(_) => {}
        }
    }

    if let Some(where_clause) = subquery.where_clause.as_ref() {
        expressions.push(where_clause);
    }
    if let Some(group_by) = subquery.group_by.as_ref() {
        expressions.extend(group_by.exprs());
    }
    if let Some(having) = subquery.having.as_ref() {
        expressions.push(having);
    }
    if let Some(distinct_on) = subquery.distinct_on.as_ref() {
        expressions.extend(distinct_on);
    }
    if let Some(order_by) = subquery.order_by.as_ref() {
        expressions.extend(order_by.iter().map(|item| &item.expr));
    }
    for join in &subquery.joins {
        if let Some(on) = join.on.as_ref() {
            expressions.push(on);
        }
    }

    expressions
}

fn expression_needs_outer_scope(
    expr: &Expression,
    local_columns: &HashSet<String>,
    outer_columns: &[ColumnDefinition],
) -> bool {
    match expr {
        Expression::Column(name) => column_needs_outer_scope(name, local_columns, outer_columns),
        Expression::BinaryOp { left, right, .. }
        | Expression::IsDistinctFrom { left, right, .. } => {
            expression_needs_outer_scope(left, local_columns, outer_columns)
                || expression_needs_outer_scope(right, local_columns, outer_columns)
        }
        Expression::UnaryOp { expr, .. }
        | Expression::IsNull { expr, .. }
        | Expression::Cast { expr, .. } => {
            expression_needs_outer_scope(expr, local_columns, outer_columns)
        }
        Expression::In { left, .. } => {
            expression_needs_outer_scope(left, local_columns, outer_columns)
        }
        Expression::Any { left, .. } | Expression::All { left, .. } => {
            expression_needs_outer_scope(left, local_columns, outer_columns)
        }
        Expression::Function(aggregate) => {
            expression_needs_outer_scope(&aggregate.expr, local_columns, outer_columns)
                || aggregate.filter.as_deref().is_some_and(|filter| {
                    expression_needs_outer_scope(filter, local_columns, outer_columns)
                })
        }
        Expression::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand.as_deref().is_some_and(|expr| {
                expression_needs_outer_scope(expr, local_columns, outer_columns)
            }) || when_clauses.iter().any(|(condition, result)| {
                expression_needs_outer_scope(condition, local_columns, outer_columns)
                    || expression_needs_outer_scope(result, local_columns, outer_columns)
            }) || else_clause.as_deref().is_some_and(|expr| {
                expression_needs_outer_scope(expr, local_columns, outer_columns)
            })
        }
        Expression::ScalarFunction { args, .. } => args
            .iter()
            .any(|arg| expression_needs_outer_scope(arg, local_columns, outer_columns)),
        Expression::WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter()
                .chain(partition_by.iter())
                .any(|expr| expression_needs_outer_scope(expr, local_columns, outer_columns))
                || order_by.iter().any(|item| {
                    expression_needs_outer_scope(&item.expr, local_columns, outer_columns)
                })
        }
        Expression::Subquery(_) | Expression::Exists(_) | Expression::Value(_) => false,
    }
}

fn column_needs_outer_scope(
    reference: &str,
    local_columns: &HashSet<String>,
    outer_columns: &[ColumnDefinition],
) -> bool {
    if reference.contains('.') {
        if outer_columns.iter().any(|column| column.name == reference) {
            return true;
        }

        if local_columns.contains(reference) {
            return false;
        }

        let unqualified = unqualified_column_name(reference);
        return outer_columns.iter().any(|column| {
            column.name == reference || unqualified_column_name(&column.name) == unqualified
        });
    }

    let unqualified = unqualified_column_name(reference);
    if local_columns.contains(reference) || local_columns.contains(unqualified) {
        return false;
    }

    outer_columns.iter().any(|column| {
        column.name == reference || unqualified_column_name(&column.name) == unqualified
    })
}

fn subquery_local_column_names(
    db: &dyn DatabaseCatalog,
    subquery: &SelectStatement,
) -> HashSet<String> {
    let mut columns = HashSet::new();
    collect_table_column_names(
        db,
        &subquery.from,
        subquery.from_alias.as_deref(),
        &mut columns,
    );

    for join in &subquery.joins {
        collect_table_column_names(db, &join.table, join.table_alias.as_deref(), &mut columns);
    }

    if let Some((_, alias, column_aliases)) = subquery.from_values.as_ref() {
        for column in column_aliases {
            columns.insert(column.clone());
            columns.insert(format!("{}.{}", alias, column));
        }
    }

    columns
}

fn collect_table_column_names(
    db: &dyn DatabaseCatalog,
    table_name: &str,
    alias: Option<&str>,
    output: &mut HashSet<String>,
) {
    if table_name.is_empty() {
        return;
    }
    if let Some(table) = db.get_table(table_name) {
        let label = alias.unwrap_or(table_name);
        for column in &table.columns {
            output.insert(column.name.clone());
            output.insert(format!("{}.{}", label, column.name));
        }
    }
}

pub(super) fn lateral_subquery_with_outer_scope(
    subquery: &SelectStatement,
    outer_table_name: &str,
) -> SelectStatement {
    let mut rewritten = subquery.clone();
    if rewritten.from.is_empty()
        && rewritten.from_subquery.is_none()
        && rewritten.from_function.is_none()
        && rewritten.from_values.is_none()
    {
        rewritten.from = outer_table_name.to_string();
    } else {
        rewritten.joins.push(Join {
            join_type: JoinType::Cross,
            table: outer_table_name.to_string(),
            table_alias: None,
            on: None,
            using_columns: None,
            lateral: false,
            subquery: None,
        });
    }
    rewritten
}

struct OuterColumnMapping {
    original_name: String,
    unqualified_name: String,
    scoped_name: String,
}

fn scoped_outer_columns(
    outer_columns: &[ColumnDefinition],
) -> (Vec<ColumnDefinition>, Vec<OuterColumnMapping>) {
    let mut scoped_columns = Vec::with_capacity(outer_columns.len());
    let mut mappings = Vec::with_capacity(outer_columns.len());

    for (idx, column) in outer_columns.iter().enumerate() {
        let scoped_name = format!("__outer_col_{}", idx);
        let mut scoped_column = column.clone();
        scoped_column.name = scoped_name.clone();
        scoped_columns.push(scoped_column);
        mappings.push(OuterColumnMapping {
            original_name: column.name.clone(),
            unqualified_name: unqualified_column_name(&column.name).to_string(),
            scoped_name,
        });
    }

    (scoped_columns, mappings)
}

fn scoped_subquery_with_outer_scope(
    subquery: &SelectStatement,
    outer_table_name: &str,
    outer_column_mappings: &[OuterColumnMapping],
    local_columns: &HashSet<String>,
) -> SelectStatement {
    let mut rewritten = lateral_subquery_with_outer_scope(subquery, outer_table_name);
    rewrite_select_outer_references(&mut rewritten, outer_column_mappings, local_columns);
    rewritten
}

fn rewrite_select_outer_references(
    stmt: &mut SelectStatement,
    outer_column_mappings: &[OuterColumnMapping],
    local_columns: &HashSet<String>,
) {
    for column in &mut stmt.columns {
        rewrite_column_outer_references(column, outer_column_mappings, local_columns);
    }
    if let Some(where_clause) = stmt.where_clause.as_mut() {
        rewrite_expression_outer_references(where_clause, outer_column_mappings, local_columns);
    }
    if let Some(group_by) = stmt.group_by.as_mut() {
        rewrite_group_by_outer_references(group_by, outer_column_mappings, local_columns);
    }
    if let Some(having) = stmt.having.as_mut() {
        rewrite_expression_outer_references(having, outer_column_mappings, local_columns);
    }
    if let Some(order_by) = stmt.order_by.as_mut() {
        for order_expr in order_by {
            rewrite_expression_outer_references(
                &mut order_expr.expr,
                outer_column_mappings,
                local_columns,
            );
        }
    }
    if let Some((_, right_select)) = stmt.set_op.as_mut() {
        rewrite_select_outer_references(right_select, outer_column_mappings, local_columns);
    }
}

fn rewrite_column_outer_references(
    column: &mut Column,
    outer_column_mappings: &[OuterColumnMapping],
    local_columns: &HashSet<String>,
) {
    match column {
        Column::Named { name, .. } => {
            if let Some(scoped_name) =
                scoped_outer_column_name(name, outer_column_mappings, local_columns)
            {
                *name = scoped_name;
            }
        }
        Column::Function(aggregate) => {
            rewrite_expression_outer_references(
                &mut aggregate.expr,
                outer_column_mappings,
                local_columns,
            );
            if let Some(filter) = aggregate.filter.as_mut() {
                rewrite_expression_outer_references(filter, outer_column_mappings, local_columns);
            }
        }
        Column::Expression { expr, .. } => {
            rewrite_expression_outer_references(expr, outer_column_mappings, local_columns);
        }
        Column::All | Column::Subquery(_) => {}
    }
}

fn rewrite_group_by_outer_references(
    group_by: &mut GroupByClause,
    outer_column_mappings: &[OuterColumnMapping],
    local_columns: &HashSet<String>,
) {
    match group_by {
        GroupByClause::Simple(exprs)
        | GroupByClause::Rollup(exprs)
        | GroupByClause::Cube(exprs) => {
            for expr in exprs {
                rewrite_expression_outer_references(expr, outer_column_mappings, local_columns);
            }
        }
        GroupByClause::GroupingSets(sets) => {
            for set in sets {
                for expr in set {
                    rewrite_expression_outer_references(expr, outer_column_mappings, local_columns);
                }
            }
        }
    }
}

fn rewrite_expression_outer_references(
    expr: &mut Expression,
    outer_column_mappings: &[OuterColumnMapping],
    local_columns: &HashSet<String>,
) {
    match expr {
        Expression::Column(name) => {
            if let Some(scoped_name) =
                scoped_outer_column_name(name, outer_column_mappings, local_columns)
            {
                *name = scoped_name;
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            rewrite_expression_outer_references(left, outer_column_mappings, local_columns);
            rewrite_expression_outer_references(right, outer_column_mappings, local_columns);
        }
        Expression::UnaryOp { expr, .. } | Expression::IsNull { expr, .. } => {
            rewrite_expression_outer_references(expr, outer_column_mappings, local_columns);
        }
        Expression::In { left, .. } => {
            rewrite_expression_outer_references(left, outer_column_mappings, local_columns);
        }
        Expression::Any { left, .. } | Expression::All { left, .. } => {
            rewrite_expression_outer_references(left, outer_column_mappings, local_columns);
        }
        Expression::Function(aggregate) => {
            rewrite_expression_outer_references(
                &mut aggregate.expr,
                outer_column_mappings,
                local_columns,
            );
            if let Some(filter) = aggregate.filter.as_mut() {
                rewrite_expression_outer_references(filter, outer_column_mappings, local_columns);
            }
        }
        Expression::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand) = operand {
                rewrite_expression_outer_references(operand, outer_column_mappings, local_columns);
            }
            for (condition, result) in when_clauses {
                rewrite_expression_outer_references(
                    condition,
                    outer_column_mappings,
                    local_columns,
                );
                rewrite_expression_outer_references(result, outer_column_mappings, local_columns);
            }
            if let Some(else_clause) = else_clause {
                rewrite_expression_outer_references(
                    else_clause,
                    outer_column_mappings,
                    local_columns,
                );
            }
        }
        Expression::ScalarFunction { args, .. } => {
            for arg in args {
                rewrite_expression_outer_references(arg, outer_column_mappings, local_columns);
            }
        }
        Expression::WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } => {
            for arg in args {
                rewrite_expression_outer_references(arg, outer_column_mappings, local_columns);
            }
            for expr in partition_by {
                rewrite_expression_outer_references(expr, outer_column_mappings, local_columns);
            }
            for order_expr in order_by {
                rewrite_expression_outer_references(
                    &mut order_expr.expr,
                    outer_column_mappings,
                    local_columns,
                );
            }
        }
        Expression::Cast { expr, .. } => {
            rewrite_expression_outer_references(expr, outer_column_mappings, local_columns);
        }
        Expression::IsDistinctFrom { left, right, .. } => {
            rewrite_expression_outer_references(left, outer_column_mappings, local_columns);
            rewrite_expression_outer_references(right, outer_column_mappings, local_columns);
        }
        Expression::Subquery(_) | Expression::Exists(_) | Expression::Value(_) => {}
    }
}

fn scoped_outer_column_name(
    reference: &str,
    outer_column_mappings: &[OuterColumnMapping],
    local_columns: &HashSet<String>,
) -> Option<String> {
    if reference.contains('.') {
        if let Some(mapping) = outer_column_mappings
            .iter()
            .find(|mapping| mapping.original_name == reference)
        {
            return Some(mapping.scoped_name.clone());
        }

        if local_columns.contains(reference) {
            return None;
        }

        let unqualified = unqualified_column_name(reference);
        return outer_column_mappings
            .iter()
            .find(|mapping| mapping.unqualified_name == unqualified)
            .map(|mapping| mapping.scoped_name.clone());
    }

    if local_columns.contains(reference) {
        return None;
    }

    let unqualified = unqualified_column_name(reference);
    outer_column_mappings
        .iter()
        .find(|mapping| mapping.unqualified_name == unqualified)
        .map(|mapping| mapping.scoped_name.clone())
}
