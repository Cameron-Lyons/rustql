use super::*;
use std::collections::HashMap;

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

    if let Some(binding) = outer_value_binding(db, subquery, outer_columns) {
        return execute_planned_select(db, &binding.bind(subquery, outer_row));
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
    let mut expressions = Vec::with_capacity(subquery_expression_ref_capacity(subquery));

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

fn subquery_expression_ref_capacity(subquery: &SelectStatement) -> usize {
    let column_refs = subquery
        .columns
        .iter()
        .map(|column| match column {
            Column::Function(aggregate) => 1 + usize::from(aggregate.filter.is_some()),
            Column::Expression { .. } => 1,
            Column::All | Column::Named { .. } | Column::Subquery(_) => 0,
        })
        .sum::<usize>();

    column_refs
        + usize::from(subquery.where_clause.is_some())
        + subquery
            .group_by
            .as_ref()
            .map(|group_by| group_by.exprs().len())
            .unwrap_or(0)
        + usize::from(subquery.having.is_some())
        + subquery
            .distinct_on
            .as_ref()
            .map(|distinct_on| distinct_on.len())
            .unwrap_or(0)
        + subquery
            .order_by
            .as_ref()
            .map(|order_by| order_by.len())
            .unwrap_or(0)
        + subquery
            .joins
            .iter()
            .filter(|join| join.on.is_some())
            .count()
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
        Expression::In { left, values } => {
            expression_needs_outer_scope(left, local_columns, outer_columns)
                || values
                    .iter()
                    .any(|value| expression_needs_outer_scope(value, local_columns, outer_columns))
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
        Expression::Subquery(_)
        | Expression::Exists(_)
        | Expression::Value(_)
        | Expression::Default => false,
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

/// Binding plan for executing a LATERAL subquery without the temp-table outer
/// scope: every outer column reference is replaced with the current outer
/// row's literal value, so the planner sees constant predicates and can use
/// indexes on the inner table.
pub(super) struct OuterValueBinding {
    /// Column-reference strings in the subquery mapped to outer-row indices.
    references: HashMap<String, usize>,
}

impl OuterValueBinding {
    pub(super) fn is_correlated(&self) -> bool {
        !self.references.is_empty()
    }

    pub(super) fn bind(&self, subquery: &SelectStatement, outer_row: &[Value]) -> SelectStatement {
        let mut bound = subquery.clone();
        for column in &mut bound.columns {
            self.bind_column(column, outer_row);
        }
        if let Some(where_clause) = bound.where_clause.as_mut() {
            self.bind_expression(where_clause, outer_row);
        }
        if let Some(group_by) = bound.group_by.as_mut() {
            self.bind_group_by(group_by, outer_row);
        }
        if let Some(having) = bound.having.as_mut() {
            self.bind_expression(having, outer_row);
        }
        if let Some(order_by) = bound.order_by.as_mut() {
            for item in order_by {
                self.bind_expression(&mut item.expr, outer_row);
            }
        }
        // Join ON and DISTINCT ON clauses are guaranteed free of outer
        // references by `outer_value_binding`, so they are not rewritten.
        bound
    }

    fn bind_column(&self, column: &mut Column, outer_row: &[Value]) {
        match column {
            Column::Named { name, alias } => {
                if let Some(&index) = self.references.get(name.as_str()) {
                    let label = alias
                        .clone()
                        .unwrap_or_else(|| unqualified_column_name(name).to_string());
                    *column = Column::Expression {
                        expr: Expression::Value(outer_row_value(outer_row, index)),
                        alias: Some(label),
                    };
                }
            }
            Column::Function(aggregate) => {
                self.bind_expression(&mut aggregate.expr, outer_row);
                if let Some(filter) = aggregate.filter.as_mut() {
                    self.bind_expression(filter, outer_row);
                }
            }
            Column::Expression { expr, .. } => self.bind_expression(expr, outer_row),
            Column::All | Column::Subquery(_) => {}
        }
    }

    fn bind_group_by(&self, group_by: &mut GroupByClause, outer_row: &[Value]) {
        match group_by {
            GroupByClause::Simple(exprs)
            | GroupByClause::Rollup(exprs)
            | GroupByClause::Cube(exprs) => {
                for expr in exprs {
                    self.bind_expression(expr, outer_row);
                }
            }
            GroupByClause::GroupingSets(sets) => {
                for set in sets {
                    for expr in set {
                        self.bind_expression(expr, outer_row);
                    }
                }
            }
        }
    }

    fn bind_expression(&self, expr: &mut Expression, outer_row: &[Value]) {
        match expr {
            Expression::Column(name) => {
                if let Some(&index) = self.references.get(name.as_str()) {
                    *expr = Expression::Value(outer_row_value(outer_row, index));
                }
            }
            Expression::BinaryOp { left, right, .. }
            | Expression::IsDistinctFrom { left, right, .. } => {
                self.bind_expression(left, outer_row);
                self.bind_expression(right, outer_row);
            }
            Expression::UnaryOp { expr, .. }
            | Expression::IsNull { expr, .. }
            | Expression::Cast { expr, .. } => self.bind_expression(expr, outer_row),
            Expression::In { left, values } => {
                self.bind_expression(left, outer_row);
                for value in values {
                    self.bind_expression(value, outer_row);
                }
            }
            Expression::Any { left, .. } | Expression::All { left, .. } => {
                self.bind_expression(left, outer_row);
            }
            Expression::Function(aggregate) => {
                self.bind_expression(&mut aggregate.expr, outer_row);
                if let Some(filter) = aggregate.filter.as_mut() {
                    self.bind_expression(filter, outer_row);
                }
            }
            Expression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(operand) = operand {
                    self.bind_expression(operand, outer_row);
                }
                for (condition, result) in when_clauses {
                    self.bind_expression(condition, outer_row);
                    self.bind_expression(result, outer_row);
                }
                if let Some(else_clause) = else_clause {
                    self.bind_expression(else_clause, outer_row);
                }
            }
            Expression::ScalarFunction { args, .. } => {
                for arg in args {
                    self.bind_expression(arg, outer_row);
                }
            }
            Expression::WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for arg in args {
                    self.bind_expression(arg, outer_row);
                }
                for expr in partition_by {
                    self.bind_expression(expr, outer_row);
                }
                for item in order_by {
                    self.bind_expression(&mut item.expr, outer_row);
                }
            }
            Expression::Subquery(_)
            | Expression::Exists(_)
            | Expression::Value(_)
            | Expression::Default => {}
        }
    }
}

fn outer_row_value(outer_row: &[Value], index: usize) -> Value {
    outer_row.get(index).cloned().unwrap_or(Value::Null)
}

/// Decide whether a correlated subquery can run by binding outer references
/// to literal values. Returns `None` for shapes where the rewrite is not
/// known to be safe (nested subqueries, CTEs, set operations, derived tables,
/// outer references inside join ON or DISTINCT ON clauses, names that are
/// ambiguous between the outer and inner scope, ...); callers fall back to
/// the temp-table outer scope in that case.
pub(super) fn outer_value_binding(
    db: &dyn DatabaseCatalog,
    subquery: &SelectStatement,
    outer_columns: &[ColumnDefinition],
) -> Option<OuterValueBinding> {
    if !subquery.ctes.is_empty()
        || subquery.from_subquery.is_some()
        || subquery.from_function.is_some()
        || subquery.from_values.is_some()
        || subquery.set_op.is_some()
        || !subquery.window_definitions.is_empty()
    {
        return None;
    }
    if subquery
        .joins
        .iter()
        .any(|join| join.lateral || join.subquery.is_some())
    {
        return None;
    }

    let local_columns = subquery_local_column_names(db, subquery);
    let mut references = HashMap::new();

    for column in &subquery.columns {
        match column {
            Column::Named { name, .. } => {
                if !record_outer_reference(name, outer_columns, &local_columns, &mut references) {
                    return None;
                }
            }
            Column::Function(aggregate) => {
                if !scan_expression_references(
                    &aggregate.expr,
                    outer_columns,
                    &local_columns,
                    &mut references,
                ) {
                    return None;
                }
                if let Some(filter) = aggregate.filter.as_deref()
                    && !scan_expression_references(
                        filter,
                        outer_columns,
                        &local_columns,
                        &mut references,
                    )
                {
                    return None;
                }
            }
            Column::Expression { expr, .. } => {
                if !scan_expression_references(expr, outer_columns, &local_columns, &mut references)
                {
                    return None;
                }
            }
            Column::All => {}
            Column::Subquery(_) => return None,
        }
    }

    let mut clause_refs: Vec<&Expression> = Vec::new();
    if let Some(where_clause) = subquery.where_clause.as_ref() {
        clause_refs.push(where_clause);
    }
    if let Some(group_by) = subquery.group_by.as_ref() {
        clause_refs.extend(group_by.exprs());
    }
    if let Some(having) = subquery.having.as_ref() {
        clause_refs.push(having);
    }
    if let Some(order_by) = subquery.order_by.as_ref() {
        clause_refs.extend(order_by.iter().map(|item| &item.expr));
    }
    for expr in clause_refs {
        if !scan_expression_references(expr, outer_columns, &local_columns, &mut references) {
            return None;
        }
    }

    // The temp-table rewrite never substitutes outer references inside join
    // ON or DISTINCT ON clauses; keep those subqueries on the fallback path
    // so their resolution behavior is unchanged.
    let mut strictly_local: Vec<&Expression> = Vec::new();
    strictly_local.extend(subquery.joins.iter().filter_map(|join| join.on.as_ref()));
    if let Some(distinct_on) = subquery.distinct_on.as_ref() {
        strictly_local.extend(distinct_on);
    }
    for expr in strictly_local {
        let mut disallowed = HashMap::new();
        if !scan_expression_references(expr, outer_columns, &local_columns, &mut disallowed)
            || !disallowed.is_empty()
        {
            return None;
        }
    }

    Some(OuterValueBinding { references })
}

/// Collect outer references from `expr` into `references`. Returns `false`
/// when the expression contains a construct the literal-binding rewrite does
/// not cover (nested subqueries) or an outer reference that cannot be
/// resolved to a single outer column.
fn scan_expression_references(
    expr: &Expression,
    outer_columns: &[ColumnDefinition],
    local_columns: &HashSet<String>,
    references: &mut HashMap<String, usize>,
) -> bool {
    match expr {
        Expression::Column(name) => {
            record_outer_reference(name, outer_columns, local_columns, references)
        }
        Expression::Subquery(_)
        | Expression::Exists(_)
        | Expression::Any { .. }
        | Expression::All { .. } => false,
        Expression::BinaryOp { left, right, .. }
        | Expression::IsDistinctFrom { left, right, .. } => {
            scan_expression_references(left, outer_columns, local_columns, references)
                && scan_expression_references(right, outer_columns, local_columns, references)
        }
        Expression::UnaryOp { expr, .. }
        | Expression::IsNull { expr, .. }
        | Expression::Cast { expr, .. } => {
            scan_expression_references(expr, outer_columns, local_columns, references)
        }
        Expression::In { left, values } => {
            scan_expression_references(left, outer_columns, local_columns, references)
                && values.iter().all(|value| {
                    scan_expression_references(value, outer_columns, local_columns, references)
                })
        }
        Expression::Function(aggregate) => {
            scan_expression_references(&aggregate.expr, outer_columns, local_columns, references)
                && aggregate.filter.as_deref().is_none_or(|filter| {
                    scan_expression_references(filter, outer_columns, local_columns, references)
                })
        }
        Expression::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand.as_deref().is_none_or(|operand| {
                scan_expression_references(operand, outer_columns, local_columns, references)
            }) && when_clauses.iter().all(|(condition, result)| {
                scan_expression_references(condition, outer_columns, local_columns, references)
                    && scan_expression_references(result, outer_columns, local_columns, references)
            }) && else_clause.as_deref().is_none_or(|else_clause| {
                scan_expression_references(else_clause, outer_columns, local_columns, references)
            })
        }
        Expression::ScalarFunction { args, .. } => args
            .iter()
            .all(|arg| scan_expression_references(arg, outer_columns, local_columns, references)),
        Expression::WindowFunction {
            args,
            partition_by,
            order_by,
            ..
        } => {
            args.iter().chain(partition_by.iter()).all(|expr| {
                scan_expression_references(expr, outer_columns, local_columns, references)
            }) && order_by.iter().all(|item| {
                scan_expression_references(&item.expr, outer_columns, local_columns, references)
            })
        }
        Expression::Value(_) | Expression::Default => true,
    }
}

fn record_outer_reference(
    reference: &str,
    outer_columns: &[ColumnDefinition],
    local_columns: &HashSet<String>,
    references: &mut HashMap<String, usize>,
) -> bool {
    if !column_needs_outer_scope(reference, local_columns, outer_columns) {
        return true;
    }
    // A name that matches the outer scope but is also resolvable locally is
    // ambiguous; leave those subqueries on the temp-table fallback path.
    if local_columns.contains(reference) {
        return false;
    }
    match resolve_outer_column_index(reference, outer_columns, local_columns) {
        Some(index) => {
            references.insert(reference.to_string(), index);
            true
        }
        None => false,
    }
}

/// Resolution mirror of [`column_needs_outer_scope`]: exact qualified matches
/// win, then local shadowing, then unqualified matches against the outer row.
fn resolve_outer_column_index(
    reference: &str,
    outer_columns: &[ColumnDefinition],
    local_columns: &HashSet<String>,
) -> Option<usize> {
    if reference.contains('.') {
        if let Some(index) = outer_columns
            .iter()
            .position(|column| column.name == reference)
        {
            return Some(index);
        }
        if local_columns.contains(reference) {
            return None;
        }
        let unqualified = unqualified_column_name(reference);
        return outer_columns
            .iter()
            .position(|column| unqualified_column_name(&column.name) == unqualified);
    }

    if local_columns.contains(reference)
        || local_columns.contains(unqualified_column_name(reference))
    {
        return None;
    }
    let unqualified = unqualified_column_name(reference);
    outer_columns.iter().position(|column| {
        column.name == reference || unqualified_column_name(&column.name) == unqualified
    })
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
        Expression::In { left, values } => {
            rewrite_expression_outer_references(left, outer_column_mappings, local_columns);
            for value in values {
                rewrite_expression_outer_references(value, outer_column_mappings, local_columns);
            }
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
        Expression::Subquery(_)
        | Expression::Exists(_)
        | Expression::Value(_)
        | Expression::Default => {}
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
