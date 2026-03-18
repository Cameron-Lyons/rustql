use crate::ast::*;
use crate::database::{Database, DatabaseCatalog, RowId, Table};
use crate::engine::QueryResult;
use crate::error::RustqlError;
use crate::plan_executor::PlanExecutor;
use crate::planner;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};

use super::aggregate::{
    compute_aggregate, evaluate_having, evaluate_window_function_outputs,
    execute_select_with_aggregates_result, execute_select_with_grouping_result,
};
use super::expr::{
    compare_values_for_sort, evaluate_expression, evaluate_select_order_expression,
    evaluate_value_expression, evaluate_value_expression_with_db,
};
use super::join::perform_multiple_joins;
use super::{ExecutionContext, SelectResult, get_database_read, get_database_write, rows_result};

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

fn execute_set_operation_result(
    context: Option<&ExecutionContext>,
    left_stmt: SelectStatement,
    right_stmt: SelectStatement,
    set_op_type: &SetOperation,
    db: &dyn DatabaseCatalog,
) -> Result<SelectResult, RustqlError> {
    let mut left_stmt_internal = left_stmt;
    left_stmt_internal.set_op = None;
    let mut right_stmt_internal = right_stmt;
    right_stmt_internal.set_op = None;

    let left_result = execute_select_internal(context, left_stmt_internal, db)?;
    let right_result = execute_select_internal(context, right_stmt_internal, db)?;

    let mut combined: Vec<Vec<Value>> = Vec::new();

    match set_op_type {
        SetOperation::UnionAll => {
            combined.extend(left_result.rows);
            combined.extend(right_result.rows);
        }
        SetOperation::Union => {
            use std::collections::BTreeSet;
            let mut seen: BTreeSet<Vec<Value>> = BTreeSet::new();
            for row in left_result.rows {
                if seen.insert(row.clone()) {
                    combined.push(row);
                }
            }
            for row in right_result.rows {
                if seen.insert(row.clone()) {
                    combined.push(row);
                }
            }
        }
        SetOperation::Intersect => {
            use std::collections::BTreeSet;
            let right_set: BTreeSet<Vec<Value>> = right_result.rows.into_iter().collect();
            let mut seen: BTreeSet<Vec<Value>> = BTreeSet::new();
            for row in left_result.rows {
                if right_set.contains(&row) && seen.insert(row.clone()) {
                    combined.push(row);
                }
            }
        }
        SetOperation::IntersectAll => {
            use std::collections::BTreeMap;
            let mut right_counts: BTreeMap<Vec<Value>, usize> = BTreeMap::new();
            for row in &right_result.rows {
                *right_counts.entry(row.clone()).or_insert(0) += 1;
            }
            let mut left_counts: BTreeMap<Vec<Value>, usize> = BTreeMap::new();
            for row in &left_result.rows {
                *left_counts.entry(row.clone()).or_insert(0) += 1;
            }
            for (row, left_count) in &left_counts {
                let right_count = right_counts.get(row).copied().unwrap_or(0);
                let emit_count = (*left_count).min(right_count);
                for _ in 0..emit_count {
                    combined.push(row.clone());
                }
            }
        }
        SetOperation::Except => {
            use std::collections::BTreeSet;
            let right_set: BTreeSet<Vec<Value>> = right_result.rows.into_iter().collect();
            let mut seen: BTreeSet<Vec<Value>> = BTreeSet::new();
            for row in left_result.rows {
                if !right_set.contains(&row) && seen.insert(row.clone()) {
                    combined.push(row);
                }
            }
        }
        SetOperation::ExceptAll => {
            use std::collections::BTreeMap;
            let mut right_counts: BTreeMap<Vec<Value>, usize> = BTreeMap::new();
            for row in &right_result.rows {
                *right_counts.entry(row.clone()).or_insert(0) += 1;
            }
            let mut left_counts: BTreeMap<Vec<Value>, usize> = BTreeMap::new();
            let mut left_order: Vec<Vec<Value>> = Vec::new();
            for row in &left_result.rows {
                let entry = left_counts.entry(row.clone()).or_insert(0);
                if *entry == 0 {
                    left_order.push(row.clone());
                }
                *entry += 1;
            }
            for row in &left_order {
                let left_count = left_counts.get(row).copied().unwrap_or(0);
                let right_count = right_counts.get(row).copied().unwrap_or(0);
                let emit_count = left_count.saturating_sub(right_count);
                for _ in 0..emit_count {
                    combined.push(row.clone());
                }
            }
        }
    }

    Ok(SelectResult {
        headers: left_result.headers,
        rows: combined,
    })
}

pub(crate) fn plan_select_for_execution(
    db: &dyn DatabaseCatalog,
    stmt: &SelectStatement,
) -> Result<Option<planner::PlanNode>, RustqlError> {
    if !can_execute_via_plan(db, stmt) {
        return Ok(None);
    }

    Ok(Some(planner::plan_query(db, stmt)?))
}

fn can_execute_via_plan(db: &dyn DatabaseCatalog, stmt: &SelectStatement) -> bool {
    let has_aggregate_columns = stmt
        .columns
        .iter()
        .any(|column| matches!(column, Column::Function(_)));

    if stmt.from.is_empty() && stmt.from_function.is_none()
        || !stmt.ctes.is_empty()
        || stmt.from_subquery.is_some()
        || stmt.from_values.is_some()
        || stmt.set_op.is_some()
    {
        return false;
    }

    if stmt
        .from_function
        .as_ref()
        .is_some_and(|function| !table_function_supported_by_plan(function, stmt))
    {
        return false;
    }

    if stmt
        .distinct_on
        .as_ref()
        .is_some_and(|exprs| !distinct_on_supported_by_plan(exprs))
    {
        return false;
    }

    if stmt
        .joins
        .iter()
        .any(|join| !join_supported_by_plan(db, join))
    {
        return false;
    }

    if has_aggregate_columns
        && stmt.group_by.is_none()
        && stmt
            .columns
            .iter()
            .any(|column| !matches!(column, Column::Function(_)))
    {
        return false;
    }

    stmt.columns.iter().all(column_supported_by_plan)
        && stmt
            .where_clause
            .as_ref()
            .is_none_or(predicate_expression_supported_by_plan)
        && stmt
            .group_by
            .as_ref()
            .is_none_or(group_by_supported_by_plan)
        && stmt
            .having
            .as_ref()
            .is_none_or(expression_supported_by_plan)
        && stmt.order_by.as_ref().is_none_or(|items| {
            items
                .iter()
                .all(|item| matches!(&item.expr, Expression::Column(_)))
        })
}

fn group_by_supported_by_plan(group_by: &GroupByClause) -> bool {
    match group_by {
        GroupByClause::Simple(exprs)
        | GroupByClause::Rollup(exprs)
        | GroupByClause::Cube(exprs) => group_by_exprs_supported_by_plan(exprs),
        GroupByClause::GroupingSets(sets) => {
            sets.iter().all(|set| group_by_exprs_supported_by_plan(set))
        }
    }
}

fn column_supported_by_plan(column: &Column) -> bool {
    match column {
        Column::All => true,
        Column::Named { .. } => true,
        Column::Function(agg) => {
            expression_supported_by_plan(&agg.expr)
                && agg
                    .filter
                    .as_deref()
                    .is_none_or(predicate_expression_supported_by_plan)
        }
        Column::Expression { expr, .. } => window_expression_supported_by_plan(expr),
        Column::Subquery(_) => false,
    }
}

fn join_supported_by_plan(db: &dyn DatabaseCatalog, join: &Join) -> bool {
    if join.lateral {
        return join.using_columns.is_none()
            && !matches!(join.join_type, JoinType::Natural)
            && join.subquery.as_ref().is_some_and(|(subquery, _)| {
                planner::infer_select_output_columns(db, subquery).is_ok()
            });
    }

    match join.join_type {
        JoinType::Inner => true,
        JoinType::Cross => join.using_columns.is_none(),
        JoinType::Left | JoinType::Right | JoinType::Full => true,
        JoinType::Natural => join.on.is_none() && join.using_columns.is_none(),
    }
}

fn join_requires_source_materialization(join: &Join) -> bool {
    !join.lateral && join.subquery.is_some()
}

fn table_function_supported_by_plan(function: &TableFunction, stmt: &SelectStatement) -> bool {
    function.name == "generate_series" && stmt.joins.is_empty()
}

fn distinct_on_supported_by_plan(exprs: &[Expression]) -> bool {
    exprs
        .iter()
        .all(|expr| matches!(expr, Expression::Column(_)))
}

fn group_by_exprs_supported_by_plan(exprs: &[Expression]) -> bool {
    exprs
        .iter()
        .all(|expr| matches!(expr, Expression::Column(_)))
}

fn window_expression_supported_by_plan(expr: &Expression) -> bool {
    matches!(expr, Expression::WindowFunction { .. })
}

fn projection_expression_supported_by_plan(expr: &Expression) -> bool {
    match expr {
        Expression::Column(_) | Expression::Value(_) => true,
        Expression::BinaryOp { left, op, right } => {
            matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
            ) && projection_expression_supported_by_plan(left)
                && projection_expression_supported_by_plan(right)
        }
        Expression::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => projection_expression_supported_by_plan(expr),
        _ => false,
    }
}

fn expression_supported_by_plan(expr: &Expression) -> bool {
    match expr {
        Expression::Column(_) | Expression::Value(_) => true,
        Expression::BinaryOp { left, right, .. } => {
            expression_supported_by_plan(left) && expression_supported_by_plan(right)
        }
        Expression::UnaryOp { expr, .. } => expression_supported_by_plan(expr),
        Expression::In { left, .. } => expression_supported_by_plan(left),
        Expression::IsNull { expr, .. } => expression_supported_by_plan(expr),
        Expression::Function(agg) => {
            expression_supported_by_plan(&agg.expr)
                && agg
                    .filter
                    .as_deref()
                    .is_none_or(predicate_expression_supported_by_plan)
        }
        Expression::IsDistinctFrom { left, right, .. } => {
            expression_supported_by_plan(left) && expression_supported_by_plan(right)
        }
        Expression::Subquery(_)
        | Expression::Exists(_)
        | Expression::Any { .. }
        | Expression::All { .. }
        | Expression::Case { .. }
        | Expression::ScalarFunction { .. }
        | Expression::WindowFunction { .. }
        | Expression::Cast { .. } => false,
    }
}

fn predicate_expression_supported_by_plan(expr: &Expression) -> bool {
    match expr {
        Expression::BinaryOp { left, op, right } => match op {
            BinaryOperator::And | BinaryOperator::Or => {
                predicate_expression_supported_by_plan(left)
                    && predicate_expression_supported_by_plan(right)
            }
            BinaryOperator::Like
            | BinaryOperator::ILike
            | BinaryOperator::Equal
            | BinaryOperator::NotEqual
            | BinaryOperator::LessThan
            | BinaryOperator::LessThanOrEqual
            | BinaryOperator::GreaterThan
            | BinaryOperator::GreaterThanOrEqual => {
                simple_value_expression_supported_by_plan(left)
                    && simple_value_expression_supported_by_plan(right)
            }
            BinaryOperator::Between => {
                simple_value_expression_supported_by_plan(left)
                    && predicate_expression_supported_by_plan(right)
            }
            BinaryOperator::In => {
                simple_value_expression_supported_by_plan(left)
                    && projection_expression_supported_by_plan(right)
            }
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide
            | BinaryOperator::Concat => false,
        },
        Expression::In { left, .. } => simple_value_expression_supported_by_plan(left),
        Expression::IsNull { expr, .. } => simple_value_expression_supported_by_plan(expr),
        Expression::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => predicate_expression_supported_by_plan(expr),
        Expression::IsDistinctFrom { left, right, .. } => {
            simple_value_expression_supported_by_plan(left)
                && simple_value_expression_supported_by_plan(right)
        }
        _ => false,
    }
}

fn simple_value_expression_supported_by_plan(expr: &Expression) -> bool {
    matches!(expr, Expression::Column(_) | Expression::Value(_))
}

fn execute_select_without_from(stmt: SelectStatement) -> Result<SelectResult, RustqlError> {
    let empty_columns: Vec<ColumnDefinition> = Vec::new();
    let empty_row: Vec<Value> = Vec::new();

    let mut headers = Vec::new();
    let mut result_row = Vec::new();

    for col in &stmt.columns {
        match col {
            Column::Named { name, alias } => {
                let col_name = alias.clone().unwrap_or_else(|| name.clone());
                headers.push(col_name);
                result_row.push(Value::Null);
            }
            Column::Expression { expr, alias } => {
                let val = evaluate_value_expression(expr, &empty_columns, &empty_row)?;
                let col_name = alias
                    .clone()
                    .unwrap_or_else(|| format!("column{}", headers.len()));
                headers.push(col_name);
                result_row.push(val);
            }
            Column::Function(agg) => {
                let col_name = agg
                    .alias
                    .clone()
                    .unwrap_or_else(|| format!("{:?}", agg.function));
                headers.push(col_name);
                result_row.push(Value::Null);
            }
            Column::Subquery(_) => {}
            Column::All => {}
        }
    }

    Ok(SelectResult {
        headers,
        rows: vec![result_row],
    })
}

fn execute_generate_series_result(
    stmt: SelectStatement,
    tf: &TableFunction,
) -> Result<SelectResult, RustqlError> {
    let empty_columns: Vec<ColumnDefinition> = Vec::new();
    let empty_row: Vec<Value> = Vec::new();

    let start = match evaluate_value_expression(&tf.args[0], &empty_columns, &empty_row)? {
        Value::Integer(n) => n,
        _ => {
            return Err(RustqlError::TypeMismatch(
                "GENERATE_SERIES arguments must be integers".to_string(),
            ));
        }
    };
    let stop = match evaluate_value_expression(&tf.args[1], &empty_columns, &empty_row)? {
        Value::Integer(n) => n,
        _ => {
            return Err(RustqlError::TypeMismatch(
                "GENERATE_SERIES arguments must be integers".to_string(),
            ));
        }
    };
    let step = if tf.args.len() > 2 {
        match evaluate_value_expression(&tf.args[2], &empty_columns, &empty_row)? {
            Value::Integer(n) => n,
            _ => {
                return Err(RustqlError::TypeMismatch(
                    "GENERATE_SERIES step must be an integer".to_string(),
                ));
            }
        }
    } else if start <= stop {
        1
    } else {
        -1
    };

    if step == 0 {
        return Err(RustqlError::Internal(
            "GENERATE_SERIES step cannot be zero".to_string(),
        ));
    }

    let col_name = tf
        .alias
        .clone()
        .unwrap_or_else(|| "generate_series".to_string());

    let series_col = ColumnDefinition {
        name: col_name.clone(),
        data_type: DataType::Integer,
        nullable: false,
        primary_key: false,
        unique: false,
        default_value: None,
        foreign_key: None,
        check: None,
        auto_increment: false,
        generated: None,
    };

    let mut series_rows: Vec<Vec<Value>> = Vec::new();
    let mut current = start;
    if step > 0 {
        while current <= stop {
            series_rows.push(vec![Value::Integer(current)]);
            current += step;
        }
    } else {
        while current >= stop {
            series_rows.push(vec![Value::Integer(current)]);
            current += step;
        }
    }

    let all_columns = vec![series_col];

    let mut filtered_rows: Vec<&Vec<Value>> = Vec::new();
    for row in &series_rows {
        let include = if let Some(ref where_expr) = stmt.where_clause {
            evaluate_expression(None, where_expr, &all_columns, row)?
        } else {
            true
        };
        if include {
            filtered_rows.push(row);
        }
    }

    let headers: Vec<String> = stmt
        .columns
        .iter()
        .map(|col| match col {
            Column::All => col_name.clone(),
            Column::Named { name, alias } => alias.clone().unwrap_or_else(|| name.clone()),
            Column::Expression { alias, .. } => {
                alias.clone().unwrap_or_else(|| "<expr>".to_string())
            }
            _ => "<col>".to_string(),
        })
        .collect();

    let offset = stmt.offset.unwrap_or(0);
    let limit = stmt.limit.unwrap_or(filtered_rows.len());
    let mut rows: Vec<Vec<Value>> = Vec::new();

    for row in filtered_rows.iter().skip(offset).take(limit) {
        let mut projected = Vec::new();
        for col in &stmt.columns {
            let val = match col {
                Column::All => row[0].clone(),
                Column::Named { name, .. } => {
                    let idx = all_columns
                        .iter()
                        .position(|c| c.name == *name)
                        .unwrap_or(0);
                    row.get(idx).cloned().unwrap_or(Value::Null)
                }
                Column::Expression { expr, .. } => {
                    evaluate_value_expression(expr, &all_columns, row)?
                }
                _ => Value::Null,
            };
            projected.push(val);
        }
        rows.push(projected);
    }

    Ok(SelectResult { headers, rows })
}

#[derive(Default)]
struct TempTableScope {
    originals: HashMap<String, Option<Table>>,
}

impl TempTableScope {
    fn insert_or_replace(&mut self, db: &mut Database, name: String, table: Table) {
        let previous = db.tables.insert(name.clone(), table);
        self.originals.entry(name).or_insert(previous);
    }

    fn cleanup(self, db: &mut Database) {
        for (name, original) in self.originals {
            match original {
                Some(table) => {
                    db.tables.insert(name, table);
                }
                None => {
                    db.tables.remove(&name);
                }
            }
        }
    }
}

fn select_requires_source_materialization(
    db: &dyn DatabaseCatalog,
    stmt: &SelectStatement,
) -> bool {
    stmt.from_values.is_some()
        || stmt.from_subquery.is_some()
        || stmt.joins.iter().any(join_requires_source_materialization)
        || !stmt.ctes.is_empty()
        || (!stmt.from.is_empty() && !db.contains_table(&stmt.from) && db.contains_view(&stmt.from))
}

pub(crate) fn explain_select(
    context: &ExecutionContext,
    mut stmt: SelectStatement,
) -> Result<planner::PlanNode, RustqlError> {
    resolve_window_definitions(&mut stmt);

    let db = get_database_read(context);
    if !select_requires_source_materialization(&db, &stmt) {
        return if let Some(plan) = plan_select_for_execution(&db, &stmt)? {
            Ok(plan)
        } else {
            planner::plan_query(&db, &stmt)
        };
    }
    drop(db);

    let mut db = get_database_write(context);
    plan_select_in_db(Some(context), stmt, &mut db)
}

fn execute_select_with_materialized_sources(
    context: &ExecutionContext,
    stmt: SelectStatement,
) -> Result<QueryResult, RustqlError> {
    let mut db = get_database_write(context);
    let result = execute_select_in_db(Some(context), stmt, &mut db)?;
    Ok(rows_result(result))
}

fn execute_select_in_db(
    context: Option<&ExecutionContext>,
    mut stmt: SelectStatement,
    db: &mut Database,
) -> Result<SelectResult, RustqlError> {
    resolve_window_definitions(&mut stmt);

    if !select_requires_source_materialization(db, &stmt)
        && let Some(plan) = plan_select_for_execution(db, &stmt)?
    {
        let execution = PlanExecutor::new(db).execute(&plan, &stmt)?;
        return Ok(SelectResult {
            headers: execution.columns,
            rows: execution.rows,
        });
    }

    if let Some(ref tf) = stmt.from_function
        && tf.name == "generate_series"
    {
        return execute_generate_series_result(stmt.clone(), tf);
    }

    let mut temp_scope = TempTableScope::default();
    let result = (|| {
        let rewritten = rewrite_select_sources_for_scope(context, stmt, db, &mut temp_scope)?;
        if let Some(plan) = plan_select_for_execution(db, &rewritten)? {
            let execution = PlanExecutor::new(db).execute(&plan, &rewritten)?;
            return Ok(SelectResult {
                headers: execution.columns,
                rows: execution.rows,
            });
        }
        execute_select_internal(context, rewritten, db)
    })();
    temp_scope.cleanup(db);
    result
}

fn plan_select_in_db(
    context: Option<&ExecutionContext>,
    mut stmt: SelectStatement,
    db: &mut Database,
) -> Result<planner::PlanNode, RustqlError> {
    resolve_window_definitions(&mut stmt);

    let mut temp_scope = TempTableScope::default();
    let result = (|| {
        let rewritten = rewrite_select_sources_for_scope(context, stmt, db, &mut temp_scope)?;
        if let Some(plan) = plan_select_for_execution(db, &rewritten)? {
            Ok(plan)
        } else {
            planner::plan_query(db, &rewritten)
        }
    })();
    temp_scope.cleanup(db);
    result
}

fn rewrite_select_sources_for_scope(
    context: Option<&ExecutionContext>,
    mut stmt: SelectStatement,
    db: &mut Database,
    temp_scope: &mut TempTableScope,
) -> Result<SelectStatement, RustqlError> {
    if !stmt.ctes.is_empty() {
        materialize_ctes_for_scope(context, &mut stmt, db, temp_scope)?;
    }

    if let Some((subquery, alias)) = stmt.from_subquery.clone() {
        let subquery_result = execute_select_in_db(context, *subquery, db)?;
        temp_scope.insert_or_replace(
            db,
            alias.clone(),
            table_from_select_result(&subquery_result),
        );
        stmt.from = alias;
        stmt.from_subquery = None;
    }

    if let Some((values_rows, alias, col_aliases)) = stmt.from_values.clone() {
        let table = table_from_values_rows(&values_rows, &col_aliases)?;
        temp_scope.insert_or_replace(db, alias.clone(), table);
        stmt.from = alias;
        stmt.from_values = None;
    }

    for join in &mut stmt.joins {
        if join_requires_source_materialization(join)
            && let Some((subquery, alias)) = join.subquery.clone()
        {
            let subquery_result = execute_select_in_db(context, *subquery, db)?;
            temp_scope.insert_or_replace(
                db,
                alias.clone(),
                table_from_select_result(&subquery_result),
            );
            join.table = alias;
            join.subquery = None;
        }
    }

    if !stmt.from.is_empty()
        && !db.contains_table(&stmt.from)
        && let Some(view) = db.get_view(&stmt.from).cloned()
    {
        let view_result = execute_view_select_in_db(context, &view.query_sql, db)?;
        temp_scope.insert_or_replace(
            db,
            view.name.clone(),
            table_from_select_result(&view_result),
        );
    }

    Ok(stmt)
}

fn execute_view_select_in_db(
    context: Option<&ExecutionContext>,
    view_sql: &str,
    db: &mut Database,
) -> Result<SelectResult, RustqlError> {
    let tokens = crate::lexer::tokenize(view_sql)?;
    let view_stmt = crate::parser::parse(tokens)?;
    let view_select = match view_stmt {
        crate::ast::Statement::Select(s) => s,
        _ => {
            return Err(RustqlError::Internal(
                "View definition is not a SELECT statement".to_string(),
            ));
        }
    };
    execute_select_in_db(context, view_select, db)
}

fn materialize_ctes_for_scope(
    context: Option<&ExecutionContext>,
    stmt: &mut SelectStatement,
    db: &mut Database,
    temp_scope: &mut TempTableScope,
) -> Result<(), RustqlError> {
    let ctes = std::mem::take(&mut stmt.ctes);

    for cte in ctes {
        if cte.recursive {
            materialize_recursive_cte_for_scope(context, &cte, db, temp_scope)?;
        } else {
            let cte_result = execute_select_in_db(context, cte.query.clone(), db)?;
            temp_scope.insert_or_replace(
                db,
                cte.name.clone(),
                table_from_select_result(&cte_result),
            );
        }
    }

    Ok(())
}

fn materialize_recursive_cte_for_scope(
    context: Option<&ExecutionContext>,
    cte: &Cte,
    db: &mut Database,
    temp_scope: &mut TempTableScope,
) -> Result<(), RustqlError> {
    let cte_query = &cte.query;
    if let Some((ref set_op_type, ref recursive_part)) = cte_query.set_op {
        if !matches!(set_op_type, SetOperation::UnionAll | SetOperation::Union) {
            return Err(RustqlError::Internal(
                "Recursive CTE requires UNION or UNION ALL".to_string(),
            ));
        }
        let is_union_all = matches!(set_op_type, SetOperation::UnionAll);

        let mut base_stmt = cte_query.clone();
        base_stmt.set_op = None;
        let base_result = execute_select_in_db(context, base_stmt, db)?;
        let columns = column_definitions_from_result(&base_result.headers, &base_result.rows);

        let mut all_rows: Vec<Vec<Value>> = base_result.rows.clone();
        let mut working_rows: Vec<Vec<Value>> = base_result.rows;
        let mut seen: Option<std::collections::BTreeSet<Vec<Value>>> = if is_union_all {
            None
        } else {
            let mut set = std::collections::BTreeSet::new();
            for row in &all_rows {
                set.insert(row.clone());
            }
            Some(set)
        };

        const MAX_ITERATIONS: usize = 1000;
        for _ in 0..MAX_ITERATIONS {
            if working_rows.is_empty() {
                break;
            }

            temp_scope.insert_or_replace(
                db,
                cte.name.clone(),
                Table::new(columns.clone(), working_rows, vec![]),
            );

            let recursive_result = execute_select_in_db(context, *recursive_part.clone(), db)?;

            let mut new_rows: Vec<Vec<Value>> = Vec::new();
            for row in recursive_result.rows {
                if let Some(ref mut seen_set) = seen {
                    if seen_set.insert(row.clone()) {
                        new_rows.push(row);
                    }
                } else {
                    new_rows.push(row);
                }
            }

            if new_rows.is_empty() {
                break;
            }

            all_rows.extend(new_rows.clone());
            working_rows = new_rows;
        }

        temp_scope.insert_or_replace(db, cte.name.clone(), Table::new(columns, all_rows, vec![]));
    } else {
        let cte_result = execute_select_in_db(context, cte.query.clone(), db)?;
        temp_scope.insert_or_replace(db, cte.name.clone(), table_from_select_result(&cte_result));
    }

    Ok(())
}

fn column_definitions_from_result(
    headers: &[String],
    rows: &[Vec<Value>],
) -> Vec<ColumnDefinition> {
    headers
        .iter()
        .enumerate()
        .map(|(idx, name)| ColumnDefinition {
            name: name.clone(),
            data_type: rows
                .first()
                .and_then(|row| row.get(idx))
                .map(value_data_type)
                .unwrap_or(DataType::Text),
            nullable: true,
            primary_key: false,
            unique: false,
            default_value: None,
            foreign_key: None,
            check: None,
            auto_increment: false,
            generated: None,
        })
        .collect()
}

fn table_from_select_result(result: &SelectResult) -> Table {
    Table::new(
        column_definitions_from_result(&result.headers, &result.rows),
        result.rows.clone(),
        vec![],
    )
}

fn table_from_values_rows(
    values_rows: &[Vec<Expression>],
    col_aliases: &[String],
) -> Result<Table, RustqlError> {
    let empty_columns: Vec<ColumnDefinition> = Vec::new();
    let empty_row: Vec<Value> = Vec::new();

    let mut evaluated_rows: Vec<Vec<Value>> = Vec::new();
    for expr_row in values_rows {
        let mut row: Vec<Value> = Vec::new();
        for expr in expr_row {
            row.push(evaluate_value_expression(expr, &empty_columns, &empty_row)?);
        }
        evaluated_rows.push(row);
    }

    let num_cols = evaluated_rows.first().map(|row| row.len()).unwrap_or(0);
    let headers: Vec<String> = (0..num_cols)
        .map(|idx| {
            col_aliases
                .get(idx)
                .cloned()
                .unwrap_or_else(|| format!("column{}", idx + 1))
        })
        .collect();

    Ok(Table::new(
        column_definitions_from_result(&headers, &evaluated_rows),
        evaluated_rows,
        vec![],
    ))
}

fn value_data_type(value: &Value) -> DataType {
    match value {
        Value::Integer(_) => DataType::Integer,
        Value::Float(_) => DataType::Float,
        Value::Boolean(_) => DataType::Boolean,
        Value::Date(_) => DataType::Date,
        Value::Time(_) => DataType::Time,
        Value::DateTime(_) => DataType::DateTime,
        Value::Null | Value::Text(_) => DataType::Text,
    }
}

pub(crate) fn execute_select_internal(
    context: Option<&ExecutionContext>,
    stmt: SelectStatement,
    db: &dyn DatabaseCatalog,
) -> Result<SelectResult, RustqlError> {
    if let Some(plan) = plan_select_for_execution(db, &stmt)? {
        let execution = PlanExecutor::new(db).execute(&plan, &stmt)?;
        return Ok(SelectResult {
            headers: execution.columns,
            rows: execution.rows,
        });
    }

    if let Some((ref set_op_type, ref other_stmt)) = stmt.set_op {
        let left_stmt = SelectStatement {
            ctes: Vec::new(),
            distinct: stmt.distinct,
            distinct_on: stmt.distinct_on.clone(),
            columns: stmt.columns.clone(),
            from: stmt.from.clone(),
            from_alias: stmt.from_alias.clone(),
            from_subquery: stmt.from_subquery.clone(),
            from_function: stmt.from_function.clone(),
            joins: stmt.joins.clone(),
            where_clause: stmt.where_clause.clone(),
            group_by: stmt.group_by.clone(),
            having: stmt.having.clone(),
            order_by: stmt.order_by.clone(),
            limit: stmt.limit,
            offset: stmt.offset,
            fetch: stmt.fetch.clone(),
            set_op: None,
            window_definitions: Vec::new(),
            from_values: None,
        };
        return execute_set_operation_result(
            context,
            left_stmt,
            other_stmt.as_ref().clone(),
            set_op_type,
            db,
        );
    }

    if let Some(ref tf) = stmt.from_function
        && tf.name == "generate_series"
    {
        return execute_generate_series_result(stmt.clone(), tf);
    }

    if stmt.from.is_empty() {
        return execute_select_without_from(stmt);
    }

    let mut joined_rows_storage: Option<Vec<Vec<Value>>> = None;
    let all_columns = if stmt.joins.is_empty() {
        let table = db
            .get_table(&stmt.from)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.from.clone()))?;
        table.columns.clone()
    } else {
        let main_table = db
            .get_table(&stmt.from)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.from.clone()))?;
        let (joined_rows, all_cols) =
            perform_multiple_joins(context, db, main_table, &stmt.from, &stmt.joins)?;
        joined_rows_storage = Some(joined_rows);
        all_cols
    };

    let mut filtered_rows: Vec<&Vec<Value>> = Vec::new();

    if let Some(joined_rows) = joined_rows_storage.as_ref() {
        filtered_rows.reserve(joined_rows.len());
        for row in joined_rows {
            let include_row = if let Some(ref where_expr) = stmt.where_clause {
                evaluate_expression(Some(db), where_expr, &all_columns, row)?
            } else {
                true
            };
            if include_row {
                filtered_rows.push(row);
            }
        }
    } else {
        let table = db
            .get_table(&stmt.from)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.from.clone()))?;
        filtered_rows.reserve(table.rows.len());

        let candidate_indices: Option<HashSet<RowId>> = if let Some(ref where_expr) =
            stmt.where_clause
        {
            if let Some(index_usage) = super::ddl::find_index_usage(db, &stmt.from, where_expr) {
                super::ddl::get_indexed_rows(db, table, &index_usage).ok()
            } else {
                None
            }
        } else {
            None
        };

        for (row_id, row) in table.iter_rows_with_ids() {
            if candidate_indices
                .as_ref()
                .is_some_and(|candidate_set| !candidate_set.contains(&row_id))
            {
                continue;
            }
            let include_row = if let Some(ref where_expr) = stmt.where_clause {
                evaluate_expression(Some(db), where_expr, &all_columns, row)?
            } else {
                true
            };
            if include_row {
                filtered_rows.push(row);
            }
        }
    }

    if stmt.group_by.is_some() {
        let temp_table = Table::new(all_columns.clone(), Vec::new(), vec![]);
        let row_refs: Vec<&Vec<Value>> = filtered_rows.to_vec();
        return execute_select_with_grouping_result(stmt.clone(), &temp_table, row_refs);
    }

    let has_aggregate = stmt
        .columns
        .iter()
        .any(|col| matches!(col, Column::Function(_)));
    if has_aggregate {
        let temp_table = Table::new(all_columns.clone(), Vec::new(), vec![]);
        let row_refs: Vec<&Vec<Value>> = filtered_rows.to_vec();
        return execute_select_with_aggregates_result(stmt.clone(), &temp_table, row_refs);
    }

    enum ProjectionStep {
        Named(usize),
        Function,
        Subquery(Box<SelectStatement>),
        Window,
        Expression(Expression),
    }

    let column_specs: Vec<(String, Column)> = if matches!(stmt.columns[0], Column::All) {
        all_columns
            .iter()
            .map(|c| {
                (
                    c.name.clone(),
                    Column::Named {
                        name: c.name.clone(),
                        alias: None,
                    },
                )
            })
            .collect()
    } else {
        stmt.columns
            .iter()
            .map(|col| match col {
                Column::Named { name, alias } => {
                    (alias.clone().unwrap_or_else(|| name.clone()), col.clone())
                }
                Column::Function(agg) => (
                    agg.alias
                        .clone()
                        .unwrap_or_else(|| format!("{:?}", agg.function)),
                    col.clone(),
                ),
                Column::Subquery(_) => ("<subquery>".to_string(), col.clone()),
                Column::Expression { alias, .. } => (
                    alias.clone().unwrap_or_else(|| "<expression>".to_string()),
                    col.clone(),
                ),
                Column::All => unreachable!(),
            })
            .collect()
    };

    let projection_steps: Vec<ProjectionStep> = column_specs
        .iter()
        .map(|(_, col)| match col {
            Column::All => unreachable!(),
            Column::Named { name, .. } => Ok(ProjectionStep::Named(resolve_named_column_index(
                &all_columns,
                name,
            )?)),
            Column::Function(_) => Ok(ProjectionStep::Function),
            Column::Subquery(subquery) => Ok(ProjectionStep::Subquery(subquery.clone())),
            Column::Expression {
                expr: Expression::WindowFunction { .. },
                ..
            } => Ok(ProjectionStep::Window),
            Column::Expression { expr, .. } => Ok(ProjectionStep::Expression(expr.clone())),
        })
        .collect::<Result<_, RustqlError>>()?;

    let headers: Vec<String> = column_specs.iter().map(|(name, _)| name.clone()).collect();
    let projected_order_lookup = build_projected_order_lookup(&column_specs);
    let base_order_lookup = build_base_order_lookup(&all_columns);

    let has_window_fn = column_specs.iter().any(|(_, col)| {
        matches!(
            col,
            Column::Expression {
                expr: Expression::WindowFunction { .. },
                ..
            }
        )
    });

    let window_rows: Option<Vec<Vec<Value>>> = if has_window_fn {
        Some(evaluate_window_function_outputs(
            &filtered_rows,
            &all_columns,
            &stmt.columns,
        )?)
    } else {
        None
    };

    let project_row = |row_idx: usize, row: &Vec<Value>| -> Result<Vec<Value>, RustqlError> {
        let mut projected: Vec<Value> = Vec::with_capacity(column_specs.len());
        let mut wf_offset = 0;

        for step in &projection_steps {
            let val = match step {
                ProjectionStep::Named(idx) => row[*idx].clone(),
                ProjectionStep::Function => {
                    return Err(RustqlError::Internal(
                        "Aggregate functions in UNION must use GROUP BY".to_string(),
                    ));
                }
                ProjectionStep::Subquery(subquery) => {
                    eval_scalar_subquery_with_outer(db, subquery, &all_columns, row)?
                }
                ProjectionStep::Window => {
                    if let Some(ref wr) = window_rows {
                        let v = wr[row_idx].get(wf_offset).cloned().unwrap_or(Value::Null);
                        wf_offset += 1;
                        v
                    } else {
                        Value::Null
                    }
                }
                ProjectionStep::Expression(expr) => {
                    evaluate_value_expression_with_db(expr, &all_columns, row, Some(db))?
                }
            };
            projected.push(val);
        }

        Ok(projected)
    };

    if stmt.order_by.is_none()
        && stmt.distinct_on.is_none()
        && !stmt.distinct
        && stmt.offset.unwrap_or(0) == 0
        && stmt.limit.is_none()
        && stmt.fetch.is_none()
    {
        let mut rows: Vec<Vec<Value>> = Vec::with_capacity(filtered_rows.len());
        for (row_idx, row_ref) in filtered_rows.iter().enumerate() {
            rows.push(project_row(row_idx, row_ref)?);
        }
        return Ok(SelectResult { headers, rows });
    }

    let mut outputs: Vec<(Vec<Value>, Vec<Value>)> = Vec::with_capacity(filtered_rows.len());
    for (row_idx, row_ref) in filtered_rows.iter().enumerate() {
        let row = *row_ref;
        let projected = project_row(row_idx, row)?;

        let mut order_values: Vec<Value> =
            Vec::with_capacity(stmt.order_by.as_ref().map_or(0, Vec::len));
        if let Some(ref order_by) = stmt.order_by {
            for order_expr in order_by {
                let value = evaluate_select_order_expression(
                    &order_expr.expr,
                    row,
                    &projected,
                    &projected_order_lookup,
                    &base_order_lookup,
                    true,
                )?;
                order_values.push(value);
            }
        }

        outputs.push((projected, order_values));
    }

    if let Some(ref order_by) = stmt.order_by {
        outputs.sort_by(|a, b| {
            for (idx, order_expr) in order_by.iter().enumerate() {
                let cmp = compare_values_for_sort(&a.1[idx], &b.1[idx]);
                if cmp != Ordering::Equal {
                    return if order_expr.asc { cmp } else { cmp.reverse() };
                }
            }
            Ordering::Equal
        });
    }

    if let Some(ref distinct_on_exprs) = stmt.distinct_on {
        let mut seen_keys: BTreeSet<Vec<Value>> = BTreeSet::new();
        let mut deduped: Vec<(Vec<Value>, Vec<Value>)> = Vec::new();
        for (projected, order_vals) in outputs {
            let key: Vec<Value> = distinct_on_exprs
                .iter()
                .map(|expr| {
                    if let Expression::Column(name) = expr {
                        for (i, (header, _)) in column_specs.iter().enumerate() {
                            if header == name {
                                return projected[i].clone();
                            }
                        }
                    }
                    evaluate_value_expression_with_db(expr, &all_columns, &projected, Some(db))
                        .unwrap_or(Value::Null)
                })
                .collect();
            if seen_keys.insert(key) {
                deduped.push((projected, order_vals));
            }
        }
        outputs = deduped;
    }

    let offset = stmt.offset.unwrap_or(0);
    let base_limit = stmt.limit.unwrap_or(outputs.len());
    let limit = if let Some(ref fetch) = stmt.fetch {
        if fetch.with_ties && stmt.order_by.is_some() {
            let target = offset + fetch.count;
            if target < outputs.len() {
                let last_included = &outputs[target - 1].1;
                let mut extended = target;
                while extended < outputs.len() && outputs[extended].1 == *last_included {
                    extended += 1;
                }
                extended - offset
            } else {
                fetch.count
            }
        } else {
            fetch.count
        }
    } else {
        base_limit
    };

    let mut seen: BTreeSet<Vec<Value>> = BTreeSet::new();
    let mut emitted = 0usize;
    let mut skipped = 0usize;
    let mut rows: Vec<Vec<Value>> = Vec::new();

    for (projected, _) in outputs {
        if stmt.distinct && !seen.insert(projected.clone()) {
            continue;
        }
        if skipped < offset {
            skipped += 1;
            continue;
        }
        if emitted >= limit {
            break;
        }
        rows.push(projected);
        emitted += 1;
    }

    Ok(SelectResult { headers, rows })
}

pub fn eval_subquery_values(
    db: &dyn DatabaseCatalog,
    subquery: &SelectStatement,
) -> Result<Vec<Value>, RustqlError> {
    if subquery.columns.len() != 1 {
        return Err(RustqlError::Internal(
            "Subquery in IN must select exactly one column".to_string(),
        ));
    }
    let table = db
        .get_table(&subquery.from)
        .ok_or_else(|| RustqlError::TableNotFound(subquery.from.clone()))?;

    let mut filtered_rows: Vec<&Vec<Value>> = Vec::new();

    if subquery.joins.is_empty() {
        let candidate_indices: Option<HashSet<RowId>> = if let Some(ref where_expr) =
            subquery.where_clause
        {
            if let Some(index_usage) = super::ddl::find_index_usage(db, &subquery.from, where_expr)
            {
                super::ddl::get_indexed_rows(db, table, &index_usage).ok()
            } else {
                None
            }
        } else {
            None
        };

        if let Some(ref candidate_set) = candidate_indices {
            for (_, row) in table
                .iter_rows_with_ids()
                .filter(|(row_id, _)| candidate_set.contains(row_id))
            {
                let include_row = if let Some(ref where_expr) = subquery.where_clause {
                    evaluate_expression(Some(db), where_expr, &table.columns, row)?
                } else {
                    true
                };
                if include_row {
                    filtered_rows.push(row);
                }
            }
        } else {
            for (_, row) in table.iter_rows_with_ids() {
                let include_row = if let Some(ref where_expr) = subquery.where_clause {
                    evaluate_expression(Some(db), where_expr, &table.columns, row)?
                } else {
                    true
                };
                if include_row {
                    filtered_rows.push(row);
                }
            }
        }
    } else {
        for row in &table.rows {
            let include_row = if let Some(ref where_expr) = subquery.where_clause {
                evaluate_expression(Some(db), where_expr, &table.columns, row)?
            } else {
                true
            };
            if include_row {
                filtered_rows.push(row);
            }
        }
    }

    match &subquery.columns[0] {
        Column::All => Err(RustqlError::Internal(
            "Subquery in IN cannot use *".to_string(),
        )),
        Column::Expression { .. } => Err(RustqlError::Internal(
            "Subquery in IN cannot use expressions".to_string(),
        )),
        Column::Subquery(scalar_subquery) => {
            let mut values = Vec::with_capacity(filtered_rows.len());
            for row in &filtered_rows {
                let scalar_value =
                    eval_scalar_subquery_with_outer(db, scalar_subquery, &table.columns, row)?;
                values.push(scalar_value);
            }
            Ok(values)
        }
        Column::Function(agg) => {
            if let Some(group_by_clause) = &subquery.group_by {
                let gb_exprs = group_by_clause.exprs();
                let mut groups: std::collections::BTreeMap<Vec<Value>, Vec<&Vec<Value>>> =
                    std::collections::BTreeMap::new();
                for row in &filtered_rows {
                    let key: Vec<Value> = gb_exprs
                        .iter()
                        .map(|expr| {
                            evaluate_value_expression(expr, &table.columns, row)
                                .or_else(|_| {
                                    evaluate_value_expression_with_db(
                                        expr,
                                        &table.columns,
                                        row,
                                        Some(db),
                                    )
                                })
                                .unwrap_or(Value::Null)
                        })
                        .collect();
                    groups.entry(key).or_default().push(*row);
                }
                let mut values = Vec::with_capacity(groups.len());
                for (_k, rows) in groups {
                    if let Some(ref having_expr) = subquery.having
                        && !evaluate_having(having_expr, &subquery.columns, table, &rows)?
                    {
                        continue;
                    }
                    let v =
                        compute_aggregate(&agg.function, &agg.expr, table, &rows, agg.distinct)?;
                    values.push(v);
                }
                Ok(values)
            } else {
                let value = compute_aggregate(
                    &agg.function,
                    &agg.expr,
                    table,
                    &filtered_rows,
                    agg.distinct,
                )?;
                Ok(vec![value])
            }
        }
        Column::Named { name, .. } => {
            if let Some(group_by_clause) = &subquery.group_by {
                let gb_exprs = group_by_clause.exprs();
                let mut groups: std::collections::BTreeMap<Vec<Value>, Vec<&Vec<Value>>> =
                    std::collections::BTreeMap::new();
                for row in &filtered_rows {
                    let key: Vec<Value> = gb_exprs
                        .iter()
                        .map(|expr| {
                            evaluate_value_expression(expr, &table.columns, row)
                                .or_else(|_| {
                                    evaluate_value_expression_with_db(
                                        expr,
                                        &table.columns,
                                        row,
                                        Some(db),
                                    )
                                })
                                .unwrap_or(Value::Null)
                        })
                        .collect();
                    groups.entry(key).or_default().push(*row);
                }

                let named_idx = table
                    .columns
                    .iter()
                    .position(|c| &c.name == name)
                    .ok_or_else(|| RustqlError::ColumnNotFound(name.clone()))?;
                let is_in_group_by = gb_exprs.iter().any(|expr| {
                    matches!(expr, Expression::Column(col_name) if col_name == name
                        || col_name.split('.').next_back() == Some(name.as_str()))
                });
                if !is_in_group_by {
                    return Err(RustqlError::Internal(format!(
                        "Column '{}' must appear in GROUP BY clause",
                        name
                    )));
                }
                let mut values = Vec::with_capacity(groups.len());
                for (_k, rows) in groups {
                    values.push(rows[0][named_idx].clone());
                }
                Ok(values)
            } else {
                let idx = table
                    .columns
                    .iter()
                    .position(|c| &c.name == name)
                    .ok_or_else(|| RustqlError::ColumnNotFound(name.clone()))?;
                let mut values = Vec::with_capacity(filtered_rows.len());
                for row in filtered_rows {
                    values.push(row[idx].clone());
                }
                Ok(values)
            }
        }
    }
}

fn scalar_subquery_requires_materialization(subquery: &SelectStatement) -> bool {
    subquery.order_by.is_some() || subquery.offset.unwrap_or(0) > 0 || subquery.limit.is_some()
}

fn remember_lookup_name(lookup: &mut HashMap<String, usize>, name: &str, index: usize) {
    lookup.entry(name.to_string()).or_insert(index);
}

fn build_projected_order_lookup(column_specs: &[(String, Column)]) -> HashMap<String, usize> {
    let mut lookup = HashMap::new();
    for (index, (header, col_spec)) in column_specs.iter().enumerate() {
        remember_lookup_name(&mut lookup, header, index);
        if let Column::Named { name, alias } = col_spec {
            if let Some(alias) = alias {
                remember_lookup_name(&mut lookup, alias, index);
            }
            remember_lookup_name(&mut lookup, name, index);
            remember_lookup_name(
                &mut lookup,
                name.split('.').next_back().unwrap_or(name),
                index,
            );
        }
    }
    lookup
}

fn build_base_order_lookup(columns: &[ColumnDefinition]) -> HashMap<String, usize> {
    let mut lookup = HashMap::new();
    for (index, column) in columns.iter().enumerate() {
        remember_lookup_name(&mut lookup, &column.name, index);
    }
    lookup
}

fn resolve_named_column_index(
    columns: &[ColumnDefinition],
    name: &str,
) -> Result<usize, RustqlError> {
    columns
        .iter()
        .position(|c| {
            c.name
                == (if name.contains('.') {
                    name.split('.').next_back().unwrap_or(name)
                } else {
                    name
                })
        })
        .ok_or_else(|| RustqlError::ColumnNotFound(name.to_string()))
}

fn combine_outer_inner_rows(outer_row: &[Value], inner_row: &[Value]) -> Vec<Value> {
    let mut combined_row = Vec::with_capacity(outer_row.len() + inner_row.len());
    combined_row.extend_from_slice(outer_row);
    combined_row.extend_from_slice(inner_row);
    combined_row
}

fn reset_combined_row(buffer: &mut Vec<Value>, outer_row: &[Value], inner_row: &[Value]) {
    buffer.clear();
    buffer.extend_from_slice(outer_row);
    buffer.extend_from_slice(inner_row);
}

fn apply_scalar_order_and_slice(
    rows: &mut Vec<Vec<Value>>,
    order_by: Option<&[OrderByExpr]>,
    combined_columns: &[ColumnDefinition],
    offset: Option<usize>,
    limit: Option<usize>,
) -> Result<(), RustqlError> {
    if let Some(order_by) = order_by {
        let mut ordered_rows: Vec<(Vec<Value>, Vec<Value>)> = rows
            .drain(..)
            .map(|row| {
                let order_values = order_by
                    .iter()
                    .map(|ob| {
                        evaluate_value_expression(&ob.expr, combined_columns, &row)
                            .unwrap_or(Value::Null)
                    })
                    .collect();
                (row, order_values)
            })
            .collect();
        ordered_rows.sort_by(|a, b| {
            for (index, ob) in order_by.iter().enumerate() {
                let ord = compare_values_for_sort(&a.1[index], &b.1[index]);
                if ord != std::cmp::Ordering::Equal {
                    return if ob.asc { ord } else { ord.reverse() };
                }
            }
            std::cmp::Ordering::Equal
        });
        rows.extend(ordered_rows.into_iter().map(|(row, _)| row));
    }
    let start = offset.unwrap_or(0);
    let end = if let Some(limit) = limit {
        start.saturating_add(limit)
    } else {
        rows.len()
    };
    let end = end.min(rows.len());
    if start >= rows.len() {
        rows.clear();
    } else {
        rows.drain(0..start);
        rows.truncate(end - start);
    }
    Ok(())
}

fn record_scalar_value(slot: &mut Option<Value>, value: Value) -> Result<(), RustqlError> {
    if slot.replace(value).is_some() {
        Err(RustqlError::Internal(
            "Scalar subquery returned more than one row".to_string(),
        ))
    } else {
        Ok(())
    }
}

pub fn eval_subquery_exists_with_outer(
    db: &dyn DatabaseCatalog,
    subquery: &SelectStatement,
    outer_columns: &[ColumnDefinition],
    outer_row: &[Value],
) -> Result<bool, RustqlError> {
    if !subquery.joins.is_empty() {
        return eval_subquery_exists_with_joins(db, subquery, outer_columns, outer_row);
    }

    let table = db
        .get_table(&subquery.from)
        .ok_or_else(|| RustqlError::TableNotFound(subquery.from.clone()))?;

    if subquery.where_clause.is_none() {
        return Ok(!table.rows.is_empty());
    }

    let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
    combined_columns.extend(table.columns.clone());
    let mut combined_row = Vec::with_capacity(outer_row.len() + table.columns.len());

    for inner_row in &table.rows {
        reset_combined_row(&mut combined_row, outer_row, inner_row);
        let include_row = evaluate_expression(
            Some(db),
            subquery.where_clause.as_ref().unwrap(),
            &combined_columns,
            &combined_row,
        )?;
        if include_row {
            return Ok(true);
        }
    }
    Ok(false)
}

fn eval_subquery_exists_with_joins(
    db: &dyn DatabaseCatalog,
    subquery: &SelectStatement,
    outer_columns: &[ColumnDefinition],
    outer_row: &[Value],
) -> Result<bool, RustqlError> {
    let main_table = db
        .get_table(&subquery.from)
        .ok_or_else(|| RustqlError::TableNotFound(subquery.from.clone()))?;

    let (joined_rows, all_subquery_columns) =
        perform_multiple_joins(None, db, main_table, &subquery.from, &subquery.joins)?;

    if subquery.where_clause.is_none() {
        return Ok(!joined_rows.is_empty());
    }

    let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
    combined_columns.extend(all_subquery_columns.clone());
    let mut combined_row = Vec::with_capacity(outer_row.len() + all_subquery_columns.len());

    for sub_row in joined_rows {
        reset_combined_row(&mut combined_row, outer_row, &sub_row);
        let include_row = evaluate_expression(
            Some(db),
            subquery.where_clause.as_ref().unwrap(),
            &combined_columns,
            &combined_row,
        )?;
        if include_row {
            return Ok(true);
        }
    }
    Ok(false)
}

pub fn eval_scalar_subquery_with_outer(
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

    if !subquery.joins.is_empty() {
        let main_table = db
            .get_table(&subquery.from)
            .ok_or_else(|| RustqlError::TableNotFound(subquery.from.clone()))?;

        let (joined_rows, all_subquery_columns) =
            perform_multiple_joins(None, db, main_table, &subquery.from, &subquery.joins)?;

        let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
        combined_columns.extend(all_subquery_columns.clone());
        let joined_rows_len = joined_rows.len();
        match &subquery.columns[0] {
            Column::All => {
                return Err(RustqlError::Internal(
                    "Scalar subquery cannot use *".to_string(),
                ));
            }
            Column::Expression { .. } => {
                return Err(RustqlError::Internal(
                    "Scalar subquery cannot use expressions".to_string(),
                ));
            }
            Column::Function(agg) => {
                let mut subquery_rows: Vec<Vec<Value>> = Vec::with_capacity(joined_rows_len);
                let mut combined_row =
                    Vec::with_capacity(outer_row.len() + all_subquery_columns.len());
                for sub_row in joined_rows {
                    reset_combined_row(&mut combined_row, outer_row, &sub_row);
                    let include_row = if let Some(ref where_expr) = subquery.where_clause {
                        evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
                    } else {
                        true
                    };
                    if include_row {
                        subquery_rows.push(sub_row);
                    }
                }

                let temp_table = Table::new(all_subquery_columns.clone(), subquery_rows, vec![]);
                let filtered_rows: Vec<&Vec<Value>> = temp_table.rows.iter().collect();

                return compute_aggregate(
                    &agg.function,
                    &agg.expr,
                    &temp_table,
                    &filtered_rows,
                    agg.distinct,
                );
            }
            Column::Named { name, .. } if !scalar_subquery_requires_materialization(subquery) => {
                let col_idx = resolve_named_column_index(&combined_columns, name)?;
                let mut result = None;
                let mut combined_row =
                    Vec::with_capacity(outer_row.len() + all_subquery_columns.len());
                for sub_row in joined_rows {
                    reset_combined_row(&mut combined_row, outer_row, &sub_row);
                    let include_row = if let Some(ref where_expr) = subquery.where_clause {
                        evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
                    } else {
                        true
                    };
                    if include_row {
                        record_scalar_value(&mut result, combined_row[col_idx].clone())?;
                    }
                }
                return Ok(result.unwrap_or(Value::Null));
            }
            Column::Subquery(nested) if !scalar_subquery_requires_materialization(subquery) => {
                let mut result = None;
                let mut combined_row =
                    Vec::with_capacity(outer_row.len() + all_subquery_columns.len());
                for sub_row in joined_rows {
                    reset_combined_row(&mut combined_row, outer_row, &sub_row);
                    let include_row = if let Some(ref where_expr) = subquery.where_clause {
                        evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
                    } else {
                        true
                    };
                    if include_row {
                        let value = eval_scalar_subquery_with_outer(
                            db,
                            nested,
                            &combined_columns,
                            &combined_row,
                        )?;
                        record_scalar_value(&mut result, value)?;
                    }
                }
                return Ok(result.unwrap_or(Value::Null));
            }
            _ => {}
        }

        let mut candidate_rows: Vec<Vec<Value>> = Vec::with_capacity(joined_rows_len);
        for sub_row in joined_rows {
            let combined_row = combine_outer_inner_rows(outer_row, &sub_row);
            let include_row = if let Some(ref where_expr) = subquery.where_clause {
                evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
            } else {
                true
            };
            if include_row {
                candidate_rows.push(combined_row);
            }
        }

        apply_scalar_order_and_slice(
            &mut candidate_rows,
            subquery.order_by.as_deref(),
            &combined_columns,
            subquery.offset,
            subquery.limit,
        )?;

        return match &subquery.columns[0] {
            Column::All => Err(RustqlError::Internal(
                "Scalar subquery cannot use *".to_string(),
            )),
            Column::Expression { .. } => Err(RustqlError::Internal(
                "Scalar subquery cannot use expressions".to_string(),
            )),
            Column::Named { name, .. } => {
                let col_idx = resolve_named_column_index(&combined_columns, name)?;
                match candidate_rows.len() {
                    0 => Ok(Value::Null),
                    1 => Ok(candidate_rows[0][col_idx].clone()),
                    _ => Err(RustqlError::Internal(
                        "Scalar subquery returned more than one row".to_string(),
                    )),
                }
            }
            Column::Subquery(nested) => {
                let mut result = None;
                for combined_row in candidate_rows {
                    let value = eval_scalar_subquery_with_outer(
                        db,
                        nested,
                        &combined_columns,
                        &combined_row,
                    )?;
                    record_scalar_value(&mut result, value)?;
                }
                Ok(result.unwrap_or(Value::Null))
            }
            Column::Function(_) => Err(RustqlError::Internal(
                "Scalar subquery cannot use aggregate functions".to_string(),
            )),
        };
    }

    let table = db
        .get_table(&subquery.from)
        .ok_or_else(|| RustqlError::TableNotFound(subquery.from.clone()))?;

    let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
    combined_columns.extend(table.columns.clone());

    if let Column::Function(agg) = &subquery.columns[0] {
        let mut filtered_rows: Vec<&Vec<Value>> = Vec::new();
        let mut combined_row = Vec::with_capacity(outer_row.len() + table.columns.len());
        for inner_row in &table.rows {
            reset_combined_row(&mut combined_row, outer_row, inner_row);

            let include_row = if let Some(ref where_expr) = subquery.where_clause {
                evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
            } else {
                true
            };
            if include_row {
                filtered_rows.push(inner_row);
            }
        }
        return compute_aggregate(
            &agg.function,
            &agg.expr,
            table,
            &filtered_rows,
            agg.distinct,
        );
    }

    match &subquery.columns[0] {
        Column::All => Err(RustqlError::Internal(
            "Scalar subquery cannot use *".to_string(),
        )),
        Column::Expression { .. } => Err(RustqlError::Internal(
            "Scalar subquery cannot use expressions".to_string(),
        )),
        Column::Subquery(nested_subquery)
            if !scalar_subquery_requires_materialization(subquery) =>
        {
            let mut result = None;
            let mut combined_row = Vec::with_capacity(outer_row.len() + table.columns.len());
            for inner_row in &table.rows {
                reset_combined_row(&mut combined_row, outer_row, inner_row);
                let include_row = if let Some(ref where_expr) = subquery.where_clause {
                    evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
                } else {
                    true
                };
                if include_row {
                    let value = eval_scalar_subquery_with_outer(
                        db,
                        nested_subquery,
                        &combined_columns,
                        &combined_row,
                    )?;
                    record_scalar_value(&mut result, value)?;
                }
            }
            Ok(result.unwrap_or(Value::Null))
        }
        Column::Named { name, .. } if !scalar_subquery_requires_materialization(subquery) => {
            let col_idx = resolve_named_column_index(&combined_columns, name)?;
            let mut result = None;
            let mut combined_row = Vec::with_capacity(outer_row.len() + table.columns.len());
            for inner_row in &table.rows {
                reset_combined_row(&mut combined_row, outer_row, inner_row);
                let include_row = if let Some(ref where_expr) = subquery.where_clause {
                    evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
                } else {
                    true
                };
                if include_row {
                    record_scalar_value(&mut result, combined_row[col_idx].clone())?;
                }
            }
            Ok(result.unwrap_or(Value::Null))
        }
        _ => {
            let mut candidate_rows: Vec<Vec<Value>> = Vec::with_capacity(table.rows.len());
            for inner_row in &table.rows {
                let combined_row = combine_outer_inner_rows(outer_row, inner_row);

                let include_row = if let Some(ref where_expr) = subquery.where_clause {
                    evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
                } else {
                    true
                };
                if include_row {
                    candidate_rows.push(combined_row);
                }
            }

            apply_scalar_order_and_slice(
                &mut candidate_rows,
                subquery.order_by.as_deref(),
                &combined_columns,
                subquery.offset,
                subquery.limit,
            )?;

            match &subquery.columns[0] {
                Column::All => Err(RustqlError::Internal(
                    "Scalar subquery cannot use *".to_string(),
                )),
                Column::Expression { .. } => Err(RustqlError::Internal(
                    "Scalar subquery cannot use expressions".to_string(),
                )),
                Column::Function(_) => Err(RustqlError::Internal(
                    "Scalar subquery cannot use aggregate functions".to_string(),
                )),
                Column::Subquery(nested_subquery) => {
                    let mut result = None;
                    for combined_row in candidate_rows {
                        let value = eval_scalar_subquery_with_outer(
                            db,
                            nested_subquery,
                            &combined_columns,
                            &combined_row,
                        )?;
                        record_scalar_value(&mut result, value)?;
                    }
                    Ok(result.unwrap_or(Value::Null))
                }
                Column::Named { name, .. } => {
                    let col_idx = resolve_named_column_index(&combined_columns, name)?;
                    match candidate_rows.len() {
                        0 => Ok(Value::Null),
                        1 => Ok(candidate_rows[0][col_idx].clone()),
                        _ => Err(RustqlError::Internal(
                            "Scalar subquery returned more than one row".to_string(),
                        )),
                    }
                }
            }
        }
    }
}

fn resolve_window_definitions(stmt: &mut SelectStatement) {
    if stmt.window_definitions.is_empty() {
        return;
    }
    let defs = stmt.window_definitions.clone();
    for col in &mut stmt.columns {
        if let Column::Expression {
            expr:
                Expression::WindowFunction {
                    partition_by,
                    order_by,
                    frame,
                    ..
                },
            ..
        } = col
            && partition_by.len() == 1
            && let Expression::Column(ref name) = partition_by[0]
            && let Some(ref_name) = name.strip_prefix("__window_ref:")
            && let Some(def) = defs.iter().find(|d| d.name == ref_name)
        {
            *partition_by = def.partition_by.clone();
            *order_by = def.order_by.clone();
            *frame = def.frame.clone();
        }
    }
}
