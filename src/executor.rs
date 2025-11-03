use crate::ast::*;
use crate::database::{Database, Table};
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};
use std::sync::{Mutex, OnceLock};

static DATABASE: OnceLock<Mutex<Database>> = OnceLock::new();

fn get_database() -> std::sync::MutexGuard<'static, Database> {
    #[cfg(test)]
    {
        DATABASE
            .get_or_init(|| Mutex::new(Database::new()))
            .lock()
            .unwrap()
    }
    #[cfg(not(test))]
    {
        DATABASE
            .get_or_init(|| Mutex::new(Database::load()))
            .lock()
            .unwrap()
    }
}

pub fn execute(statement: Statement) -> Result<String, String> {
    match statement {
        Statement::CreateTable(stmt) => execute_create_table(stmt),
        Statement::DropTable(stmt) => execute_drop_table(stmt),
        Statement::Insert(stmt) => execute_insert(stmt),
        Statement::Select(stmt) => execute_select(stmt),
        Statement::Update(stmt) => execute_update(stmt),
        Statement::Delete(stmt) => execute_delete(stmt),
        Statement::AlterTable(stmt) => execute_alter_table(stmt),
    }
}

pub fn reset_database_state() {
    let mut db = get_database();
    db.tables.clear();
}

fn execute_create_table(stmt: CreateTableStatement) -> Result<String, String> {
    let mut db = get_database();
    if db.tables.contains_key(&stmt.name) {
        return Err(format!("Table '{}' already exists", stmt.name));
    }
    db.tables.insert(
        stmt.name.clone(),
        Table {
            columns: stmt.columns,
            rows: Vec::new(),
        },
    );
    db.save()?;
    Ok(format!("Table '{}' created", stmt.name))
}

fn execute_drop_table(stmt: DropTableStatement) -> Result<String, String> {
    let mut db = get_database();
    if db.tables.remove(&stmt.name).is_some() {
        db.save()?;
        Ok(format!("Table '{}' dropped", stmt.name))
    } else {
        Err(format!("Table '{}' does not exist", stmt.name))
    }
}

fn execute_insert(stmt: InsertStatement) -> Result<String, String> {
    let mut db = get_database();
    let table = db
        .tables
        .get_mut(&stmt.table)
        .ok_or_else(|| format!("Table '{}' does not exist", stmt.table))?;
    let row_count = stmt.values.len();
    for values in stmt.values {
        if values.len() != table.columns.len() {
            return Err(format!(
                "Column count mismatch: expected {}, got {}",
                table.columns.len(),
                values.len()
            ));
        }
        table.rows.push(values);
    }
    db.save()?;
    Ok(format!("{} row(s) inserted", row_count))
}

fn execute_select(stmt: SelectStatement) -> Result<String, String> {
    let db = get_database();
    
    if !stmt.joins.is_empty() {
        return execute_select_with_joins(stmt, &db);
    }
    
    let db_ref: &Database = &db;
    let table = db_ref
        .tables
        .get(&stmt.from)
        .ok_or_else(|| format!("Table '{}' does not exist", stmt.from))?;

    let mut filtered_rows: Vec<&Vec<Value>> = Vec::new();
    for row in &table.rows {
        let include_row = if let Some(ref where_expr) = stmt.where_clause {
            evaluate_expression(Some(db_ref), where_expr, &table.columns, row)?
        } else {
            true
        };
        if include_row {
            filtered_rows.push(row);
        }
    }

    if stmt.group_by.is_some() {
        return execute_select_with_grouping(stmt, table, filtered_rows);
    }

    let has_aggregate = stmt
        .columns
        .iter()
        .any(|col| matches!(col, Column::Function(_)));

    if has_aggregate {
        return execute_select_with_aggregates(stmt, table, filtered_rows);
    }

    let column_specs: Vec<(String, Column)> = if matches!(stmt.columns[0], Column::All) {
        table.columns.iter().map(|c| (c.name.clone(), Column::Named(c.name.clone()))).collect()
    } else {
        stmt.columns.iter().map(|col| {
            match col {
                Column::Named(name) => (name.clone(), Column::Named(name.clone())),
                Column::Subquery(_) => ("<subquery>".to_string(), col.clone()),
                Column::Function(_) => ("<aggregate>".to_string(), col.clone()),
                Column::All => unreachable!(),
            }
        }).collect()
    };

    if let Some(ref order_by) = stmt.order_by {
        filtered_rows.sort_by(|a, b| {
            for order_expr in order_by {
                let a_val = evaluate_value_expression(&order_expr.expr, &table.columns, a)
                    .unwrap_or(Value::Null);
                let b_val = evaluate_value_expression(&order_expr.expr, &table.columns, b)
                    .unwrap_or(Value::Null);
                let cmp = compare_values_for_sort(&a_val, &b_val);
                if cmp != Ordering::Equal {
                    return if order_expr.asc { cmp } else { cmp.reverse() };
                }
            }
            Ordering::Equal
        });
    }

    let offset = stmt.offset.unwrap_or(0);
    let limit = stmt.limit.unwrap_or(filtered_rows.len());

    let mut result = String::new();
    for (name, _) in &column_specs {
        result.push_str(&format!("{}\t", name));
    }
    result.push('\n');
    result.push_str(&"-".repeat(40));
    result.push('\n');

    use std::collections::BTreeSet;
    let mut seen: BTreeSet<Vec<Value>> = BTreeSet::new();
    let mut emitted = 0usize;
    let mut skipped = 0usize;
    for row in filtered_rows.iter() {
        let mut projected: Vec<Value> = Vec::new();
        for (_, col) in &column_specs {
            let val = match col {
                Column::All => {
                    unreachable!("Column::All should not appear in column_specs")
                }
                Column::Named(name) => {
                    let idx = table
                        .columns
                        .iter()
                        .position(|c| &c.name == name)
                        .ok_or_else(|| format!("Column '{}' not found", name))?;
                    row[idx].clone()
                }
                Column::Subquery(subquery) => {
                    eval_scalar_subquery_with_outer(db_ref, subquery, &table.columns, row)?
                }
                Column::Function(_) => {
                    return Err("Aggregate functions must be used with GROUP BY or without other columns".to_string());
                }
            };
            projected.push(val);
        }
        if stmt.distinct
            && !seen.insert(projected.clone()) {
                continue;
            }
        if skipped < offset {
            skipped += 1;
            continue;
        }
        if emitted >= limit {
            break;
        }
        for val in &projected {
            result.push_str(&format!("{}\t", format_value(val)));
        }
        result.push('\n');
        emitted += 1;
    }

    Ok(result)
}

fn execute_select_with_aggregates(
    stmt: SelectStatement,
    table: &Table,
    rows: Vec<&Vec<Value>>,
) -> Result<String, String> {
    let mut result = String::new();
    for col in &stmt.columns {
        match col {
            Column::Function(agg) => {
                let name = agg
                    .alias
                    .clone()
                    .unwrap_or_else(|| format!("{:?}(*)", agg.function));
                result.push_str(&format!("{}\t", name));
            }
            Column::Named(name) => {
                result.push_str(&format!("{}\t", name));
            }
            _ => {}
        }
    }
    result.push('\n');
    result.push_str(&"-".repeat(40));
    result.push('\n');

    for col in &stmt.columns {
        match col {
            Column::Function(agg) => {
                let value = compute_aggregate(&agg.function, &agg.expr, table, &rows)?;
                result.push_str(&format!("{}\t", format_value(&value)));
            }
            _ => {
                return Err(
                    "Cannot mix aggregate and non-aggregate columns without GROUP BY".to_string(),
                );
            }
        }
    }
    result.push('\n');
    Ok(result)
}

fn execute_select_with_grouping(
    stmt: SelectStatement,
    table: &Table,
    rows: Vec<&Vec<Value>>,
) -> Result<String, String> {
    let group_by_indices: Vec<usize> = stmt
        .group_by
        .as_ref()
        .unwrap()
        .iter()
        .map(|name| {
            table
                .columns
                .iter()
                .position(|c| &c.name == name)
                .ok_or_else(|| format!("Column '{}' not found", name))
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut groups: BTreeMap<Vec<Value>, Vec<&Vec<Value>>> = BTreeMap::new();
    for row in rows {
        let key: Vec<Value> = group_by_indices
            .iter()
            .map(|&idx| row[idx].clone())
            .collect();
        groups.entry(key).or_default().push(row);
    }

    let mut result = String::new();
    for col in &stmt.columns {
        match col {
            Column::Function(agg) => {
                let name = agg
                    .alias
                    .clone()
                    .unwrap_or_else(|| format!("{:?}(*)", agg.function));
                result.push_str(&format!("{}\t", name));
            }
            Column::Named(name) => {
                result.push_str(&format!("{}\t", name));
            }
            _ => {}
        }
    }
    result.push('\n');
    result.push_str(&"-".repeat(40));
    result.push('\n');

    for (_group_key, group_rows) in groups {
        if let Some(ref having_expr) = stmt.having {
            let should_include = evaluate_having(having_expr, &stmt.columns, table, &group_rows)?;
            if !should_include {
                continue;
            }
        }
        for col in &stmt.columns {
            match col {
                Column::Function(agg) => {
                    let value = compute_aggregate(&agg.function, &agg.expr, table, &group_rows)?;
                    result.push_str(&format!("{}\t", format_value(&value)));
                }
                Column::Named(name) => {
                    if let Some(idx) = stmt
                        .group_by
                        .as_ref()
                        .unwrap()
                        .iter()
                        .position(|n| n == name)
                    {
                        let group_idx = group_by_indices[idx];
                        result.push_str(&format!("{}\t", format_value(&group_rows[0][group_idx])));
                    } else {
                        return Err(format!("Column '{}' must appear in GROUP BY clause", name));
                    }
                }
                _ => {}
            }
        }
        result.push('\n');
    }
    Ok(result)
}

fn compute_aggregate(
    func: &AggregateFunctionType,
    expr: &Expression,
    table: &Table,
    rows: &[&Vec<Value>],
) -> Result<Value, String> {
    match func {
        AggregateFunctionType::Count => {
            if matches!(expr, Expression::Column(name) if name == "*") {
                Ok(Value::Integer(rows.len() as i64))
            } else {
                let mut count = 0;
                for row in rows {
                    let val = evaluate_value_expression(expr, &table.columns, row)?;
                    if !matches!(val, Value::Null) {
                        count += 1;
                    }
                }
                Ok(Value::Integer(count))
            }
        }
        AggregateFunctionType::Sum => {
            let mut sum = 0.0;
            let mut has_value = false;
            for row in rows {
                let val = evaluate_value_expression(expr, &table.columns, row)?;
                match val {
                    Value::Integer(n) => {
                        sum += n as f64;
                        has_value = true;
                    }
                    Value::Float(f) => {
                        sum += f;
                        has_value = true;
                    }
                    Value::Null => {}
                    _ => return Err("SUM requires numeric values".to_string()),
                }
            }
            if has_value {
                Ok(Value::Float(sum))
            } else {
                Ok(Value::Null)
            }
        }
        AggregateFunctionType::Avg => {
            let mut sum = 0.0;
            let mut count = 0;
            for row in rows {
                let val = evaluate_value_expression(expr, &table.columns, row)?;
                match val {
                    Value::Integer(n) => {
                        sum += n as f64;
                        count += 1;
                    }
                    Value::Float(f) => {
                        sum += f;
                        count += 1;
                    }
                    Value::Null => {}
                    _ => return Err("AVG requires numeric values".to_string()),
                }
            }
            if count > 0 {
                Ok(Value::Float(sum / count as f64))
            } else {
                Ok(Value::Null)
            }
        }
        AggregateFunctionType::Min => {
            let mut min_val: Option<Value> = None;
            for row in rows {
                let val = evaluate_value_expression(expr, &table.columns, row)?;
                if !matches!(val, Value::Null) {
                    min_val = Some(match min_val {
                        None => val,
                        Some(ref current) => {
                            if compare_values_for_sort(&val, current) == Ordering::Less {
                                val
                            } else {
                                current.clone()
                            }
                        }
                    });
                }
            }
            Ok(min_val.unwrap_or(Value::Null))
        }
        AggregateFunctionType::Max => {
            let mut max_val: Option<Value> = None;
            for row in rows {
                let val = evaluate_value_expression(expr, &table.columns, row)?;
                if !matches!(val, Value::Null) {
                    max_val = Some(match max_val {
                        None => val,
                        Some(ref current) => {
                            if compare_values_for_sort(&val, current) == Ordering::Greater {
                                val
                            } else {
                                current.clone()
                            }
                        }
                    });
                }
            }
            Ok(max_val.unwrap_or(Value::Null))
        }
    }
}

fn evaluate_having(
    expr: &Expression,
    _columns: &[Column],
    table: &Table,
    rows: &[&Vec<Value>],
) -> Result<bool, String> {
    match expr {
        Expression::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(evaluate_having(left, _columns, table, rows)?
                && evaluate_having(right, _columns, table, rows)?),
            BinaryOperator::Or => Ok(evaluate_having(left, _columns, table, rows)?
                || evaluate_having(right, _columns, table, rows)?),
            _ => {
                let left_val = evaluate_having_value(left, _columns, table, rows)?;
                let right_val = evaluate_having_value(right, _columns, table, rows)?;
                compare_values(&left_val, op, &right_val)
            }
        },
        Expression::IsNull { expr, not } => {
            let value = evaluate_having_value(expr, _columns, table, rows)?;
            let is_null = matches!(value, Value::Null);
            Ok(if *not { !is_null } else { is_null })
        }
        Expression::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => Ok(!evaluate_having(expr, _columns, table, rows)?),
            _ => Err("Unsupported unary operation in HAVING clause".to_string()),
        },
        _ => Err("Invalid expression in HAVING clause".to_string()),
    }
}

fn evaluate_having_value(
    expr: &Expression,
    _columns: &[Column],
    table: &Table,
    rows: &[&Vec<Value>],
) -> Result<Value, String> {
    match expr {
        Expression::Function(agg) => compute_aggregate(&agg.function, &agg.expr, table, rows),
        Expression::Value(val) => Ok(val.clone()),
        Expression::Column(name) => {
            if !rows.is_empty() {
                if let Some(idx) = table.columns.iter().position(|c| &c.name == name) {
                    Ok(rows[0][idx].clone())
                } else {
                    Err(format!("Column '{}' not found in HAVING clause", name))
                }
            } else {
                Err("No rows in group for HAVING clause".to_string())
            }
        }
        _ => Err("Complex expressions not yet supported in HAVING".to_string()),
    }
}

fn execute_update(stmt: UpdateStatement) -> Result<String, String> {
    let mut db = get_database();
    let table = db
        .tables
        .get_mut(&stmt.table)
        .ok_or_else(|| format!("Table '{}' does not exist", stmt.table))?;
    let mut updated_count = 0;
    for row in &mut table.rows {
        let should_update = if let Some(ref where_expr) = stmt.where_clause {
            evaluate_expression(None, where_expr, &table.columns, row)?
        } else {
            true
        };
        if should_update {
            for assignment in &stmt.assignments {
                if let Some(idx) = table
                    .columns
                    .iter()
                    .position(|c| c.name == assignment.column)
                {
                    row[idx] = assignment.value.clone();
                } else {
                    return Err(format!("Column '{}' not found", assignment.column));
                }
            }
            updated_count += 1;
        }
    }
    db.save()?;
    Ok(format!("{} row(s) updated", updated_count))
}

fn execute_delete(stmt: DeleteStatement) -> Result<String, String> {
    let mut db = get_database();
    let table = db
        .tables
        .get_mut(&stmt.table)
        .ok_or_else(|| format!("Table '{}' does not exist", stmt.table))?;
    let initial_count = table.rows.len();
    if let Some(ref where_expr) = stmt.where_clause {
        table
            .rows
            .retain(|row| !evaluate_expression(None, where_expr, &table.columns, row).unwrap_or(false));
    } else {
        table.rows.clear();
    }
    let deleted_count = initial_count - table.rows.len();
    db.save()?;
    Ok(format!("{} row(s) deleted", deleted_count))
}

fn evaluate_expression(
    db: Option<&Database>,
    expr: &Expression,
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<bool, String> {
    match expr {
        Expression::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(evaluate_expression(db, left, columns, row)?
                && evaluate_expression(db, right, columns, row)?),
            BinaryOperator::Or => Ok(evaluate_expression(db, left, columns, row)?
                || evaluate_expression(db, right, columns, row)?),
            BinaryOperator::Like => {
                let left_val = evaluate_value_expression(left, columns, row)?;
                let right_val = evaluate_value_expression(right, columns, row)?;
                match (left_val, right_val) {
                    (Value::Text(text), Value::Text(pattern)) => Ok(match_like(&text, &pattern)),
                    _ => Err("LIKE operator requires text values".to_string()),
                }
            }
            BinaryOperator::Between => {
                let left_val = evaluate_value_expression(left, columns, row)?;
                match &**right {
                    Expression::BinaryOp { left: lb, op: lb_op, right: rb } if *lb_op == BinaryOperator::And => {
                        let lower = evaluate_value_expression(lb, columns, row)?;
                        let upper = evaluate_value_expression(rb, columns, row)?;
                        Ok(is_between(&left_val, &lower, &upper))
                    }
                    _ => Err("BETWEEN requires two values".to_string()),
                }
            }
            BinaryOperator::In => {
                let left_val = evaluate_value_expression(left, columns, row)?;
                match &**right {
                    Expression::Subquery(subquery_stmt) => {
                        let db_ref = db.ok_or_else(|| "Subquery not allowed in this context".to_string())?;
                        let sub_vals = eval_subquery_values(db_ref, subquery_stmt)?;
                        Ok(sub_vals.contains(&left_val))
                    }
                    _ => {
                        let right_val = evaluate_value_expression(right, columns, row)?;
                        compare_values(&left_val, op, &right_val)
                    }
                }
            }
            _ => {
                let left_val = evaluate_value_expression(left, columns, row)?;
                let right_val = evaluate_value_expression(right, columns, row)?;
                compare_values(&left_val, op, &right_val)
            }
        },
        Expression::In { left, values } => {
            let left_val = evaluate_value_expression(left, columns, row)?;
            Ok(values.contains(&left_val))
        }
        Expression::Exists(subquery_stmt) => {
            let db_ref = db.ok_or_else(|| "EXISTS subquery not allowed in this context".to_string())?;
            eval_subquery_exists_with_outer(db_ref, subquery_stmt, columns, row)
        }
        Expression::IsNull { expr, not } => {
            let value = evaluate_value_expression(expr, columns, row)?;
            let is_null = matches!(value, Value::Null);
            Ok(if *not { !is_null } else { is_null })
        }
        Expression::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => Ok(!evaluate_expression(db, expr, columns, row)?),
            _ => Err("Unsupported unary operation in WHERE clause".to_string()),
        },
        _ => Err("Invalid expression in WHERE clause".to_string()),
    }
}

fn match_like(text: &str, pattern: &str) -> bool {
    let text_chars: Vec<char> = text.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();
    
    fn match_pattern(text: &[char], pattern: &[char], text_idx: usize, pattern_idx: usize) -> bool {
        let text_len = text.len();
        let pattern_len = pattern.len();
        
        if pattern_idx == pattern_len {
            return text_idx == text_len;
        }
        
        if pattern[pattern_idx] == '%' {
            if pattern_idx + 1 == pattern_len {
                return true;
            }
            for i in text_idx..=text_len {
                if match_pattern(text, pattern, i, pattern_idx + 1) {
                    return true;
                }
            }
            return false;
        }
        
        if pattern[pattern_idx] == '_' {
            if text_idx < text_len {
                return match_pattern(text, pattern, text_idx + 1, pattern_idx + 1);
            }
            return false;
        }
        
        if text_idx < text_len && text[text_idx] == pattern[pattern_idx] {
            return match_pattern(text, pattern, text_idx + 1, pattern_idx + 1);
        }
        
        false
    }
    
    match_pattern(&text_chars, &pattern_chars, 0, 0)
}

fn is_between(val: &Value, lower: &Value, upper: &Value) -> bool {
    match (val, lower, upper) {
        (Value::Integer(v), Value::Integer(l), Value::Integer(u)) => *v >= *l && *v <= *u,
        (Value::Float(v), Value::Float(l), Value::Float(u)) => *v >= *l && *v <= *u,
        (Value::Integer(v), Value::Integer(l), Value::Float(u)) => *v as f64 >= *l as f64 && *v as f64 <= *u as f64,
        (Value::Integer(v), Value::Float(l), Value::Integer(u)) => *v as f64 >= *l && *v as f64 <= *u as f64,
        (Value::Float(v), Value::Integer(l), Value::Integer(u)) => *v >= *l as f64 && *v <= *u as f64,
        (Value::Float(v), Value::Integer(l), Value::Float(u)) => *v >= *l as f64 && *v <= *u,
        (Value::Float(v), Value::Float(l), Value::Integer(u)) => *v >= *l && *v <= *u as f64,
        (Value::Integer(v), Value::Float(l), Value::Float(u)) => *v as f64 >= *l && *v as f64 <= *u,
        (Value::Text(v), Value::Text(l), Value::Text(u)) => v >= l && v <= u,
        _ => false,
    }
}

fn evaluate_value_expression(
    expr: &Expression,
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<Value, String> {
    match expr {
        Expression::Column(name) => {
            if name == "*" {
                return Ok(Value::Integer(1));
            }
            let col_name = if name.contains('.') {
                name.split('.').next_back().unwrap_or(name)
            } else {
                name
            };
            let idx = columns
                .iter()
                .position(|c| c.name == col_name)
                .ok_or_else(|| format!("Column '{}' not found", name))?;
            Ok(row[idx].clone())
        }
        Expression::Value(val) => Ok(val.clone()),
        _ => Err("Complex expressions not yet supported".to_string()),
    }
}

fn compare_values(left: &Value, op: &BinaryOperator, right: &Value) -> Result<bool, String> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(false);
    }
    
    let (left_num, right_num) = match (left, right) {
        (Value::Integer(l), Value::Integer(r)) => (Some(*l as f64), Some(*r as f64)),
        (Value::Float(l), Value::Float(r)) => (Some(*l), Some(*r)),
        (Value::Integer(l), Value::Float(r)) => (Some(*l as f64), Some(*r)),
        (Value::Float(l), Value::Integer(r)) => (Some(*l), Some(*r as f64)),
        _ => (None, None),
    };
    if let (Some(l), Some(r)) = (left_num, right_num) {
        Ok(match op {
            BinaryOperator::Equal => (l - r).abs() < f64::EPSILON,
            BinaryOperator::NotEqual => (l - r).abs() >= f64::EPSILON,
            BinaryOperator::LessThan => l < r,
            BinaryOperator::LessThanOrEqual => l <= r,
            BinaryOperator::GreaterThan => l > r,
            BinaryOperator::GreaterThanOrEqual => l >= r,
            _ => return Err("Invalid operator for numeric comparison".to_string()),
        })
    } else {
        match (left, right, op) {
            (Value::Text(l), Value::Text(r), op) => Ok(match op {
                BinaryOperator::Equal => l == r,
                BinaryOperator::NotEqual => l != r,
                _ => return Err("Invalid operator for strings".to_string()),
            }),
            (Value::Date(l), Value::Date(r), op) => Ok(match op {
                BinaryOperator::Equal => l == r,
                BinaryOperator::NotEqual => l != r,
                BinaryOperator::LessThan => l < r,
                BinaryOperator::LessThanOrEqual => l <= r,
                BinaryOperator::GreaterThan => l > r,
                BinaryOperator::GreaterThanOrEqual => l >= r,
                _ => return Err("Invalid operator for dates".to_string()),
            }),
            (Value::Time(l), Value::Time(r), op) => Ok(match op {
                BinaryOperator::Equal => l == r,
                BinaryOperator::NotEqual => l != r,
                BinaryOperator::LessThan => l < r,
                BinaryOperator::LessThanOrEqual => l <= r,
                BinaryOperator::GreaterThan => l > r,
                BinaryOperator::GreaterThanOrEqual => l >= r,
                _ => return Err("Invalid operator for times".to_string()),
            }),
            (Value::DateTime(l), Value::DateTime(r), op) => Ok(match op {
                BinaryOperator::Equal => l == r,
                BinaryOperator::NotEqual => l != r,
                BinaryOperator::LessThan => l < r,
                BinaryOperator::LessThanOrEqual => l <= r,
                BinaryOperator::GreaterThan => l > r,
                BinaryOperator::GreaterThanOrEqual => l >= r,
                _ => return Err("Invalid operator for datetimes".to_string()),
            }),
            _ => Err("Type mismatch in comparison".to_string()),
        }
    }
}

pub fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Boolean(b) => b.to_string(),
        Value::Date(d) => d.clone(),
        Value::Time(t) => t.clone(),
        Value::DateTime(dt) => dt.clone(),
    }
}

fn eval_subquery_values(db: &Database, subquery: &SelectStatement) -> Result<Vec<Value>, String> {
    // Evaluate a simple subquery of form SELECT single_column FROM table [WHERE ...]
    // Only supports single-column projection for IN (...)
    if subquery.columns.len() != 1 {
        return Err("Subquery in IN must select exactly one column".to_string());
    }
    let table = db
        .tables
        .get(&subquery.from)
        .ok_or_else(|| format!("Table '{}' does not exist", subquery.from))?;
    let mut values = Vec::new();
    for row in &table.rows {
        let include_row = if let Some(ref where_expr) = subquery.where_clause {
            evaluate_expression(Some(db), where_expr, &table.columns, row)?
        } else {
            true
        };
        if !include_row {
            continue;
        }

        match &subquery.columns[0] {
            Column::All => return Err("Subquery in IN cannot use *".to_string()),
            Column::Named(name) => {
                let idx = table
                    .columns
                    .iter()
                    .position(|c| &c.name == name)
                    .ok_or_else(|| format!("Column '{}' not found", name))?;
                values.push(row[idx].clone());
            }
            Column::Function(_) => {
                return Err("Aggregates in subquery IN not supported yet".to_string());
            }
            Column::Subquery(_) => {
                return Err("Nested subqueries in IN not supported yet".to_string());
            }
        }
    }
    Ok(values)
}

fn eval_subquery_exists_with_outer(
    db: &Database,
    subquery: &SelectStatement,
    outer_columns: &[ColumnDefinition],
    outer_row: &[Value],
) -> Result<bool, String> {
    if !subquery.joins.is_empty() {
        return eval_subquery_exists_with_joins(db, subquery, outer_columns, outer_row);
    }
    
    let table = db
        .tables
        .get(&subquery.from)
        .ok_or_else(|| format!("Table '{}' does not exist", subquery.from))?;

    let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
    combined_columns.extend(table.columns.clone());

    for inner_row in &table.rows {
        let mut combined_row: Vec<Value> = outer_row.to_vec();
        combined_row.extend(inner_row.clone());

        let include_row = if let Some(ref where_expr) = subquery.where_clause {
            evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
        } else {
            true
        };
        if include_row {
            return Ok(true);
        }
    }
    Ok(false)
}

fn eval_subquery_exists_with_joins(
    db: &Database,
    subquery: &SelectStatement,
    outer_columns: &[ColumnDefinition],
    outer_row: &[Value],
) -> Result<bool, String> {
    if subquery.joins.len() != 1 {
        return Err("Multiple joins in EXISTS subquery not yet supported".to_string());
    }
    
    let main_table = db
        .tables
        .get(&subquery.from)
        .ok_or_else(|| format!("Table '{}' does not exist", subquery.from))?;
    
    let join = &subquery.joins[0];
    let join_table = db
        .tables
        .get(&join.table)
        .ok_or_else(|| format!("Table '{}' does not exist", join.table))?;
    
    let mut all_subquery_columns = main_table.columns.clone();
    all_subquery_columns.extend(join_table.columns.clone());
    
    let main_table_name = subquery.from.clone();
    let join_table_name = join.table.clone();
    
    let check_join_match = |main_row: &Vec<Value>, join_row: &Vec<Value>| -> bool {
        if let Expression::BinaryOp { left, op, right } = &join.on
            && *op == BinaryOperator::Equal
                && let (Expression::Column(left_col), Expression::Column(right_col)) = (left.as_ref(), right.as_ref()) {
                    let get_col_idx = |col_name: &str| -> Option<(bool, usize)> {
                        if col_name.contains('.') {
                            let parts: Vec<&str> = col_name.split('.').collect();
                            if parts.len() == 2 {
                                let table_name = parts[0];
                                let col_name = parts[1];
                                if table_name == main_table_name {
                                    main_table.columns.iter().position(|c| c.name == col_name).map(|idx| (true, idx))
                                } else if table_name == join_table_name {
                                    join_table.columns.iter().position(|c| c.name == col_name).map(|idx| (false, idx))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    };
                    
                    let left_col_idx = get_col_idx(left_col);
                    let right_col_idx = get_col_idx(right_col);
                    
                    let left_val = if let Some((in_main, idx)) = left_col_idx {
                        if in_main {
                            Some(&main_row[idx])
                        } else {
                            Some(&join_row[idx])
                        }
                    } else {
                        None
                    };
                    
                    let right_val = if let Some((in_main, idx)) = right_col_idx {
                        if in_main {
                            Some(&main_row[idx])
                        } else {
                            Some(&join_row[idx])
                        }
                    } else {
                        None
                    };
                    
                    if let (Some(lval), Some(rval)) = (left_val, right_val) {
                        return lval == rval;
                    }
                }
        false
    };
    
    match join.join_type {
        JoinType::Inner => {
            for main_row in &main_table.rows {
                for join_row in &join_table.rows {
                    if check_join_match(main_row, join_row) {
                        let mut combined_subquery_row = main_row.clone();
                        combined_subquery_row.extend(join_row.clone());
                        
                        let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
                        combined_columns.extend(all_subquery_columns.clone());
                        let mut combined_row: Vec<Value> = outer_row.to_vec();
                        combined_row.extend(combined_subquery_row);
                        
                        let include_row = if let Some(ref where_expr) = subquery.where_clause {
                            evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
                        } else {
                            return Ok(true);
                        };
                        if include_row {
                            return Ok(true);
                        }
                    }
                }
            }
        }
        JoinType::Left => {
            for main_row in &main_table.rows {
                let mut has_match = false;
                for join_row in &join_table.rows {
                    if check_join_match(main_row, join_row) {
                        has_match = true;
                        let mut combined_subquery_row = main_row.clone();
                        combined_subquery_row.extend(join_row.clone());
                        
                        let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
                        combined_columns.extend(all_subquery_columns.clone());
                        let mut combined_row: Vec<Value> = outer_row.to_vec();
                        combined_row.extend(combined_subquery_row);
                        
                        let include_row = if let Some(ref where_expr) = subquery.where_clause {
                            evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
                        } else {
                            return Ok(true);
                        };
                        if include_row {
                            return Ok(true);
                        }
                    }
                }
                if !has_match {
                    let mut combined_subquery_row = main_row.clone();
                    combined_subquery_row.extend(vec![Value::Null; join_table.columns.len()]);
                    
                    let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
                    combined_columns.extend(all_subquery_columns.clone());
                    let mut combined_row: Vec<Value> = outer_row.to_vec();
                    combined_row.extend(combined_subquery_row);
                    
                    let include_row = if let Some(ref where_expr) = subquery.where_clause {
                        evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
                    } else {
                        return Ok(true);
                    };
                    if include_row {
                        return Ok(true);
                    }
                }
            }
        }
        JoinType::Right => {
            for join_row in &join_table.rows {
                let mut has_match = false;
                for main_row in &main_table.rows {
                    if check_join_match(main_row, join_row) {
                        has_match = true;
                        let mut combined_subquery_row = main_row.clone();
                        combined_subquery_row.extend(join_row.clone());
                        
                        let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
                        combined_columns.extend(all_subquery_columns.clone());
                        let mut combined_row: Vec<Value> = outer_row.to_vec();
                        combined_row.extend(combined_subquery_row);
                        
                        let include_row = if let Some(ref where_expr) = subquery.where_clause {
                            evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
                        } else {
                            return Ok(true);
                        };
                        if include_row {
                            return Ok(true);
                        }
                    }
                }
                if !has_match {
                    let mut combined_subquery_row = vec![Value::Null; main_table.columns.len()];
                    combined_subquery_row.extend(join_row.clone());
                    
                    let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
                    combined_columns.extend(all_subquery_columns.clone());
                    let mut combined_row: Vec<Value> = outer_row.to_vec();
                    combined_row.extend(combined_subquery_row);
                    
                    let include_row = if let Some(ref where_expr) = subquery.where_clause {
                        evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
                    } else {
                        return Ok(true);
                    };
                    if include_row {
                        return Ok(true);
                    }
                }
            }
        }
        JoinType::Full => {
            let mut matched_pairs = HashSet::new();
            
            for (main_idx, main_row) in main_table.rows.iter().enumerate() {
                let mut has_match = false;
                for (join_idx, join_row) in join_table.rows.iter().enumerate() {
                    if check_join_match(main_row, join_row) {
                        has_match = true;
                        matched_pairs.insert((main_idx, join_idx));
                        let mut combined_subquery_row = main_row.clone();
                        combined_subquery_row.extend(join_row.clone());
                        
                        let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
                        combined_columns.extend(all_subquery_columns.clone());
                        let mut combined_row: Vec<Value> = outer_row.to_vec();
                        combined_row.extend(combined_subquery_row);
                        
                        let include_row = if let Some(ref where_expr) = subquery.where_clause {
                            evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
                        } else {
                            return Ok(true);
                        };
                        if include_row {
                            return Ok(true);
                        }
                    }
                }
                if !has_match {
                    let mut combined_subquery_row = main_row.clone();
                    combined_subquery_row.extend(vec![Value::Null; join_table.columns.len()]);
                    
                    let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
                    combined_columns.extend(all_subquery_columns.clone());
                    let mut combined_row: Vec<Value> = outer_row.to_vec();
                    combined_row.extend(combined_subquery_row);
                    
                    let include_row = if let Some(ref where_expr) = subquery.where_clause {
                        evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
                    } else {
                        return Ok(true);
                    };
                    if include_row {
                        return Ok(true);
                    }
                }
            }
            
            for (join_idx, join_row) in join_table.rows.iter().enumerate() {
                let mut has_match = false;
                for (main_idx, main_row) in main_table.rows.iter().enumerate() {
                    if check_join_match(main_row, join_row) && matched_pairs.contains(&(main_idx, join_idx)) {
                        has_match = true;
                        break;
                    }
                }
                if !has_match {
                    let mut combined_subquery_row = vec![Value::Null; main_table.columns.len()];
                    combined_subquery_row.extend(join_row.clone());
                    
                    let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
                    combined_columns.extend(all_subquery_columns.clone());
                    let mut combined_row: Vec<Value> = outer_row.to_vec();
                    combined_row.extend(combined_subquery_row);
                    
                    let include_row = if let Some(ref where_expr) = subquery.where_clause {
                        evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
                    } else {
                        return Ok(true);
                    };
                    if include_row {
                        return Ok(true);
                    }
                }
            }
        }
    }
    
    Ok(false)
}

fn eval_scalar_subquery_with_outer(
    db: &Database,
    subquery: &SelectStatement,
    outer_columns: &[ColumnDefinition],
    outer_row: &[Value],
) -> Result<Value, String> {
    if subquery.columns.len() != 1 {
        return Err("Scalar subquery must select exactly one column".to_string());
    }

    if !subquery.joins.is_empty() {
        return Err("Scalar subqueries with joins not yet supported".to_string());
    }

    let table = db
        .tables
        .get(&subquery.from)
        .ok_or_else(|| format!("Table '{}' does not exist", subquery.from))?;

    let mut combined_columns: Vec<ColumnDefinition> = outer_columns.to_vec();
    combined_columns.extend(table.columns.clone());

    // Helper: apply ORDER BY, OFFSET, LIMIT to a mutable list of candidate combined rows
    let apply_order_and_slice = |rows: &mut Vec<Vec<Value>>| -> Result<(), String> {
        if let Some(order_by) = &subquery.order_by {
            rows.sort_by(|a, b| {
                for ob in order_by {
                    let va = evaluate_value_expression(&ob.expr, &combined_columns, a).unwrap_or(Value::Null);
                    let vb = evaluate_value_expression(&ob.expr, &combined_columns, b).unwrap_or(Value::Null);
                    let ord = compare_values_for_sort(&va, &vb);
                    if ord != std::cmp::Ordering::Equal {
                        return if ob.asc { ord } else { ord.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        let start = subquery.offset.unwrap_or(0);
        let end = if let Some(limit) = subquery.limit { start.saturating_add(limit) } else { rows.len() };
        let end = end.min(rows.len());
        if start >= rows.len() {
            rows.clear();
        } else {
            rows.drain(0..start);
            rows.truncate(end - start);
        }
        Ok(())
    };

    if let Column::Function(agg) = &subquery.columns[0] {
        let mut filtered_rows: Vec<&Vec<Value>> = Vec::new();
        for inner_row in &table.rows {
            let mut combined_row: Vec<Value> = outer_row.to_vec();
            combined_row.extend(inner_row.clone());

            let include_row = if let Some(ref where_expr) = subquery.where_clause {
                evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
            } else {
                true
            };
            if include_row {
                filtered_rows.push(inner_row);
            }
        }
        return compute_aggregate(&agg.function, &agg.expr, table, &filtered_rows);
    }

    if let Column::Subquery(nested_subquery) = &subquery.columns[0] {
        // Build candidate combined rows first
        let mut candidate_rows: Vec<Vec<Value>> = Vec::new();
        for inner_row in &table.rows {
            let mut combined_row: Vec<Value> = outer_row.to_vec();
            combined_row.extend(inner_row.clone());

            let include_row = if let Some(ref where_expr) = subquery.where_clause {
                evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
            } else {
                true
            };
            if include_row {
                candidate_rows.push(combined_row);
            }
        }

        // Apply ORDER BY / OFFSET / LIMIT on combined rows
        apply_order_and_slice(&mut candidate_rows)?;

        let mut results = Vec::new();
        for combined_row in candidate_rows {
            let nested_result = eval_scalar_subquery_with_outer(
                db,
                nested_subquery,
                &combined_columns,
                &combined_row,
            )?;
            results.push(nested_result);
        }

        return match results.len() {
            0 => Ok(Value::Null),
            1 => Ok(results[0].clone()),
            _ => Err("Scalar subquery returned more than one row".to_string()),
        };
    } else {
        // Simple non-aggregate scalar subquery selecting a single column
        // Build candidate combined rows to support ORDER BY / LIMIT
        let mut candidate_rows: Vec<Vec<Value>> = Vec::new();
        for inner_row in &table.rows {
            let mut combined_row: Vec<Value> = outer_row.to_vec();
            combined_row.extend(inner_row.clone());

            let include_row = if let Some(ref where_expr) = subquery.where_clause {
                evaluate_expression(Some(db), where_expr, &combined_columns, &combined_row)?
            } else {
                true
            };
            if include_row {
                candidate_rows.push(combined_row);
            }
        }

        // Apply ORDER BY / OFFSET / LIMIT on combined rows
        apply_order_and_slice(&mut candidate_rows)?;

        let mut results = Vec::new();
        for combined_row in &candidate_rows {
            let val = match &subquery.columns[0] {
                Column::All => return Err("Scalar subquery cannot use *".to_string()),
                Column::Named(name) => {
                    let col_idx = combined_columns
                        .iter()
                        .position(|c| c.name == (if name.contains('.') { name.split('.').next_back().unwrap_or(name) } else { name }))
                        .ok_or_else(|| format!("Column '{}' not found", name))?;
                    combined_row[col_idx].clone()
                }
                Column::Function(_) => {
                    unreachable!()
                }
                Column::Subquery(_) => {
                    unreachable!()
                }
            };
            results.push(val);
        }

        return match results.len() {
            0 => Ok(Value::Null),
            1 => Ok(results[0].clone()),
            _ => Err("Scalar subquery returned more than one row".to_string()),
        };
    }
}

fn compare_values_for_sort(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Integer(l), Value::Integer(r)) => l.cmp(r),
        (Value::Float(l), Value::Float(r)) => {
            if l < r {
                Ordering::Less
            } else if l > r {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        (Value::Integer(l), Value::Float(r)) => {
            let l = *l as f64;
            if l < *r {
                Ordering::Less
            } else if l > *r {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        (Value::Float(l), Value::Integer(r)) => {
            let r = *r as f64;
            if l < &r {
                Ordering::Less
            } else if l > &r {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        (Value::Text(l), Value::Text(r)) => l.cmp(r),
        (Value::Boolean(l), Value::Boolean(r)) => l.cmp(r),
        (Value::Date(l), Value::Date(r)) => l.cmp(r),
        (Value::Time(l), Value::Time(r)) => l.cmp(r),
        (Value::DateTime(l), Value::DateTime(r)) => l.cmp(r),
        _ => Ordering::Equal,
    }
}

fn execute_select_with_joins(stmt: SelectStatement, db: &Database) -> Result<String, String> {
    let main_table = db
        .tables
        .get(&stmt.from)
        .ok_or_else(|| format!("Table '{}' does not exist", stmt.from))?;
    
    if stmt.joins.len() != 1 {
        return Err("Multiple joins not yet supported".to_string());
    }
    
    let join = &stmt.joins[0];
    let join_table = db
        .tables
        .get(&join.table)
        .ok_or_else(|| format!("Table '{}' does not exist", join.table))?;
    
    let mut all_columns = main_table.columns.clone();
    all_columns.extend(join_table.columns.clone());
    
    let mut joined_rows = Vec::new();
    let mut matched_rows = HashSet::new();
    let main_table_name = stmt.from.clone();
    let join_table_name = join.table.clone();
    
    let check_join_match = |main_row: &Vec<Value>, join_row: &Vec<Value>| -> bool {
        if let Expression::BinaryOp { left, op, right } = &join.on
            && *op == BinaryOperator::Equal
                && let (Expression::Column(left_col), Expression::Column(right_col)) = (left.as_ref(), right.as_ref()) {
                    let get_col_idx = |col_name: &str| -> Option<(bool, usize)> {
                        if col_name.contains('.') {
                            let parts: Vec<&str> = col_name.split('.').collect();
                            if parts.len() == 2 {
                                let table_name = parts[0];
                                let col_name = parts[1];
                                if table_name == main_table_name {
                                    main_table.columns.iter().position(|c| c.name == col_name).map(|idx| (true, idx))
                                } else if table_name == join_table_name {
                                    join_table.columns.iter().position(|c| c.name == col_name).map(|idx| (false, idx))
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    };
                    
                    let left_col_idx = get_col_idx(left_col);
                    let right_col_idx = get_col_idx(right_col);
                    
                    let left_val = if let Some((in_main, idx)) = left_col_idx {
                        if in_main {
                            Some(&main_row[idx])
                        } else {
                            Some(&join_row[idx])
                        }
                    } else {
                        None
                    };
                    
                    let right_val = if let Some((in_main, idx)) = right_col_idx {
                        if in_main {
                            Some(&main_row[idx])
                        } else {
                            Some(&join_row[idx])
                        }
                    } else {
                        None
                    };
                    
                    if let (Some(lval), Some(rval)) = (left_val, right_val) {
                        return lval == rval;
                    }
                }
        false
    };
    
    if matches!(join.join_type, JoinType::Inner | JoinType::Left | JoinType::Full) {
        for (main_idx, main_row) in main_table.rows.iter().enumerate() {
            let mut has_match = false;
            
            for (join_idx, join_row) in join_table.rows.iter().enumerate() {
                if check_join_match(main_row, join_row) {
                    let mut combined_row = main_row.clone();
                    combined_row.extend(join_row.clone());
                    joined_rows.push(combined_row);
                    has_match = true;
                    
                    matched_rows.insert((main_idx, join_idx));
                }
            }
            
            if matches!(join.join_type, JoinType::Left | JoinType::Full) && !has_match {
                let mut combined_row = main_row.clone();
                combined_row.extend(vec![Value::Null; join_table.columns.len()]);
                joined_rows.push(combined_row);
            }
        }
    }
    
    if matches!(join.join_type, JoinType::Right | JoinType::Full) {
        for (join_idx, join_row) in join_table.rows.iter().enumerate() {
            let mut has_match = false;
            
            for (main_idx, main_row) in main_table.rows.iter().enumerate() {
                if check_join_match(main_row, join_row) {
                    has_match = true;
                    
                    if !matches!(join.join_type, JoinType::Full) || !matched_rows.contains(&(main_idx, join_idx)) {
                        let mut combined_row = main_row.clone();
                        combined_row.extend(join_row.clone());
                        joined_rows.push(combined_row);
                    }
                }
            }
            
            if !has_match {
                let mut combined_row = vec![Value::Null; main_table.columns.len()];
                combined_row.extend(join_row.clone());
                joined_rows.push(combined_row);
            }
        }
    }
    
    let mut filtered_rows: Vec<Vec<Value>> = Vec::new();
    let db_ref: &Database = db;
    for row in &joined_rows {
        let include_row = if let Some(ref where_expr) = stmt.where_clause {
            evaluate_expression_multi_table(db_ref, where_expr, &all_columns, row, &main_table.columns, &join_table.columns, main_table, join_table)?
        } else {
            true
        };
        if include_row {
            filtered_rows.push(row.clone());
        }
    }
    
    let column_indices: Vec<usize> = match &stmt.columns[0] {
        Column::All => (0..all_columns.len()).collect(),
        Column::Named(_) => stmt
            .columns
            .iter()
            .map(|col| match col {
                Column::Named(name) => {
                    let col_name = if name.contains('.') {
                        name.split('.').next_back().unwrap_or(name)
                    } else {
                        name
                    };
                    all_columns
                        .iter()
                        .position(|c| c.name == col_name)
                        .unwrap_or(usize::MAX)
                }
                _ => usize::MAX,
            })
            .collect(),
        _ => return Err("Invalid column type".to_string()),
    };
    
    for idx in &column_indices {
        if *idx == usize::MAX {
            return Err("Column not found".to_string());
        }
    }
    
    let mut result = String::new();
    for idx in &column_indices {
        result.push_str(&format!("{}\t", all_columns[*idx].name));
    }
    result.push('\n');
    result.push_str(&"-".repeat(40));
    result.push('\n');
    
    use std::collections::BTreeSet;
    let mut seen: BTreeSet<Vec<Value>> = BTreeSet::new();
    for row in &filtered_rows {
        let projected: Vec<Value> = column_indices.iter().map(|&i| row[i].clone()).collect();
        if stmt.distinct
            && !seen.insert(projected.clone()) {
                continue;
            }
        for val in &projected {
            result.push_str(&format!("{}\t", format_value(val)));
        }
        result.push('\n');
    }
    
    Ok(result)
}

fn evaluate_expression_multi_table(
    db: &Database,
    expr: &Expression,
    all_columns: &[ColumnDefinition],
    row: &[Value],
    _main_cols: &[ColumnDefinition],
    _join_cols: &[ColumnDefinition],
    _main_table: &Table,
    _join_table: &Table,
) -> Result<bool, String> {
    evaluate_expression(Some(db), expr, all_columns, row)
}

fn execute_alter_table(stmt: AlterTableStatement) -> Result<String, String> {
    let mut db = get_database();
    let table = db
        .tables
        .get_mut(&stmt.table)
        .ok_or_else(|| format!("Table '{}' does not exist", stmt.table))?;

    match stmt.operation {
        AlterOperation::AddColumn(col_def) => {
            if table.columns.iter().any(|c| c.name == col_def.name) {
                return Err(format!("Column '{}' already exists", col_def.name));
            }
            table.columns.push(col_def.clone());
            let default_value = match col_def.data_type {
                DataType::Integer => Value::Integer(0),
                DataType::Float => Value::Float(0.0),
                DataType::Text => Value::Text(String::new()),
                DataType::Boolean => Value::Boolean(false),
                DataType::Date => Value::Date("1970-01-01".to_string()),
                DataType::Time => Value::Time("00:00:00".to_string()),
                DataType::DateTime => Value::DateTime("1970-01-01 00:00:00".to_string()),
            };
            for row in &mut table.rows {
                row.push(default_value.clone());
            }
            db.save()?;
            Ok(format!(
                "Column '{}' added to table '{}'",
                col_def.name, stmt.table
            ))
        }
        AlterOperation::DropColumn(col_name) => {
            let col_index = table
                .columns
                .iter()
                .position(|c| c.name == col_name)
                .ok_or_else(|| format!("Column '{}' does not exist", col_name))?;
            table.columns.remove(col_index);
            for row in &mut table.rows {
                if col_index < row.len() {
                    row.remove(col_index);
                }
            }
            db.save()?;
            Ok(format!(
                "Column '{}' dropped from table '{}'",
                col_name, stmt.table
            ))
        }
        AlterOperation::RenameColumn { old, new } => {
            let col_exists = table.columns.iter().any(|c| c.name == old);
            if !col_exists {
                return Err(format!("Column '{}' does not exist", old));
            }
            if table.columns.iter().any(|c| c.name == new && c.name != old) {
                return Err(format!("Column '{}' already exists", new));
            }
            for column in &mut table.columns {
                if column.name == old {
                    column.name = new.clone();
                    break;
                }
            }
            db.save()?;
            Ok(format!(
                "Column '{}' renamed to '{}' in table '{}'",
                old, new, stmt.table
            ))
        }
    }
}
