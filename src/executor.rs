use crate::ast::*;
use crate::database::{Database, Table};
use std::cmp::Ordering;
use std::collections::BTreeMap;
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
    let table = db
        .tables
        .get(&stmt.from)
        .ok_or_else(|| format!("Table '{}' does not exist", stmt.from))?;

    let mut filtered_rows: Vec<&Vec<Value>> = Vec::new();
    for row in &table.rows {
        let include_row = if let Some(ref where_expr) = stmt.where_clause {
            evaluate_expression(where_expr, &table.columns, row)?
        } else {
            true
        };
        if include_row {
            filtered_rows.push(row);
        }
    }

    if let Some(_) = stmt.group_by {
        return execute_select_with_grouping(stmt, table, filtered_rows);
    }

    let has_aggregate = stmt
        .columns
        .iter()
        .any(|col| matches!(col, Column::Function(_)));

    if has_aggregate {
        return execute_select_with_aggregates(stmt, table, filtered_rows);
    }

    let column_indices: Vec<usize> = match &stmt.columns[0] {
        Column::All => (0..table.columns.len()).collect(),
        Column::Named(_) => stmt
            .columns
            .iter()
            .map(|col| match col {
                Column::Named(name) => table
                    .columns
                    .iter()
                    .position(|c| &c.name == name)
                    .unwrap_or(usize::MAX),
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
    for idx in &column_indices {
        result.push_str(&format!("{}\t", table.columns[*idx].name));
    }
    result.push('\n');
    result.push_str(&"-".repeat(40));
    result.push('\n');

    for row in filtered_rows.iter().skip(offset).take(limit) {
        for idx in &column_indices {
            result.push_str(&format!("{}\t", format_value(&row[*idx])));
        }
        result.push('\n');
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
        groups.entry(key).or_insert_with(Vec::new).push(row);
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
            let should_include = evaluate_having(&having_expr, &stmt.columns, table, &group_rows)?;
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
            evaluate_expression(where_expr, &table.columns, row)?
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
            .retain(|row| !evaluate_expression(where_expr, &table.columns, row).unwrap_or(false));
    } else {
        table.rows.clear();
    }
    let deleted_count = initial_count - table.rows.len();
    db.save()?;
    Ok(format!("{} row(s) deleted", deleted_count))
}

fn evaluate_expression(
    expr: &Expression,
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<bool, String> {
    match expr {
        Expression::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(evaluate_expression(left, columns, row)?
                && evaluate_expression(right, columns, row)?),
            BinaryOperator::Or => Ok(evaluate_expression(left, columns, row)?
                || evaluate_expression(right, columns, row)?),
            _ => {
                let left_val = evaluate_value_expression(left, columns, row)?;
                let right_val = evaluate_value_expression(right, columns, row)?;
                compare_values(&left_val, op, &right_val)
            }
        },
        Expression::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => Ok(!evaluate_expression(expr, columns, row)?),
            _ => Err("Unsupported unary operation in WHERE clause".to_string()),
        },
        _ => Err("Invalid expression in WHERE clause".to_string()),
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
            let idx = columns
                .iter()
                .position(|c| &c.name == name)
                .ok_or_else(|| format!("Column '{}' not found", name))?;
            Ok(row[idx].clone())
        }
        Expression::Value(val) => Ok(val.clone()),
        _ => Err("Complex expressions not yet supported".to_string()),
    }
}

fn compare_values(left: &Value, op: &BinaryOperator, right: &Value) -> Result<bool, String> {
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
            _ => Err("Type mismatch in comparison".to_string()),
        }
    }
}

fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Integer(n) => n.to_string(),
        Value::Float(f) => f.to_string(),
        Value::Text(s) => s.clone(),
        Value::Boolean(b) => b.to_string(),
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
        _ => Ordering::Equal,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ast::{ColumnDefinition, CreateTableStatement, DataType, Statement};

    #[test]
    fn test_format_value() {
        assert_eq!(format_value(&Value::Integer(42)), "42");
        assert_eq!(format_value(&Value::Null), "NULL");
    }

    #[test]
    fn test_compare_values_sort() {
        use std::cmp::Ordering::*;
        assert_eq!(
            compare_values_for_sort(&Value::Integer(1), &Value::Integer(2)),
            Less
        );
    }

    #[test]
    fn test_create_table() {
        reset_database_state();
        let stmt = Statement::CreateTable(CreateTableStatement {
            name: "users".into(),
            columns: vec![
                ColumnDefinition {
                    name: "id".into(),
                    data_type: DataType::Integer,
                    nullable: false,
                },
                ColumnDefinition {
                    name: "name".into(),
                    data_type: DataType::Text,
                    nullable: false,
                },
            ],
        });
        assert!(execute(stmt).is_ok());
    }
}
