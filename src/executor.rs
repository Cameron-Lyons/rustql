use crate::ast::*;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::{Mutex, OnceLock};

const DATABASE_FILE: &str = "rustql_data.json";

#[derive(Serialize, Deserialize)]
pub struct Database {
    tables: HashMap<String, Table>,
}

#[derive(Serialize, Deserialize)]
struct Table {
    columns: Vec<ColumnDefinition>,
    rows: Vec<Vec<Value>>,
}

impl Database {
    fn new() -> Self {
        Database {
            tables: HashMap::new(),
        }
    }

    fn load() -> Self {
        if Path::new(DATABASE_FILE).exists() {
            let data = fs::read_to_string(DATABASE_FILE).unwrap_or_default();
            serde_json::from_str(&data).unwrap_or_else(|_| Database::new())
        } else {
            Database::new()
        }
    }

    fn save(&self) -> Result<(), String> {
        let data = serde_json::to_string_pretty(self)
            .map_err(|e| format!("Failed to serialize database: {}", e))?;
        fs::write(DATABASE_FILE, data)
            .map_err(|e| format!("Failed to write database file: {}", e))?;
        Ok(())
    }
}

static DATABASE: OnceLock<Mutex<Database>> = OnceLock::new();

fn get_database() -> std::sync::MutexGuard<'static, Database> {
    DATABASE
        .get_or_init(|| Mutex::new(Database::load()))
        .lock()
        .unwrap()
}

pub fn execute(statement: Statement) -> Result<String, String> {
    match statement {
        Statement::CreateTable(stmt) => execute_create_table(stmt),
        Statement::DropTable(stmt) => execute_drop_table(stmt),
        Statement::Insert(stmt) => execute_insert(stmt),
        Statement::Select(stmt) => execute_select(stmt),
        Statement::Update(stmt) => execute_update(stmt),
        Statement::Delete(stmt) => execute_delete(stmt),
    }
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

    let mut result = String::new();

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
    };

    for idx in &column_indices {
        if *idx == usize::MAX {
            return Err("Column not found".to_string());
        }
    }

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
    // Convert values to compatible types for comparison
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
