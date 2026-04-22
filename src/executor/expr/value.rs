use super::*;

pub fn evaluate_value_expression(
    expr: &Expression,
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<Value, RustqlError> {
    evaluate_value_expression_with_db(expr, columns, row, None::<&dyn DatabaseCatalog>)
}

pub fn evaluate_value_expression_with_db(
    expr: &Expression,
    columns: &[ColumnDefinition],
    row: &[Value],
    db: Option<&dyn DatabaseCatalog>,
) -> Result<Value, RustqlError> {
    match expr {
        Expression::Column(name) => {
            if name == "*" {
                return Ok(Value::Integer(1));
            }
            if let Some(idx) = columns.iter().position(|c| c.name == *name) {
                return Ok(row[idx].clone());
            }
            if name.contains('.') {
                let col_name = name.split('.').next_back().unwrap_or(name);
                if let Some(idx) = columns.iter().position(|c| {
                    c.name == col_name
                        || c.name.split('.').next_back().unwrap_or(&c.name) == col_name
                }) {
                    return Ok(row[idx].clone());
                }
            } else if let Some(idx) = columns
                .iter()
                .position(|c| c.name.split('.').next_back().unwrap_or(&c.name) == name.as_str())
            {
                return Ok(row[idx].clone());
            }
            Err(RustqlError::ColumnNotFound(name.clone()))
        }
        Expression::Value(val) => Ok(val.clone()),
        Expression::BinaryOp { left, op, right } => {
            let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
            let right_val = evaluate_value_expression_with_db(right, columns, row, db)?;
            match op {
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide => apply_arithmetic(&left_val, &right_val, op),
                BinaryOperator::Concat => {
                    let l = format_value(&left_val);
                    let r = format_value(&right_val);
                    Ok(Value::Text(format!("{}{}", l, r)))
                }
                _ => Err(RustqlError::Internal(
                    "Only arithmetic operators are supported in SELECT expressions".to_string(),
                )),
            }
        }
        Expression::UnaryOp { op, expr } => {
            let val = evaluate_value_expression_with_db(expr, columns, row, db)?;
            match op {
                UnaryOperator::Minus => match val {
                    Value::Integer(n) => Ok(Value::Integer(-n)),
                    Value::Float(f) => Ok(Value::Float(-f)),
                    _ => Err(RustqlError::Internal(
                        "Unary minus only supported for numeric types".to_string(),
                    )),
                },
                _ => Err(RustqlError::Internal(
                    "Unsupported unary operator in SELECT expression".to_string(),
                )),
            }
        }
        Expression::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            if let Some(operand_expr) = operand {
                let operand_val =
                    evaluate_value_expression_with_db(operand_expr, columns, row, db)?;
                for (when_expr, then_expr) in when_clauses {
                    let when_val = evaluate_value_expression_with_db(when_expr, columns, row, db)?;
                    if operand_val == when_val {
                        return evaluate_value_expression_with_db(then_expr, columns, row, db);
                    }
                }
            } else {
                for (when_expr, then_expr) in when_clauses {
                    let is_true = evaluate_expression(db, when_expr, columns, row)?;
                    if is_true {
                        return evaluate_value_expression_with_db(then_expr, columns, row, db);
                    }
                }
            }
            if let Some(else_expr) = else_clause {
                evaluate_value_expression_with_db(else_expr, columns, row, db)
            } else {
                Ok(Value::Null)
            }
        }
        Expression::ScalarFunction { name, args } => {
            super::functions::evaluate_scalar_function(name, args, columns, row, db)
        }
        Expression::Cast { expr, data_type } => {
            let val = evaluate_value_expression_with_db(expr, columns, row, db)?;
            super::cast::execute_cast(val, data_type)
        }
        Expression::WindowFunction { .. } => Err(RustqlError::Internal(
            "Window functions must be evaluated in a separate pass".to_string(),
        )),
        Expression::Subquery(subquery) => {
            let db_ref = db.ok_or_else(|| {
                RustqlError::Internal(
                    "Scalar subquery evaluation requires an explicit database context".to_string(),
                )
            })?;
            crate::plan_executor::evaluate_planned_scalar_subquery_with_outer(
                db_ref, subquery, columns, row,
            )
        }
        _ => Err(RustqlError::Internal(
            "Complex expressions not yet supported in SELECT".to_string(),
        )),
    }
}
