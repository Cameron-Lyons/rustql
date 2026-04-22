use super::*;

pub fn evaluate_expression(
    db: Option<&dyn DatabaseCatalog>,
    expr: &Expression,
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<bool, RustqlError> {
    match expr {
        Expression::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => Ok(evaluate_expression(db, left, columns, row)?
                && evaluate_expression(db, right, columns, row)?),
            BinaryOperator::Or => Ok(evaluate_expression(db, left, columns, row)?
                || evaluate_expression(db, right, columns, row)?),
            BinaryOperator::Like => {
                let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
                let right_val = evaluate_value_expression_with_db(right, columns, row, db)?;
                match (left_val, right_val) {
                    (Value::Text(text), Value::Text(pattern)) => Ok(match_like(&text, &pattern)),
                    _ => Err(RustqlError::TypeMismatch(
                        "LIKE operator requires text values".to_string(),
                    )),
                }
            }
            BinaryOperator::ILike => {
                let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
                let right_val = evaluate_value_expression_with_db(right, columns, row, db)?;
                match (left_val, right_val) {
                    (Value::Text(text), Value::Text(pattern)) => {
                        Ok(match_like(&text.to_lowercase(), &pattern.to_lowercase()))
                    }
                    _ => Err(RustqlError::TypeMismatch(
                        "ILIKE operator requires text values".to_string(),
                    )),
                }
            }
            BinaryOperator::Between => {
                let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
                match &**right {
                    Expression::BinaryOp {
                        left: lb,
                        op: lb_op,
                        right: rb,
                    } if *lb_op == BinaryOperator::And => {
                        let lower = evaluate_value_expression_with_db(lb, columns, row, db)?;
                        let upper = evaluate_value_expression_with_db(rb, columns, row, db)?;
                        Ok(is_between(&left_val, &lower, &upper))
                    }
                    _ => Err(RustqlError::TypeMismatch(
                        "BETWEEN requires two values".to_string(),
                    )),
                }
            }
            BinaryOperator::In => {
                let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
                match &**right {
                    Expression::Subquery(subquery_stmt) => {
                        let db_ref =
                            db.ok_or_else(|| "Subquery not allowed in this context".to_string())?;
                        let sub_vals =
                            crate::plan_executor::evaluate_planned_subquery_values_with_outer(
                                db_ref,
                                subquery_stmt,
                                columns,
                                row,
                            )?;
                        Ok(sub_vals.contains(&left_val))
                    }
                    _ => {
                        let right_val = evaluate_value_expression_with_db(right, columns, row, db)?;
                        compare_values(&left_val, op, &right_val)
                    }
                }
            }
            _ => {
                let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
                let right_val = evaluate_value_expression_with_db(right, columns, row, db)?;
                compare_values(&left_val, op, &right_val)
            }
        },
        Expression::In { left, values } => {
            let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
            Ok(values.contains(&left_val))
        }
        Expression::Exists(subquery_stmt) => {
            let db_ref =
                db.ok_or_else(|| "EXISTS subquery not allowed in this context".to_string())?;
            crate::plan_executor::evaluate_planned_subquery_exists_with_outer(
                db_ref,
                subquery_stmt,
                columns,
                row,
            )
        }
        Expression::Any { left, op, subquery } => {
            let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
            let db_ref =
                db.ok_or_else(|| "ANY subquery not allowed in this context".to_string())?;
            let sub_vals = crate::plan_executor::evaluate_planned_subquery_values_with_outer(
                db_ref, subquery, columns, row,
            )?;
            for value in &sub_vals {
                if compare_values(&left_val, op, value)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        Expression::All { left, op, subquery } => {
            let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
            let db_ref =
                db.ok_or_else(|| "ALL subquery not allowed in this context".to_string())?;
            let sub_vals = crate::plan_executor::evaluate_planned_subquery_values_with_outer(
                db_ref, subquery, columns, row,
            )?;
            for value in &sub_vals {
                if !compare_values(&left_val, op, value)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        Expression::IsNull { expr, not } => {
            let value = evaluate_value_expression_with_db(expr, columns, row, db)?;
            let is_null = matches!(value, Value::Null);
            Ok(if *not { !is_null } else { is_null })
        }
        Expression::IsDistinctFrom { left, right, not } => {
            let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
            let right_val = evaluate_value_expression_with_db(right, columns, row, db)?;
            let is_distinct = match (&left_val, &right_val) {
                (Value::Null, Value::Null) => false,
                (Value::Null, _) | (_, Value::Null) => true,
                _ => left_val != right_val,
            };
            Ok(if *not { !is_distinct } else { is_distinct })
        }
        Expression::Value(Value::Boolean(value)) => Ok(*value),
        Expression::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => Ok(!evaluate_expression(db, expr, columns, row)?),
            _ => Err(RustqlError::Internal(
                "Unsupported unary operation in WHERE clause".to_string(),
            )),
        },
        _ => Err(RustqlError::Internal(
            "Invalid expression in WHERE clause".to_string(),
        )),
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
        (Value::Integer(v), Value::Integer(l), Value::Float(u)) => {
            *v as f64 >= *l as f64 && *v as f64 <= *u
        }
        (Value::Integer(v), Value::Float(l), Value::Integer(u)) => {
            *v as f64 >= *l && *v as f64 <= *u as f64
        }
        (Value::Float(v), Value::Integer(l), Value::Integer(u)) => {
            *v >= *l as f64 && *v <= *u as f64
        }
        (Value::Float(v), Value::Integer(l), Value::Float(u)) => *v >= *l as f64 && *v <= *u,
        (Value::Float(v), Value::Float(l), Value::Integer(u)) => *v >= *l && *v <= *u as f64,
        (Value::Integer(v), Value::Float(l), Value::Float(u)) => *v as f64 >= *l && *v as f64 <= *u,
        (Value::Text(v), Value::Text(l), Value::Text(u)) => v >= l && v <= u,
        _ => false,
    }
}
