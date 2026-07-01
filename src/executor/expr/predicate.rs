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
                        let db_ref = db.ok_or_else(|| {
                            RustqlError::Internal(
                                "Subquery not allowed in this context".to_string(),
                            )
                        })?;
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
            let db_ref = db.ok_or_else(|| {
                RustqlError::Internal("EXISTS subquery not allowed in this context".to_string())
            })?;
            crate::plan_executor::evaluate_planned_subquery_exists_with_outer(
                db_ref,
                subquery_stmt,
                columns,
                row,
            )
        }
        Expression::Any { left, op, subquery } => {
            let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
            let db_ref = db.ok_or_else(|| {
                RustqlError::Internal("ANY subquery not allowed in this context".to_string())
            })?;
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
            let db_ref = db.ok_or_else(|| {
                RustqlError::Internal("ALL subquery not allowed in this context".to_string())
            })?;
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
    let mut text_idx = 0;
    let mut pattern_idx = 0;
    let mut last_percent_pattern_idx = None;
    let mut last_percent_text_idx = 0;

    while text_idx < text.len() {
        match next_char(pattern, pattern_idx) {
            Some(('%', next_pattern_idx)) => {
                pattern_idx = next_pattern_idx;
                last_percent_pattern_idx = Some(pattern_idx);
                last_percent_text_idx = text_idx;
            }
            Some(('_', next_pattern_idx)) => {
                let Some((_, next_text_idx)) = next_char(text, text_idx) else {
                    return false;
                };
                text_idx = next_text_idx;
                pattern_idx = next_pattern_idx;
            }
            Some((pattern_ch, next_pattern_idx)) => {
                if let Some((text_ch, next_text_idx)) = next_char(text, text_idx)
                    && text_ch == pattern_ch
                {
                    text_idx = next_text_idx;
                    pattern_idx = next_pattern_idx;
                } else if let Some(retry_pattern_idx) = last_percent_pattern_idx {
                    let Some((_, next_text_idx)) = next_char(text, last_percent_text_idx) else {
                        return false;
                    };
                    last_percent_text_idx = next_text_idx;
                    text_idx = last_percent_text_idx;
                    pattern_idx = retry_pattern_idx;
                } else {
                    return false;
                }
            }
            None => {
                if let Some(retry_pattern_idx) = last_percent_pattern_idx {
                    let Some((_, next_text_idx)) = next_char(text, last_percent_text_idx) else {
                        return false;
                    };
                    last_percent_text_idx = next_text_idx;
                    text_idx = last_percent_text_idx;
                    pattern_idx = retry_pattern_idx;
                } else {
                    return false;
                }
            }
        }
    }

    while let Some(('%', next_pattern_idx)) = next_char(pattern, pattern_idx) {
        pattern_idx = next_pattern_idx;
    }

    pattern_idx == pattern.len()
}

fn next_char(text: &str, idx: usize) -> Option<(char, usize)> {
    let ch = text.get(idx..)?.chars().next()?;
    Some((ch, idx + ch.len_utf8()))
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

#[cfg(test)]
mod tests {
    use super::match_like;

    #[test]
    fn like_matches_literal_and_wildcards() {
        assert!(match_like("alphabet", "alpha%"));
        assert!(match_like("alphabet", "%ha%et"));
        assert!(match_like("alphabet", "a_pha_et"));
        assert!(!match_like("alphabet", "alpha_"));
    }

    #[test]
    fn like_backtracks_after_percent() {
        assert!(match_like("abbc", "a%bc"));
        assert!(match_like("abcabc", "%abc"));
        assert!(!match_like("abcabx", "%abc"));
    }

    #[test]
    fn like_underscore_matches_one_unicode_scalar() {
        let text = "na\u{ef}ve";

        assert!(match_like(text, "na_ve"));
        assert!(match_like(text, "na%"));
        assert!(match_like(text, "n%v_"));
        assert!(match_like(text, "n____"));
        assert!(!match_like(text, "n_____"));
    }
}
