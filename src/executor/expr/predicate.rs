use super::*;

pub fn evaluate_expression(
    db: Option<&dyn DatabaseCatalog>,
    expr: &Expression,
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<bool, RustqlError> {
    match evaluate_predicate_value(db, expr, columns, row)? {
        Value::Boolean(value) => Ok(value),
        Value::Null => Ok(false),
        _ => Err(RustqlError::TypeMismatch(
            "Predicate expression must evaluate to BOOLEAN".to_string(),
        )),
    }
}

pub fn evaluate_predicate_value(
    db: Option<&dyn DatabaseCatalog>,
    expr: &Expression,
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<Value, RustqlError> {
    match expr {
        Expression::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                let left_value = evaluate_predicate_value(db, left, columns, row)?;
                let right_value = evaluate_predicate_value(db, right, columns, row)?;
                sql_and(left_value, right_value)
            }
            BinaryOperator::Or => {
                let left_value = evaluate_predicate_value(db, left, columns, row)?;
                let right_value = evaluate_predicate_value(db, right, columns, row)?;
                sql_or(left_value, right_value)
            }
            BinaryOperator::Like => {
                let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
                let (pattern_val, escape_val) = evaluate_like_pattern(db, right, columns, row)?;
                evaluate_like_values(left_val, pattern_val, escape_val, false)
            }
            BinaryOperator::ILike => {
                let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
                let (pattern_val, escape_val) = evaluate_like_pattern(db, right, columns, row)?;
                evaluate_like_values(left_val, pattern_val, escape_val, true)
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
                        if matches!(left_val, Value::Null)
                            || matches!(lower, Value::Null)
                            || matches!(upper, Value::Null)
                        {
                            Ok(Value::Null)
                        } else {
                            Ok(Value::Boolean(is_between(&left_val, &lower, &upper)))
                        }
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
                        value_matches_any(&left_val, &sub_vals)
                    }
                    _ => {
                        let right_val = evaluate_value_expression_with_db(right, columns, row, db)?;
                        compare_predicate_values(&left_val, op, &right_val)
                    }
                }
            }
            _ => {
                let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
                let right_val = evaluate_value_expression_with_db(right, columns, row, db)?;
                compare_predicate_values(&left_val, op, &right_val)
            }
        },
        Expression::In { left, values } => {
            let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
            let mut saw_unknown = matches!(left_val, Value::Null);
            for value_expr in values {
                let value = evaluate_value_expression_with_db(value_expr, columns, row, db)?;
                let comparison =
                    compare_predicate_values(&left_val, &BinaryOperator::Equal, &value)?;
                match comparison {
                    Value::Boolean(true) => return Ok(Value::Boolean(true)),
                    Value::Boolean(false) => {}
                    Value::Null => saw_unknown = true,
                    _ => {
                        return Err(RustqlError::TypeMismatch(
                            "IN comparison must produce BOOLEAN".to_string(),
                        ));
                    }
                }
            }
            if saw_unknown {
                Ok(Value::Null)
            } else {
                Ok(Value::Boolean(false))
            }
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
            .map(Value::Boolean)
        }
        Expression::Any { left, op, subquery } => {
            let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
            let db_ref = db.ok_or_else(|| {
                RustqlError::Internal("ANY subquery not allowed in this context".to_string())
            })?;
            let sub_vals = crate::plan_executor::evaluate_planned_subquery_values_with_outer(
                db_ref, subquery, columns, row,
            )?;
            let mut saw_unknown = matches!(left_val, Value::Null);
            for value in &sub_vals {
                let comparison = compare_predicate_values(&left_val, op, value)?;
                match comparison {
                    Value::Boolean(true) => return Ok(Value::Boolean(true)),
                    Value::Boolean(false) => {}
                    Value::Null => saw_unknown = true,
                    _ => {
                        return Err(RustqlError::TypeMismatch(
                            "ANY comparison must produce BOOLEAN".to_string(),
                        ));
                    }
                }
            }
            if saw_unknown {
                Ok(Value::Null)
            } else {
                Ok(Value::Boolean(false))
            }
        }
        Expression::All { left, op, subquery } => {
            let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
            let db_ref = db.ok_or_else(|| {
                RustqlError::Internal("ALL subquery not allowed in this context".to_string())
            })?;
            let sub_vals = crate::plan_executor::evaluate_planned_subquery_values_with_outer(
                db_ref, subquery, columns, row,
            )?;
            let mut saw_unknown = matches!(left_val, Value::Null);
            for value in &sub_vals {
                let comparison = compare_predicate_values(&left_val, op, value)?;
                match comparison {
                    Value::Boolean(true) => {}
                    Value::Boolean(false) => return Ok(Value::Boolean(false)),
                    Value::Null => saw_unknown = true,
                    _ => {
                        return Err(RustqlError::TypeMismatch(
                            "ALL comparison must produce BOOLEAN".to_string(),
                        ));
                    }
                }
            }
            if saw_unknown {
                Ok(Value::Null)
            } else {
                Ok(Value::Boolean(true))
            }
        }
        Expression::IsNull { expr, not } => {
            let value = evaluate_value_expression_with_db(expr, columns, row, db)?;
            let is_null = matches!(value, Value::Null);
            Ok(Value::Boolean(if *not { !is_null } else { is_null }))
        }
        Expression::IsDistinctFrom { left, right, not } => {
            let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
            let right_val = evaluate_value_expression_with_db(right, columns, row, db)?;
            let is_distinct = match (&left_val, &right_val) {
                (Value::Null, Value::Null) => false,
                (Value::Null, _) | (_, Value::Null) => true,
                _ => !compare_values(&left_val, &BinaryOperator::Equal, &right_val)?,
            };
            Ok(Value::Boolean(if *not {
                !is_distinct
            } else {
                is_distinct
            }))
        }
        Expression::Value(Value::Boolean(value)) => Ok(Value::Boolean(*value)),
        Expression::Value(Value::Null) => Ok(Value::Null),
        Expression::UnaryOp { op, expr } => match op {
            UnaryOperator::Not => sql_not(evaluate_predicate_value(db, expr, columns, row)?),
            _ => Err(RustqlError::Internal(
                "Unsupported unary operation in WHERE clause".to_string(),
            )),
        },
        _ => match evaluate_value_expression_with_db(expr, columns, row, db)? {
            Value::Boolean(value) => Ok(Value::Boolean(value)),
            Value::Null => Ok(Value::Null),
            _ => Err(RustqlError::TypeMismatch(
                "Predicate expression must evaluate to BOOLEAN".to_string(),
            )),
        },
    }
}

fn value_matches_any(left: &Value, values: &[Value]) -> Result<Value, RustqlError> {
    let mut saw_unknown = matches!(left, Value::Null);
    for value in values {
        let comparison = compare_predicate_values(left, &BinaryOperator::Equal, value)?;
        match comparison {
            Value::Boolean(true) => return Ok(Value::Boolean(true)),
            Value::Boolean(false) => {}
            Value::Null => saw_unknown = true,
            _ => {
                return Err(RustqlError::TypeMismatch(
                    "IN subquery comparison must produce BOOLEAN".to_string(),
                ));
            }
        }
    }

    if saw_unknown {
        Ok(Value::Null)
    } else {
        Ok(Value::Boolean(false))
    }
}

fn evaluate_like_pattern(
    db: Option<&dyn DatabaseCatalog>,
    pattern_expr: &Expression,
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<(Value, Option<Value>), RustqlError> {
    match pattern_expr {
        Expression::BinaryOp { left, op, right } if *op == BinaryOperator::Escape => {
            let pattern = evaluate_value_expression_with_db(left, columns, row, db)?;
            let escape = evaluate_value_expression_with_db(right, columns, row, db)?;
            Ok((pattern, Some(escape)))
        }
        _ => {
            let pattern = evaluate_value_expression_with_db(pattern_expr, columns, row, db)?;
            Ok((pattern, None))
        }
    }
}

fn evaluate_like_values(
    left_val: Value,
    pattern_val: Value,
    escape_val: Option<Value>,
    case_insensitive: bool,
) -> Result<Value, RustqlError> {
    if matches!(&left_val, Value::Null)
        || matches!(&pattern_val, Value::Null)
        || matches!(&escape_val, Some(Value::Null))
    {
        return Ok(Value::Null);
    }

    let (mut text, mut pattern) = match (left_val, pattern_val) {
        (Value::Text(text), Value::Text(pattern)) => (text, pattern),
        _ => {
            return Err(RustqlError::TypeMismatch(
                "LIKE operator requires text values".to_string(),
            ));
        }
    };

    let mut escape = match escape_val {
        Some(Value::Text(value)) => Some(value),
        Some(_) => {
            return Err(RustqlError::TypeMismatch(
                "LIKE ESCAPE requires a text value".to_string(),
            ));
        }
        None => None,
    };

    if case_insensitive {
        text = text.to_lowercase();
        pattern = pattern.to_lowercase();
        escape = escape.map(|value| value.to_lowercase());
    }

    let escape = match escape {
        Some(value) => Some(single_escape_char(&value)?),
        None => None,
    };

    Ok(Value::Boolean(match_like(&text, &pattern, escape)?))
}

fn single_escape_char(value: &str) -> Result<char, RustqlError> {
    let mut chars = value.chars();
    match (chars.next(), chars.next()) {
        (Some(ch), None) => Ok(ch),
        _ => Err(RustqlError::TypeMismatch(
            "LIKE ESCAPE requires a single-character text value".to_string(),
        )),
    }
}

fn compare_predicate_values(
    left: &Value,
    op: &BinaryOperator,
    right: &Value,
) -> Result<Value, RustqlError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        Ok(Value::Null)
    } else {
        compare_values(left, op, right).map(Value::Boolean)
    }
}

fn sql_and(left: Value, right: Value) -> Result<Value, RustqlError> {
    match (left, right) {
        (Value::Boolean(false), _) | (_, Value::Boolean(false)) => Ok(Value::Boolean(false)),
        (Value::Boolean(true), Value::Boolean(true)) => Ok(Value::Boolean(true)),
        (Value::Boolean(true), Value::Null) | (Value::Null, Value::Boolean(true)) => {
            Ok(Value::Null)
        }
        (Value::Null, Value::Null) => Ok(Value::Null),
        _ => Err(RustqlError::TypeMismatch(
            "AND operands must evaluate to BOOLEAN".to_string(),
        )),
    }
}

fn sql_or(left: Value, right: Value) -> Result<Value, RustqlError> {
    match (left, right) {
        (Value::Boolean(true), _) | (_, Value::Boolean(true)) => Ok(Value::Boolean(true)),
        (Value::Boolean(false), Value::Boolean(false)) => Ok(Value::Boolean(false)),
        (Value::Boolean(false), Value::Null) | (Value::Null, Value::Boolean(false)) => {
            Ok(Value::Null)
        }
        (Value::Null, Value::Null) => Ok(Value::Null),
        _ => Err(RustqlError::TypeMismatch(
            "OR operands must evaluate to BOOLEAN".to_string(),
        )),
    }
}

fn sql_not(value: Value) -> Result<Value, RustqlError> {
    match value {
        Value::Boolean(value) => Ok(Value::Boolean(!value)),
        Value::Null => Ok(Value::Null),
        _ => Err(RustqlError::TypeMismatch(
            "NOT operand must evaluate to BOOLEAN".to_string(),
        )),
    }
}

#[derive(Clone, Copy)]
enum LikePatternToken {
    AnySequence,
    AnySingle,
    Literal(char),
}

fn match_like(text: &str, pattern: &str, escape: Option<char>) -> Result<bool, RustqlError> {
    let text_chars: Vec<char> = text.chars().collect();
    let pattern_tokens = compile_like_pattern(pattern, escape)?;
    let mut memo = vec![vec![None; pattern_tokens.len() + 1]; text_chars.len() + 1];

    fn match_pattern(
        text: &[char],
        pattern: &[LikePatternToken],
        text_idx: usize,
        pattern_idx: usize,
        memo: &mut [Vec<Option<bool>>],
    ) -> bool {
        let text_len = text.len();
        let pattern_len = pattern.len();

        if let Some(result) = memo[text_idx][pattern_idx] {
            return result;
        }

        if pattern_idx == pattern_len {
            let result = text_idx == text_len;
            memo[text_idx][pattern_idx] = Some(result);
            return result;
        }

        let result = match pattern[pattern_idx] {
            LikePatternToken::AnySequence => {
                if pattern_idx + 1 == pattern_len {
                    true
                } else {
                    let mut matched = false;
                    for idx in text_idx..=text_len {
                        if match_pattern(text, pattern, idx, pattern_idx + 1, memo) {
                            matched = true;
                            break;
                        }
                    }
                    matched
                }
            }
            LikePatternToken::AnySingle => {
                text_idx < text_len
                    && match_pattern(text, pattern, text_idx + 1, pattern_idx + 1, memo)
            }
            LikePatternToken::Literal(ch) => {
                text_idx < text_len
                    && text[text_idx] == ch
                    && match_pattern(text, pattern, text_idx + 1, pattern_idx + 1, memo)
            }
        };

        memo[text_idx][pattern_idx] = Some(result);
        result
    }

    Ok(match_pattern(&text_chars, &pattern_tokens, 0, 0, &mut memo))
}

fn compile_like_pattern(
    pattern: &str,
    escape: Option<char>,
) -> Result<Vec<LikePatternToken>, RustqlError> {
    let mut tokens = Vec::new();
    let mut chars = pattern.chars();

    while let Some(ch) = chars.next() {
        if escape == Some(ch) {
            if let Some(escaped) = chars.next() {
                tokens.push(LikePatternToken::Literal(escaped));
            } else {
                return Err(RustqlError::TypeMismatch(
                    "LIKE pattern cannot end with the ESCAPE character".to_string(),
                ));
            }
        } else {
            match ch {
                '%' => tokens.push(LikePatternToken::AnySequence),
                '_' => tokens.push(LikePatternToken::AnySingle),
                _ => tokens.push(LikePatternToken::Literal(ch)),
            }
        }
    }

    Ok(tokens)
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
