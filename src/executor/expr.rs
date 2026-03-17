use crate::ast::*;
use crate::database::Database;
use crate::error::RustqlError;
use std::cmp::Ordering;

pub fn evaluate_expression(
    db: Option<&Database>,
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
                        let sub_vals = super::select::eval_subquery_values(db_ref, subquery_stmt)?;
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
            super::select::eval_subquery_exists_with_outer(db_ref, subquery_stmt, columns, row)
        }
        Expression::Any { left, op, subquery } => {
            let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
            let db_ref =
                db.ok_or_else(|| "ANY subquery not allowed in this context".to_string())?;
            let sub_vals = super::select::eval_subquery_values(db_ref, subquery)?;
            Ok(sub_vals
                .iter()
                .any(|v| compare_values(&left_val, op, v).unwrap_or(false)))
        }
        Expression::All { left, op, subquery } => {
            let left_val = evaluate_value_expression_with_db(left, columns, row, db)?;
            let db_ref =
                db.ok_or_else(|| "ALL subquery not allowed in this context".to_string())?;
            let sub_vals = super::select::eval_subquery_values(db_ref, subquery)?;
            Ok(sub_vals.is_empty()
                || sub_vals
                    .iter()
                    .all(|v| compare_values(&left_val, op, v).unwrap_or(false)))
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

pub fn evaluate_value_expression(
    expr: &Expression,
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<Value, RustqlError> {
    evaluate_value_expression_with_db(expr, columns, row, None)
}

pub fn evaluate_value_expression_with_db(
    expr: &Expression,
    columns: &[ColumnDefinition],
    row: &[Value],
    db: Option<&Database>,
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
            let evaluated_args: Vec<Value> = args
                .iter()
                .map(|a| evaluate_value_expression_with_db(a, columns, row, db))
                .collect::<Result<Vec<_>, _>>()?;
            match name {
                ScalarFunctionType::Upper => match evaluated_args.first() {
                    Some(Value::Text(s)) => Ok(Value::Text(s.to_uppercase())),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "UPPER requires a text argument".to_string(),
                    )),
                },
                ScalarFunctionType::Lower => match evaluated_args.first() {
                    Some(Value::Text(s)) => Ok(Value::Text(s.to_lowercase())),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "LOWER requires a text argument".to_string(),
                    )),
                },
                ScalarFunctionType::Length => match evaluated_args.first() {
                    Some(Value::Text(s)) => Ok(Value::Integer(s.len() as i64)),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "LENGTH requires a text argument".to_string(),
                    )),
                },
                ScalarFunctionType::Substring => {
                    let s = match evaluated_args.first() {
                        Some(Value::Text(s)) => s.clone(),
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "SUBSTRING requires a text first argument".to_string(),
                            ));
                        }
                    };
                    let start = match evaluated_args.get(1) {
                        Some(Value::Integer(i)) => (*i as usize).saturating_sub(1),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "SUBSTRING requires an integer start position".to_string(),
                            ));
                        }
                    };
                    let chars: Vec<char> = s.chars().collect();
                    if start >= chars.len() {
                        return Ok(Value::Text(String::new()));
                    }
                    let len = match evaluated_args.get(2) {
                        Some(Value::Integer(l)) => *l as usize,
                        None => chars.len() - start,
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "SUBSTRING length must be an integer".to_string(),
                            ));
                        }
                    };
                    let result: String = chars.iter().skip(start).take(len).collect();
                    Ok(Value::Text(result))
                }
                ScalarFunctionType::Abs => match evaluated_args.first() {
                    Some(Value::Integer(i)) => Ok(Value::Integer(i.abs())),
                    Some(Value::Float(f)) => Ok(Value::Float(f.abs())),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "ABS requires a numeric argument".to_string(),
                    )),
                },
                ScalarFunctionType::Round => {
                    let val = match evaluated_args.first() {
                        Some(Value::Float(f)) => *f,
                        Some(Value::Integer(i)) => *i as f64,
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "ROUND requires a numeric argument".to_string(),
                            ));
                        }
                    };
                    let decimals = match evaluated_args.get(1) {
                        Some(Value::Integer(d)) => *d as i32,
                        None => 0,
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "ROUND decimals must be an integer".to_string(),
                            ));
                        }
                    };
                    let factor = 10f64.powi(decimals);
                    Ok(Value::Float((val * factor).round() / factor))
                }
                ScalarFunctionType::Coalesce => {
                    for arg in &evaluated_args {
                        if !matches!(arg, Value::Null) {
                            return Ok(arg.clone());
                        }
                    }
                    Ok(Value::Null)
                }
                ScalarFunctionType::Trim => match evaluated_args.first() {
                    Some(Value::Text(s)) => Ok(Value::Text(s.trim().to_string())),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "TRIM requires a text argument".to_string(),
                    )),
                },
                ScalarFunctionType::Replace => {
                    let s = match evaluated_args.first() {
                        Some(Value::Text(s)) => s.clone(),
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "REPLACE requires a text first argument".to_string(),
                            ));
                        }
                    };
                    let from = match evaluated_args.get(1) {
                        Some(Value::Text(s)) => s.clone(),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "REPLACE requires a text second argument".to_string(),
                            ));
                        }
                    };
                    let to = match evaluated_args.get(2) {
                        Some(Value::Text(s)) => s.clone(),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "REPLACE requires a text third argument".to_string(),
                            ));
                        }
                    };
                    Ok(Value::Text(s.replace(&from, &to)))
                }
                ScalarFunctionType::ConcatFn => {
                    let mut result = String::new();
                    for arg in &evaluated_args {
                        match arg {
                            Value::Null => {}
                            other => result.push_str(&format_value(other)),
                        }
                    }
                    Ok(Value::Text(result))
                }
                ScalarFunctionType::Position => {
                    let needle = match evaluated_args.first() {
                        Some(Value::Text(s)) => s.clone(),
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "POSITION requires a text first argument".to_string(),
                            ));
                        }
                    };
                    let haystack = match evaluated_args.get(1) {
                        Some(Value::Text(s)) => s.clone(),
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "POSITION requires a text second argument".to_string(),
                            ));
                        }
                    };
                    match haystack.find(&needle) {
                        Some(pos) => Ok(Value::Integer(pos as i64 + 1)),
                        None => Ok(Value::Integer(0)),
                    }
                }
                ScalarFunctionType::Instr => {
                    let haystack = match evaluated_args.first() {
                        Some(Value::Text(s)) => s.clone(),
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "INSTR requires a text first argument".to_string(),
                            ));
                        }
                    };
                    let needle = match evaluated_args.get(1) {
                        Some(Value::Text(s)) => s.clone(),
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "INSTR requires a text second argument".to_string(),
                            ));
                        }
                    };
                    match haystack.find(&needle) {
                        Some(pos) => Ok(Value::Integer(pos as i64 + 1)),
                        None => Ok(Value::Integer(0)),
                    }
                }
                ScalarFunctionType::Ceil => match evaluated_args.first() {
                    Some(Value::Float(f)) => Ok(Value::Integer(f.ceil() as i64)),
                    Some(Value::Integer(i)) => Ok(Value::Integer(*i)),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "CEIL requires a numeric argument".to_string(),
                    )),
                },
                ScalarFunctionType::Floor => match evaluated_args.first() {
                    Some(Value::Float(f)) => Ok(Value::Integer(f.floor() as i64)),
                    Some(Value::Integer(i)) => Ok(Value::Integer(*i)),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "FLOOR requires a numeric argument".to_string(),
                    )),
                },
                ScalarFunctionType::Sqrt => match evaluated_args.first() {
                    Some(Value::Float(f)) => {
                        if *f < 0.0 {
                            Err(RustqlError::TypeMismatch(
                                "SQRT of a negative number".to_string(),
                            ))
                        } else {
                            Ok(Value::Float(f.sqrt()))
                        }
                    }
                    Some(Value::Integer(i)) => {
                        let f = *i as f64;
                        if f < 0.0 {
                            Err(RustqlError::TypeMismatch(
                                "SQRT of a negative number".to_string(),
                            ))
                        } else {
                            Ok(Value::Float(f.sqrt()))
                        }
                    }
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "SQRT requires a numeric argument".to_string(),
                    )),
                },
                ScalarFunctionType::Power => {
                    let base = match evaluated_args.first() {
                        Some(Value::Float(f)) => *f,
                        Some(Value::Integer(i)) => *i as f64,
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "POWER requires a numeric first argument".to_string(),
                            ));
                        }
                    };
                    let exp = match evaluated_args.get(1) {
                        Some(Value::Float(f)) => *f,
                        Some(Value::Integer(i)) => *i as f64,
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "POWER requires a numeric second argument".to_string(),
                            ));
                        }
                    };
                    Ok(Value::Float(base.powf(exp)))
                }
                ScalarFunctionType::Mod => {
                    let left = match evaluated_args.first() {
                        Some(Value::Integer(i)) => *i,
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "MOD requires integer arguments".to_string(),
                            ));
                        }
                    };
                    let right = match evaluated_args.get(1) {
                        Some(Value::Integer(i)) => *i,
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "MOD requires integer arguments".to_string(),
                            ));
                        }
                    };
                    if right == 0 {
                        Err(RustqlError::DivisionByZero)
                    } else {
                        Ok(Value::Integer(left % right))
                    }
                }
                ScalarFunctionType::Now => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default();
                    let secs = now.as_secs();
                    let days = secs / 86400;
                    let remaining = secs % 86400;
                    let hours = remaining / 3600;
                    let minutes = (remaining % 3600) / 60;
                    let seconds = remaining % 60;

                    let (year, month, day) = days_to_ymd(days as i64);
                    Ok(Value::DateTime(format!(
                        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
                        year, month, day, hours, minutes, seconds
                    )))
                }
                ScalarFunctionType::Year => match evaluated_args.first() {
                    Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => {
                        match parse_date_components(d) {
                            Some((y, _, _)) => Ok(Value::Integer(y)),
                            None => Err(RustqlError::TypeMismatch(
                                "Cannot extract YEAR from value".to_string(),
                            )),
                        }
                    }
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "YEAR requires a date/datetime argument".to_string(),
                    )),
                },
                ScalarFunctionType::Month => match evaluated_args.first() {
                    Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => {
                        match parse_date_components(d) {
                            Some((_, m, _)) => Ok(Value::Integer(m)),
                            None => Err(RustqlError::TypeMismatch(
                                "Cannot extract MONTH from value".to_string(),
                            )),
                        }
                    }
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "MONTH requires a date/datetime argument".to_string(),
                    )),
                },
                ScalarFunctionType::Day => match evaluated_args.first() {
                    Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => {
                        match parse_date_components(d) {
                            Some((_, _, d)) => Ok(Value::Integer(d)),
                            None => Err(RustqlError::TypeMismatch(
                                "Cannot extract DAY from value".to_string(),
                            )),
                        }
                    }
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "DAY requires a date/datetime argument".to_string(),
                    )),
                },
                ScalarFunctionType::DateAdd => {
                    let date_str = match evaluated_args.first() {
                        Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => {
                            d.clone()
                        }
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "DATE_ADD requires a date first argument".to_string(),
                            ));
                        }
                    };
                    let days_to_add = match evaluated_args.get(1) {
                        Some(Value::Integer(i)) => *i,
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "DATE_ADD requires an integer second argument".to_string(),
                            ));
                        }
                    };
                    let (y, m, d) = parse_date_components(&date_str).ok_or_else(|| {
                        RustqlError::TypeMismatch("Invalid date format".to_string())
                    })?;
                    let jdn = ymd_to_days(y, m, d);
                    let new_jdn = jdn + days_to_add;
                    let (ny, nm, nd) = days_to_ymd(new_jdn);
                    Ok(Value::Date(format!("{:04}-{:02}-{:02}", ny, nm, nd)))
                }
                ScalarFunctionType::Datediff => {
                    let date1_str = match evaluated_args.first() {
                        Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => {
                            d.clone()
                        }
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "DATEDIFF requires a date first argument".to_string(),
                            ));
                        }
                    };
                    let date2_str = match evaluated_args.get(1) {
                        Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => {
                            d.clone()
                        }
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "DATEDIFF requires a date second argument".to_string(),
                            ));
                        }
                    };
                    let (y1, m1, d1) = parse_date_components(&date1_str).ok_or_else(|| {
                        RustqlError::TypeMismatch("Invalid first date format".to_string())
                    })?;
                    let (y2, m2, d2) = parse_date_components(&date2_str).ok_or_else(|| {
                        RustqlError::TypeMismatch("Invalid second date format".to_string())
                    })?;
                    let jdn1 = ymd_to_days(y1, m1, d1);
                    let jdn2 = ymd_to_days(y2, m2, d2);
                    Ok(Value::Integer(jdn1 - jdn2))
                }
                ScalarFunctionType::Nullif => {
                    if evaluated_args.len() < 2 {
                        return Err(RustqlError::TypeMismatch(
                            "NULLIF requires two arguments".to_string(),
                        ));
                    }
                    if evaluated_args[0] == evaluated_args[1] {
                        Ok(Value::Null)
                    } else {
                        Ok(evaluated_args[0].clone())
                    }
                }
                ScalarFunctionType::Greatest => {
                    let non_null: Vec<&Value> = evaluated_args
                        .iter()
                        .filter(|v| !matches!(v, Value::Null))
                        .collect();
                    if non_null.is_empty() {
                        Ok(Value::Null)
                    } else {
                        let mut max = non_null[0];
                        for v in &non_null[1..] {
                            if (*v).cmp(max) == std::cmp::Ordering::Greater {
                                max = *v;
                            }
                        }
                        Ok(max.clone())
                    }
                }
                ScalarFunctionType::Least => {
                    let non_null: Vec<&Value> = evaluated_args
                        .iter()
                        .filter(|v| !matches!(v, Value::Null))
                        .collect();
                    if non_null.is_empty() {
                        Ok(Value::Null)
                    } else {
                        let mut min = non_null[0];
                        for v in &non_null[1..] {
                            if (*v).cmp(min) == std::cmp::Ordering::Less {
                                min = *v;
                            }
                        }
                        Ok(min.clone())
                    }
                }
                ScalarFunctionType::Lpad => {
                    let s = match evaluated_args.first() {
                        Some(Value::Text(s)) => s.clone(),
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "LPAD requires a text first argument".to_string(),
                            ));
                        }
                    };
                    let len = match evaluated_args.get(1) {
                        Some(Value::Integer(i)) => *i as usize,
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "LPAD requires an integer second argument".to_string(),
                            ));
                        }
                    };
                    let pad = match evaluated_args.get(2) {
                        Some(Value::Text(p)) => p.clone(),
                        None => " ".to_string(),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "LPAD pad argument must be text".to_string(),
                            ));
                        }
                    };
                    let chars: Vec<char> = s.chars().collect();
                    if chars.len() >= len {
                        Ok(Value::Text(chars[..len].iter().collect()))
                    } else {
                        let needed = len - chars.len();
                        let pad_chars: Vec<char> = pad.chars().collect();
                        let mut result = String::new();
                        for i in 0..needed {
                            result.push(pad_chars[i % pad_chars.len()]);
                        }
                        result.extend(chars);
                        Ok(Value::Text(result))
                    }
                }
                ScalarFunctionType::Rpad => {
                    let s = match evaluated_args.first() {
                        Some(Value::Text(s)) => s.clone(),
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "RPAD requires a text first argument".to_string(),
                            ));
                        }
                    };
                    let len = match evaluated_args.get(1) {
                        Some(Value::Integer(i)) => *i as usize,
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "RPAD requires an integer second argument".to_string(),
                            ));
                        }
                    };
                    let pad = match evaluated_args.get(2) {
                        Some(Value::Text(p)) => p.clone(),
                        None => " ".to_string(),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "RPAD pad argument must be text".to_string(),
                            ));
                        }
                    };
                    let chars: Vec<char> = s.chars().collect();
                    if chars.len() >= len {
                        Ok(Value::Text(chars[..len].iter().collect()))
                    } else {
                        let needed = len - chars.len();
                        let pad_chars: Vec<char> = pad.chars().collect();
                        let mut result: String = chars.into_iter().collect();
                        for i in 0..needed {
                            result.push(pad_chars[i % pad_chars.len()]);
                        }
                        Ok(Value::Text(result))
                    }
                }
                ScalarFunctionType::LeftFn => {
                    let s = match evaluated_args.first() {
                        Some(Value::Text(s)) => s.clone(),
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "LEFT requires a text first argument".to_string(),
                            ));
                        }
                    };
                    let n = match evaluated_args.get(1) {
                        Some(Value::Integer(i)) => *i as usize,
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "LEFT requires an integer second argument".to_string(),
                            ));
                        }
                    };
                    let result: String = s.chars().take(n).collect();
                    Ok(Value::Text(result))
                }
                ScalarFunctionType::RightFn => {
                    let s = match evaluated_args.first() {
                        Some(Value::Text(s)) => s.clone(),
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "RIGHT requires a text first argument".to_string(),
                            ));
                        }
                    };
                    let n = match evaluated_args.get(1) {
                        Some(Value::Integer(i)) => *i as usize,
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "RIGHT requires an integer second argument".to_string(),
                            ));
                        }
                    };
                    let chars: Vec<char> = s.chars().collect();
                    let start = chars.len().saturating_sub(n);
                    let result: String = chars[start..].iter().collect();
                    Ok(Value::Text(result))
                }
                ScalarFunctionType::Reverse => match evaluated_args.first() {
                    Some(Value::Text(s)) => Ok(Value::Text(s.chars().rev().collect())),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "REVERSE requires a text argument".to_string(),
                    )),
                },
                ScalarFunctionType::Repeat => {
                    let s = match evaluated_args.first() {
                        Some(Value::Text(s)) => s.clone(),
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "REPEAT requires a text first argument".to_string(),
                            ));
                        }
                    };
                    let n = match evaluated_args.get(1) {
                        Some(Value::Integer(i)) => *i as usize,
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "REPEAT requires an integer second argument".to_string(),
                            ));
                        }
                    };
                    Ok(Value::Text(s.repeat(n)))
                }
                ScalarFunctionType::Log => {
                    if evaluated_args.len() == 1 {
                        match evaluated_args.first() {
                            Some(Value::Float(f)) => Ok(Value::Float(f.ln())),
                            Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).ln())),
                            Some(Value::Null) => Ok(Value::Null),
                            _ => Err(RustqlError::TypeMismatch(
                                "LOG requires a numeric argument".to_string(),
                            )),
                        }
                    } else {
                        let base = match evaluated_args.first() {
                            Some(Value::Float(f)) => *f,
                            Some(Value::Integer(i)) => *i as f64,
                            Some(Value::Null) => return Ok(Value::Null),
                            _ => {
                                return Err(RustqlError::TypeMismatch(
                                    "LOG requires numeric arguments".to_string(),
                                ));
                            }
                        };
                        let x = match evaluated_args.get(1) {
                            Some(Value::Float(f)) => *f,
                            Some(Value::Integer(i)) => *i as f64,
                            Some(Value::Null) => return Ok(Value::Null),
                            _ => {
                                return Err(RustqlError::TypeMismatch(
                                    "LOG requires numeric arguments".to_string(),
                                ));
                            }
                        };
                        Ok(Value::Float(x.ln() / base.ln()))
                    }
                }
                ScalarFunctionType::Exp => match evaluated_args.first() {
                    Some(Value::Float(f)) => Ok(Value::Float(f.exp())),
                    Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).exp())),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "EXP requires a numeric argument".to_string(),
                    )),
                },
                ScalarFunctionType::Sign => match evaluated_args.first() {
                    Some(Value::Integer(i)) => {
                        if *i > 0 {
                            Ok(Value::Integer(1))
                        } else if *i < 0 {
                            Ok(Value::Integer(-1))
                        } else {
                            Ok(Value::Integer(0))
                        }
                    }
                    Some(Value::Float(f)) => {
                        if *f > 0.0 {
                            Ok(Value::Integer(1))
                        } else if *f < 0.0 {
                            Ok(Value::Integer(-1))
                        } else {
                            Ok(Value::Integer(0))
                        }
                    }
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "SIGN requires a numeric argument".to_string(),
                    )),
                },
                ScalarFunctionType::DateTrunc => {
                    let part = match evaluated_args.first() {
                        Some(Value::Text(s)) => s.to_lowercase(),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "DATE_TRUNC requires a text part argument".to_string(),
                            ));
                        }
                    };
                    let date_str = match evaluated_args.get(1) {
                        Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => {
                            d.clone()
                        }
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "DATE_TRUNC requires a date second argument".to_string(),
                            ));
                        }
                    };
                    let (y, m, d) = parse_date_components(&date_str).ok_or_else(|| {
                        RustqlError::TypeMismatch("Invalid date format".to_string())
                    })?;
                    match part.as_str() {
                        "year" => Ok(Value::Date(format!("{:04}-01-01", y))),
                        "month" => Ok(Value::Date(format!("{:04}-{:02}-01", y, m))),
                        "day" => Ok(Value::Date(format!("{:04}-{:02}-{:02}", y, m, d))),
                        _ => Err(RustqlError::TypeMismatch(format!(
                            "DATE_TRUNC unsupported part: {}",
                            part
                        ))),
                    }
                }
                ScalarFunctionType::Extract => {
                    let part = match evaluated_args.first() {
                        Some(Value::Text(s)) => s.to_lowercase(),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "EXTRACT requires a text part argument".to_string(),
                            ));
                        }
                    };
                    let date_str = match evaluated_args.get(1) {
                        Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => {
                            d.clone()
                        }
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "EXTRACT requires a date/datetime second argument".to_string(),
                            ));
                        }
                    };
                    match part.as_str() {
                        "year" => {
                            let (y, _, _) = parse_date_components(&date_str).ok_or_else(|| {
                                RustqlError::TypeMismatch("Invalid date format".to_string())
                            })?;
                            Ok(Value::Integer(y))
                        }
                        "month" => {
                            let (_, m, _) = parse_date_components(&date_str).ok_or_else(|| {
                                RustqlError::TypeMismatch("Invalid date format".to_string())
                            })?;
                            Ok(Value::Integer(m))
                        }
                        "day" => {
                            let (_, _, d) = parse_date_components(&date_str).ok_or_else(|| {
                                RustqlError::TypeMismatch("Invalid date format".to_string())
                            })?;
                            Ok(Value::Integer(d))
                        }
                        "hour" | "minute" | "second" => {
                            let time_part = if let Some(space_pos) = date_str.find(' ') {
                                &date_str[space_pos + 1..]
                            } else {
                                &date_str
                            };
                            let parts: Vec<&str> = time_part.split(':').collect();
                            match part.as_str() {
                                "hour" => {
                                    let h = parts
                                        .first()
                                        .and_then(|s| s.parse::<i64>().ok())
                                        .unwrap_or(0);
                                    Ok(Value::Integer(h))
                                }
                                "minute" => {
                                    let m = parts
                                        .get(1)
                                        .and_then(|s| s.parse::<i64>().ok())
                                        .unwrap_or(0);
                                    Ok(Value::Integer(m))
                                }
                                "second" => {
                                    let s = parts
                                        .get(2)
                                        .and_then(|s| s.parse::<i64>().ok())
                                        .unwrap_or(0);
                                    Ok(Value::Integer(s))
                                }
                                _ => unreachable!(),
                            }
                        }
                        "quarter" => {
                            let (_, m, _) = parse_date_components(&date_str).ok_or_else(|| {
                                RustqlError::TypeMismatch("Invalid date format".to_string())
                            })?;
                            Ok(Value::Integer((m - 1) / 3 + 1))
                        }
                        "week" => {
                            let (y, m, d) = parse_date_components(&date_str).ok_or_else(|| {
                                RustqlError::TypeMismatch("Invalid date format".to_string())
                            })?;
                            let day_of_year = ymd_to_days(y, m, d) - ymd_to_days(y, 1, 1);
                            Ok(Value::Integer(day_of_year / 7 + 1))
                        }
                        "dow" | "dayofweek" => {
                            let (y, m, d) = parse_date_components(&date_str).ok_or_else(|| {
                                RustqlError::TypeMismatch("Invalid date format".to_string())
                            })?;
                            let days = ymd_to_days(y, m, d);
                            Ok(Value::Integer(((days % 7 + 7 + 4) % 7) + 1))
                        }
                        _ => Err(RustqlError::TypeMismatch(format!(
                            "EXTRACT unsupported part: {}",
                            part
                        ))),
                    }
                }
                ScalarFunctionType::Ltrim => match evaluated_args.first() {
                    Some(Value::Text(s)) => Ok(Value::Text(s.trim_start().to_string())),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "LTRIM requires a text argument".to_string(),
                    )),
                },
                ScalarFunctionType::Rtrim => match evaluated_args.first() {
                    Some(Value::Text(s)) => Ok(Value::Text(s.trim_end().to_string())),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "RTRIM requires a text argument".to_string(),
                    )),
                },
                ScalarFunctionType::Ascii => match evaluated_args.first() {
                    Some(Value::Text(s)) => {
                        if s.is_empty() {
                            Ok(Value::Null)
                        } else {
                            Ok(Value::Integer(s.chars().next().unwrap() as i64))
                        }
                    }
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "ASCII requires a text argument".to_string(),
                    )),
                },
                ScalarFunctionType::Chr => match evaluated_args.first() {
                    Some(Value::Integer(i)) => match char::from_u32(*i as u32) {
                        Some(c) => Ok(Value::Text(c.to_string())),
                        None => Ok(Value::Null),
                    },
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "CHR requires an integer argument".to_string(),
                    )),
                },
                ScalarFunctionType::Sin => match evaluated_args.first() {
                    Some(Value::Float(f)) => Ok(Value::Float(f.sin())),
                    Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).sin())),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "SIN requires a numeric argument".to_string(),
                    )),
                },
                ScalarFunctionType::Cos => match evaluated_args.first() {
                    Some(Value::Float(f)) => Ok(Value::Float(f.cos())),
                    Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).cos())),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "COS requires a numeric argument".to_string(),
                    )),
                },
                ScalarFunctionType::Tan => match evaluated_args.first() {
                    Some(Value::Float(f)) => Ok(Value::Float(f.tan())),
                    Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).tan())),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "TAN requires a numeric argument".to_string(),
                    )),
                },
                ScalarFunctionType::Asin => match evaluated_args.first() {
                    Some(Value::Float(f)) => Ok(Value::Float(f.asin())),
                    Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).asin())),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "ASIN requires a numeric argument".to_string(),
                    )),
                },
                ScalarFunctionType::Acos => match evaluated_args.first() {
                    Some(Value::Float(f)) => Ok(Value::Float(f.acos())),
                    Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).acos())),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "ACOS requires a numeric argument".to_string(),
                    )),
                },
                ScalarFunctionType::Atan => match evaluated_args.first() {
                    Some(Value::Float(f)) => Ok(Value::Float(f.atan())),
                    Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).atan())),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "ATAN requires a numeric argument".to_string(),
                    )),
                },
                ScalarFunctionType::Atan2 => {
                    let y = match evaluated_args.first() {
                        Some(Value::Float(f)) => *f,
                        Some(Value::Integer(i)) => *i as f64,
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "ATAN2 requires numeric arguments".to_string(),
                            ));
                        }
                    };
                    let x = match evaluated_args.get(1) {
                        Some(Value::Float(f)) => *f,
                        Some(Value::Integer(i)) => *i as f64,
                        Some(Value::Null) => return Ok(Value::Null),
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "ATAN2 requires numeric arguments".to_string(),
                            ));
                        }
                    };
                    Ok(Value::Float(y.atan2(x)))
                }
                ScalarFunctionType::Random => {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default();
                    let nanos = now.subsec_nanos() as u64;
                    let secs = now.as_secs();
                    let seed = secs.wrapping_mul(6364136223846793005).wrapping_add(nanos);
                    let val = (seed as f64) / (u64::MAX as f64);
                    Ok(Value::Float(val.abs()))
                }
                ScalarFunctionType::Degrees => match evaluated_args.first() {
                    Some(Value::Float(f)) => Ok(Value::Float(f.to_degrees())),
                    Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).to_degrees())),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "DEGREES requires a numeric argument".to_string(),
                    )),
                },
                ScalarFunctionType::Radians => match evaluated_args.first() {
                    Some(Value::Float(f)) => Ok(Value::Float(f.to_radians())),
                    Some(Value::Integer(i)) => Ok(Value::Float((*i as f64).to_radians())),
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "RADIANS requires a numeric argument".to_string(),
                    )),
                },
                ScalarFunctionType::Quarter => match evaluated_args.first() {
                    Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => {
                        match parse_date_components(d) {
                            Some((_, m, _)) => Ok(Value::Integer((m - 1) / 3 + 1)),
                            None => Err(RustqlError::TypeMismatch(
                                "Cannot extract QUARTER from value".to_string(),
                            )),
                        }
                    }
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "QUARTER requires a date argument".to_string(),
                    )),
                },
                ScalarFunctionType::Week => match evaluated_args.first() {
                    Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => {
                        match parse_date_components(d) {
                            Some((y, m, d_val)) => {
                                let day_of_year = ymd_to_days(y, m, d_val) - ymd_to_days(y, 1, 1);
                                Ok(Value::Integer(day_of_year / 7 + 1))
                            }
                            None => Err(RustqlError::TypeMismatch(
                                "Cannot extract WEEK from value".to_string(),
                            )),
                        }
                    }
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "WEEK requires a date argument".to_string(),
                    )),
                },
                ScalarFunctionType::DayOfWeek => match evaluated_args.first() {
                    Some(Value::Date(d)) | Some(Value::DateTime(d)) | Some(Value::Text(d)) => {
                        match parse_date_components(d) {
                            Some((y, m, d_val)) => {
                                let days = ymd_to_days(y, m, d_val);
                                Ok(Value::Integer(((days % 7 + 7 + 4) % 7) + 1))
                            }
                            None => Err(RustqlError::TypeMismatch(
                                "Cannot extract DAYOFWEEK from value".to_string(),
                            )),
                        }
                    }
                    Some(Value::Null) => Ok(Value::Null),
                    _ => Err(RustqlError::TypeMismatch(
                        "DAYOFWEEK requires a date argument".to_string(),
                    )),
                },
                ScalarFunctionType::Pi => Ok(Value::Float(std::f64::consts::PI)),
                ScalarFunctionType::Trunc => {
                    let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
                    let precision = if args.len() > 1 {
                        match evaluate_value_expression_with_db(&args[1], columns, row, db)? {
                            Value::Integer(n) => n as i32,
                            Value::Float(f) => f as i32,
                            _ => 0,
                        }
                    } else {
                        0
                    };
                    match val {
                        Value::Float(f) => {
                            let factor = 10f64.powi(precision);
                            Ok(Value::Float((f * factor).trunc() / factor))
                        }
                        Value::Integer(n) => {
                            if precision >= 0 {
                                Ok(Value::Integer(n))
                            } else {
                                let factor = 10i64.pow((-precision) as u32);
                                Ok(Value::Integer((n / factor) * factor))
                            }
                        }
                        _ => Ok(Value::Null),
                    }
                }
                ScalarFunctionType::Log10 => {
                    let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
                    match val {
                        Value::Float(f) => Ok(Value::Float(f.log10())),
                        Value::Integer(n) => Ok(Value::Float((n as f64).log10())),
                        _ => Ok(Value::Null),
                    }
                }
                ScalarFunctionType::Log2 => {
                    let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
                    match val {
                        Value::Float(f) => Ok(Value::Float(f.log2())),
                        Value::Integer(n) => Ok(Value::Float((n as f64).log2())),
                        _ => Ok(Value::Null),
                    }
                }
                ScalarFunctionType::Cbrt => {
                    let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
                    match val {
                        Value::Float(f) => Ok(Value::Float(f.cbrt())),
                        Value::Integer(n) => Ok(Value::Float((n as f64).cbrt())),
                        _ => Ok(Value::Null),
                    }
                }
                ScalarFunctionType::Gcd => {
                    let a = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
                    let b = evaluate_value_expression_with_db(&args[1], columns, row, db)?;
                    match (a, b) {
                        (Value::Integer(mut a), Value::Integer(mut b)) => {
                            a = a.abs();
                            b = b.abs();
                            while b != 0 {
                                let t = b;
                                b = a % b;
                                a = t;
                            }
                            Ok(Value::Integer(a))
                        }
                        _ => Ok(Value::Null),
                    }
                }
                ScalarFunctionType::Lcm => {
                    let a = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
                    let b = evaluate_value_expression_with_db(&args[1], columns, row, db)?;
                    match (a, b) {
                        (Value::Integer(a), Value::Integer(b)) => {
                            if a == 0 && b == 0 {
                                Ok(Value::Integer(0))
                            } else {
                                let mut ga = a.abs();
                                let mut gb = b.abs();
                                let prod = ga * gb;
                                while gb != 0 {
                                    let t = gb;
                                    gb = ga % gb;
                                    ga = t;
                                }
                                Ok(Value::Integer(prod / ga))
                            }
                        }
                        _ => Ok(Value::Null),
                    }
                }
                ScalarFunctionType::Initcap => {
                    let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
                    match val {
                        Value::Text(s) => {
                            let mut result = String::new();
                            let mut capitalize_next = true;
                            for ch in s.chars() {
                                if ch.is_whitespace() || !ch.is_alphanumeric() {
                                    capitalize_next = true;
                                    result.push(ch);
                                } else if capitalize_next {
                                    result.extend(ch.to_uppercase());
                                    capitalize_next = false;
                                } else {
                                    result.extend(ch.to_lowercase());
                                }
                            }
                            Ok(Value::Text(result))
                        }
                        _ => Ok(Value::Null),
                    }
                }
                ScalarFunctionType::SplitPart => {
                    let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
                    let delim = evaluate_value_expression_with_db(&args[1], columns, row, db)?;
                    let part = evaluate_value_expression_with_db(&args[2], columns, row, db)?;
                    match (val, delim, part) {
                        (Value::Text(s), Value::Text(d), Value::Integer(n)) => {
                            let parts: Vec<&str> = s.split(&d).collect();
                            let idx = (n - 1) as usize;
                            if idx < parts.len() {
                                Ok(Value::Text(parts[idx].to_string()))
                            } else {
                                Ok(Value::Text(String::new()))
                            }
                        }
                        _ => Ok(Value::Null),
                    }
                }
                ScalarFunctionType::Translate => {
                    let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
                    let from = evaluate_value_expression_with_db(&args[1], columns, row, db)?;
                    let to = evaluate_value_expression_with_db(&args[2], columns, row, db)?;
                    match (val, from, to) {
                        (Value::Text(s), Value::Text(from_chars), Value::Text(to_chars)) => {
                            let from_v: Vec<char> = from_chars.chars().collect();
                            let to_v: Vec<char> = to_chars.chars().collect();
                            let result: String = s
                                .chars()
                                .filter_map(|ch| {
                                    if let Some(pos) = from_v.iter().position(|&fc| fc == ch) {
                                        if pos < to_v.len() {
                                            Some(to_v[pos])
                                        } else {
                                            None
                                        }
                                    } else {
                                        Some(ch)
                                    }
                                })
                                .collect();
                            Ok(Value::Text(result))
                        }
                        _ => Ok(Value::Null),
                    }
                }
                ScalarFunctionType::RegexpMatch => {
                    let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
                    let pattern = evaluate_value_expression_with_db(&args[1], columns, row, db)?;
                    match (val, pattern) {
                        (Value::Text(s), Value::Text(p)) => match regex::Regex::new(&p) {
                            Ok(re) => {
                                if let Some(m) = re.find(&s) {
                                    Ok(Value::Text(m.as_str().to_string()))
                                } else {
                                    Ok(Value::Null)
                                }
                            }
                            Err(_) => Ok(Value::Null),
                        },
                        _ => Ok(Value::Null),
                    }
                }
                ScalarFunctionType::RegexpReplace => {
                    let val = evaluate_value_expression_with_db(&args[0], columns, row, db)?;
                    let pattern = evaluate_value_expression_with_db(&args[1], columns, row, db)?;
                    let replacement =
                        evaluate_value_expression_with_db(&args[2], columns, row, db)?;
                    match (val, pattern, replacement) {
                        (Value::Text(s), Value::Text(p), Value::Text(r)) => {
                            match regex::Regex::new(&p) {
                                Ok(re) => {
                                    Ok(Value::Text(re.replace_all(&s, r.as_str()).to_string()))
                                }
                                Err(_) => Ok(Value::Null),
                            }
                        }
                        _ => Ok(Value::Null),
                    }
                }
            }
        }
        Expression::Cast { expr, data_type } => {
            let val = evaluate_value_expression_with_db(expr, columns, row, db)?;
            execute_cast(val, data_type)
        }
        Expression::WindowFunction { .. } => Err(RustqlError::Internal(
            "Window functions must be evaluated in a separate pass".to_string(),
        )),
        Expression::Subquery(subquery) => {
            let db_ref = db.ok_or_else(|| {
                RustqlError::Internal("Scalar subquery not allowed in this context".to_string())
            })?;
            let result =
                super::select::execute_select_internal((**subquery).clone(), db_ref, None)?;
            if result.rows.is_empty() {
                Ok(Value::Null)
            } else if result.rows.len() > 1 {
                Err(RustqlError::Internal(
                    "Scalar subquery returned more than one row".to_string(),
                ))
            } else if result.rows[0].len() != 1 {
                Err(RustqlError::Internal(
                    "Scalar subquery must return exactly one column".to_string(),
                ))
            } else {
                Ok(result.rows[0][0].clone())
            }
        }
        _ => Err(RustqlError::Internal(
            "Complex expressions not yet supported in SELECT".to_string(),
        )),
    }
}

pub fn compare_values(
    left: &Value,
    op: &BinaryOperator,
    right: &Value,
) -> Result<bool, RustqlError> {
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
            _ => {
                return Err(RustqlError::TypeMismatch(
                    "Invalid operator for numeric comparison".to_string(),
                ));
            }
        })
    } else {
        match (left, right, op) {
            (Value::Text(l), Value::Text(r), op) => Ok(match op {
                BinaryOperator::Equal => l == r,
                BinaryOperator::NotEqual => l != r,
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "Invalid operator for strings".to_string(),
                    ));
                }
            }),
            (Value::Date(l), Value::Date(r), op) => Ok(match op {
                BinaryOperator::Equal => l == r,
                BinaryOperator::NotEqual => l != r,
                BinaryOperator::LessThan => l < r,
                BinaryOperator::LessThanOrEqual => l <= r,
                BinaryOperator::GreaterThan => l > r,
                BinaryOperator::GreaterThanOrEqual => l >= r,
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "Invalid operator for dates".to_string(),
                    ));
                }
            }),
            (Value::Time(l), Value::Time(r), op) => Ok(match op {
                BinaryOperator::Equal => l == r,
                BinaryOperator::NotEqual => l != r,
                BinaryOperator::LessThan => l < r,
                BinaryOperator::LessThanOrEqual => l <= r,
                BinaryOperator::GreaterThan => l > r,
                BinaryOperator::GreaterThanOrEqual => l >= r,
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "Invalid operator for times".to_string(),
                    ));
                }
            }),
            (Value::DateTime(l), Value::DateTime(r), op) => Ok(match op {
                BinaryOperator::Equal => l == r,
                BinaryOperator::NotEqual => l != r,
                BinaryOperator::LessThan => l < r,
                BinaryOperator::LessThanOrEqual => l <= r,
                BinaryOperator::GreaterThan => l > r,
                BinaryOperator::GreaterThanOrEqual => l >= r,
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "Invalid operator for datetimes".to_string(),
                    ));
                }
            }),
            _ => Err(RustqlError::TypeMismatch(
                "Type mismatch in comparison".to_string(),
            )),
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

pub fn apply_arithmetic(
    left: &Value,
    right: &Value,
    op: &BinaryOperator,
) -> Result<Value, RustqlError> {
    let to_float = |value: &Value| -> Result<f64, RustqlError> {
        match value {
            Value::Integer(i) => Ok(*i as f64),
            Value::Float(f) => Ok(*f),
            Value::Null => Ok(0.0),
            _ => Err(RustqlError::TypeMismatch(
                "Arithmetic in ORDER BY requires numeric values".to_string(),
            )),
        }
    };

    match (left, right) {
        (Value::Integer(l), Value::Integer(r)) => match op {
            BinaryOperator::Plus => Ok(Value::Integer(l + r)),
            BinaryOperator::Minus => Ok(Value::Integer(l - r)),
            BinaryOperator::Multiply => Ok(Value::Integer(l * r)),
            BinaryOperator::Divide => {
                if *r == 0 {
                    Err(RustqlError::DivisionByZero)
                } else if l % r == 0 {
                    Ok(Value::Integer(l / r))
                } else {
                    Ok(Value::Float(*l as f64 / *r as f64))
                }
            }
            _ => unreachable!(),
        },
        _ => {
            let l = to_float(left)?;
            let r = to_float(right)?;
            match op {
                BinaryOperator::Plus => Ok(Value::Float(l + r)),
                BinaryOperator::Minus => Ok(Value::Float(l - r)),
                BinaryOperator::Multiply => Ok(Value::Float(l * r)),
                BinaryOperator::Divide => {
                    if r.abs() < f64::EPSILON {
                        Err(RustqlError::DivisionByZero)
                    } else {
                        Ok(Value::Float(l / r))
                    }
                }
                _ => unreachable!(),
            }
        }
    }
}

pub fn compare_values_for_sort(left: &Value, right: &Value) -> Ordering {
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

pub fn parse_value_from_string(s: &str) -> Value {
    let s = s.trim();
    if s == "NULL" || s.is_empty() {
        return Value::Null;
    }
    if let Ok(i) = s.parse::<i64>() {
        return Value::Integer(i);
    }
    if let Ok(f) = s.parse::<f64>() {
        return Value::Float(f);
    }
    if s == "true" || s == "1" {
        return Value::Boolean(true);
    }
    if s == "false" || s == "0" {
        return Value::Boolean(false);
    }
    if s.starts_with('\'') && s.ends_with('\'') {
        return Value::Text(s[1..s.len() - 1].to_string());
    }
    Value::Text(s.to_string())
}

fn parse_date_components(s: &str) -> Option<(i64, i64, i64)> {
    let date_part = s.split(' ').next()?;
    let parts: Vec<&str> = date_part.split('-').collect();
    if parts.len() != 3 {
        return None;
    }
    let y = parts[0].parse::<i64>().ok()?;
    let m = parts[1].parse::<i64>().ok()?;
    let d = parts[2].parse::<i64>().ok()?;
    Some((y, m, d))
}

fn ymd_to_days(y: i64, m: i64, d: i64) -> i64 {
    let m_adj = if m <= 2 { m + 9 } else { m - 3 };
    let y_adj = if m <= 2 { y - 1 } else { y };
    let era = if y_adj >= 0 {
        y_adj / 400
    } else {
        (y_adj - 399) / 400
    };
    let yoe = y_adj - era * 400;
    let doy = (153 * m_adj + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn days_to_ymd(days: i64) -> (i64, i64, i64) {
    let z = days + 719468;
    let era = if z >= 0 {
        z / 146097
    } else {
        (z - 146096) / 146097
    };
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

fn execute_cast(val: Value, target_type: &DataType) -> Result<Value, RustqlError> {
    if matches!(val, Value::Null) {
        return Ok(Value::Null);
    }
    match target_type {
        DataType::Integer => {
            match &val {
                Value::Integer(_) => Ok(val),
                Value::Float(f) => Ok(Value::Integer(*f as i64)),
                Value::Text(s) => s.trim().parse::<i64>().map(Value::Integer).map_err(|_| {
                    RustqlError::TypeMismatch(format!("Cannot cast '{}' to INTEGER", s))
                }),
                Value::Boolean(b) => Ok(Value::Integer(if *b { 1 } else { 0 })),
                _ => Err(RustqlError::TypeMismatch(format!(
                    "Cannot cast {:?} to INTEGER",
                    val
                ))),
            }
        }
        DataType::Float => {
            match &val {
                Value::Float(_) => Ok(val),
                Value::Integer(i) => Ok(Value::Float(*i as f64)),
                Value::Text(s) => s.trim().parse::<f64>().map(Value::Float).map_err(|_| {
                    RustqlError::TypeMismatch(format!("Cannot cast '{}' to FLOAT", s))
                }),
                _ => Err(RustqlError::TypeMismatch(format!(
                    "Cannot cast {:?} to FLOAT",
                    val
                ))),
            }
        }
        DataType::Text => Ok(Value::Text(format_value(&val))),
        DataType::Boolean => match &val {
            Value::Boolean(_) => Ok(val),
            Value::Integer(i) => Ok(Value::Boolean(*i != 0)),
            Value::Text(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Ok(Value::Boolean(true)),
                "false" | "0" | "no" => Ok(Value::Boolean(false)),
                _ => Err(RustqlError::TypeMismatch(format!(
                    "Cannot cast '{}' to BOOLEAN",
                    s
                ))),
            },
            _ => Err(RustqlError::TypeMismatch(format!(
                "Cannot cast {:?} to BOOLEAN",
                val
            ))),
        },
        DataType::Date => match &val {
            Value::Date(_) => Ok(val),
            Value::DateTime(dt) | Value::Text(dt) => {
                let date_part = dt.split(' ').next().unwrap_or(dt);
                if parse_date_components(date_part).is_some() {
                    Ok(Value::Date(date_part.to_string()))
                } else {
                    Err(RustqlError::TypeMismatch(format!(
                        "Cannot cast '{}' to DATE",
                        dt
                    )))
                }
            }
            _ => Err(RustqlError::TypeMismatch(format!(
                "Cannot cast {:?} to DATE",
                val
            ))),
        },
        DataType::DateTime => match &val {
            Value::DateTime(_) => Ok(val),
            Value::Date(d) => Ok(Value::DateTime(format!("{} 00:00:00", d))),
            Value::Text(s) => {
                if s.contains(' ') {
                    Ok(Value::DateTime(s.clone()))
                } else if parse_date_components(s).is_some() {
                    Ok(Value::DateTime(format!("{} 00:00:00", s)))
                } else {
                    Ok(Value::DateTime(s.clone()))
                }
            }
            _ => Err(RustqlError::TypeMismatch(format!(
                "Cannot cast {:?} to DATETIME",
                val
            ))),
        },
        DataType::Time => match &val {
            Value::Time(_) => Ok(val),
            Value::DateTime(dt) => {
                let time_part = dt.split(' ').nth(1).unwrap_or("00:00:00");
                Ok(Value::Time(time_part.to_string()))
            }
            Value::Text(s) => Ok(Value::Time(s.clone())),
            _ => Err(RustqlError::TypeMismatch(format!(
                "Cannot cast {:?} to TIME",
                val
            ))),
        },
    }
}

pub fn evaluate_select_order_expression(
    expr: &Expression,
    columns: &[ColumnDefinition],
    row: &[Value],
    column_specs: &[(String, Column)],
    projected_row: &[Value],
    allow_ordinal: bool,
) -> Result<Value, RustqlError> {
    match expr {
        Expression::Column(name) => {
            for (idx, (header, col_spec)) in column_specs.iter().enumerate() {
                if header == name {
                    return Ok(projected_row[idx].clone());
                }
                if let Column::Named {
                    name: original_name,
                    alias,
                } = col_spec
                    && (alias.as_ref().map(|a| a == name).unwrap_or(false)
                        || original_name == name
                        || original_name
                            .split('.')
                            .next_back()
                            .map(|n| n == name)
                            .unwrap_or(false))
                {
                    return Ok(projected_row[idx].clone());
                }
            }

            let column_name = if name.contains('.') {
                name.split('.').next_back().unwrap_or(name)
            } else {
                name.as_str()
            };

            let idx = columns
                .iter()
                .position(|c| c.name == column_name)
                .ok_or_else(|| RustqlError::ColumnNotFound(format!("{} (ORDER BY)", name)))?;
            Ok(row[idx].clone())
        }
        Expression::Value(val) => {
            if allow_ordinal
                && let Value::Integer(ord) = val
                && *ord >= 1
                && (*ord as usize) <= projected_row.len()
            {
                return Ok(projected_row[*ord as usize - 1].clone());
            }
            Ok(val.clone())
        }
        Expression::BinaryOp { left, op, right } => match op {
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide => {
                let left_val = evaluate_select_order_expression(
                    left,
                    columns,
                    row,
                    column_specs,
                    projected_row,
                    false,
                )?;
                let right_val = evaluate_select_order_expression(
                    right,
                    columns,
                    row,
                    column_specs,
                    projected_row,
                    false,
                )?;
                apply_arithmetic(&left_val, &right_val, op)
            }
            _ => Err(RustqlError::Internal(
                "Unsupported operator in ORDER BY".to_string(),
            )),
        },
        _ => Err(RustqlError::Internal(
            "Unsupported expression in ORDER BY".to_string(),
        )),
    }
}
