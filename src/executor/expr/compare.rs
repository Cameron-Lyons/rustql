use super::*;

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
                BinaryOperator::LessThan => l < r,
                BinaryOperator::LessThanOrEqual => l <= r,
                BinaryOperator::GreaterThan => l > r,
                BinaryOperator::GreaterThanOrEqual => l >= r,
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "Invalid operator for strings".to_string(),
                    ));
                }
            }),
            (Value::Boolean(l), Value::Boolean(r), op) => Ok(match op {
                BinaryOperator::Equal => l == r,
                BinaryOperator::NotEqual => l != r,
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "Invalid operator for booleans".to_string(),
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
    value.to_string()
}

pub fn apply_arithmetic(
    left: &Value,
    right: &Value,
    op: &BinaryOperator,
) -> Result<Value, RustqlError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        return Ok(Value::Null);
    }

    let to_float = |value: &Value| -> Result<f64, RustqlError> {
        match value {
            Value::Integer(i) => Ok(*i as f64),
            Value::Float(f) => Ok(*f),
            _ => Err(RustqlError::TypeMismatch(
                "Arithmetic requires numeric values".to_string(),
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
            _ => Err(RustqlError::Internal(
                "Invalid arithmetic operator".to_string(),
            )),
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
                _ => Err(RustqlError::Internal(
                    "Invalid arithmetic operator".to_string(),
                )),
            }
        }
    }
}

pub fn compare_values_same_type(left: &Value, right: &Value) -> Ordering {
    compare_values_for_sort(left, right)
}

pub fn compare_values_for_sort(left: &Value, right: &Value) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Greater,
        (_, Value::Null) => Ordering::Less,
        (Value::Integer(l), Value::Integer(r)) => l.cmp(r),
        (Value::Float(l), Value::Float(r)) => compare_floats_for_sort(*l, *r),
        (Value::Integer(l), Value::Float(r)) => compare_floats_for_sort(*l as f64, *r),
        (Value::Float(l), Value::Integer(r)) => compare_floats_for_sort(*l, *r as f64),
        (Value::Text(l), Value::Text(r)) => l.cmp(r),
        (Value::Boolean(l), Value::Boolean(r)) => l.cmp(r),
        (Value::Date(l), Value::Date(r)) => l.cmp(r),
        (Value::Time(l), Value::Time(r)) => l.cmp(r),
        (Value::DateTime(l), Value::DateTime(r)) => l.cmp(r),
        _ => sort_rank(left).cmp(&sort_rank(right)),
    }
}

fn sort_rank(value: &Value) -> u8 {
    match value {
        Value::Integer(_) | Value::Float(_) => 0,
        Value::Text(_) => 1,
        Value::Boolean(_) => 2,
        Value::Date(_) => 3,
        Value::Time(_) => 4,
        Value::DateTime(_) => 5,
        Value::Null => 6,
    }
}

fn compare_floats_for_sort(left: f64, right: f64) -> Ordering {
    if left == right || (left.is_nan() && right.is_nan()) {
        return Ordering::Equal;
    }

    match (left.is_nan(), right.is_nan()) {
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        _ if left < right => Ordering::Less,
        _ => Ordering::Greater,
    }
}
