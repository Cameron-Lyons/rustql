use super::*;

pub(super) fn execute_cast(val: Value, target_type: &DataType) -> Result<Value, RustqlError> {
    coerce_value_for_type(val, target_type)
}

pub(crate) fn coerce_value_for_type(
    val: Value,
    target_type: &DataType,
) -> Result<Value, RustqlError> {
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
        DataType::Text => Ok(Value::Text(super::compare::format_value(&val))),
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
            Value::Date(d) | Value::Text(d) => super::date::normalize_date(d)
                .map(Value::Date)
                .or_else(|| super::date::datetime_date_part(d).map(Value::Date))
                .ok_or_else(|| temporal_cast_error(d, "DATE", super::date::CANONICAL_DATE_FORMAT)),
            Value::DateTime(dt) => super::date::datetime_date_part(dt)
                .map(Value::Date)
                .ok_or_else(|| {
                    temporal_cast_error(dt, "DATE", super::date::CANONICAL_DATETIME_FORMAT)
                }),
            _ => Err(RustqlError::TypeMismatch(format!(
                "Cannot cast {:?} to DATE",
                val
            ))),
        },
        DataType::DateTime => match &val {
            Value::DateTime(dt) | Value::Text(dt) => super::date::normalize_datetime(dt)
                .map(Value::DateTime)
                .ok_or_else(|| {
                    temporal_cast_error(dt, "DATETIME", super::date::CANONICAL_DATETIME_FORMAT)
                }),
            Value::Date(d) => super::date::normalize_date(d)
                .map(|date| Value::DateTime(format!("{} 00:00:00", date)))
                .ok_or_else(|| {
                    temporal_cast_error(d, "DATETIME", super::date::CANONICAL_DATE_FORMAT)
                }),
            _ => Err(RustqlError::TypeMismatch(format!(
                "Cannot cast {:?} to DATETIME",
                val
            ))),
        },
        DataType::Time => match &val {
            Value::Time(t) | Value::Text(t) => super::date::normalize_time(t)
                .map(Value::Time)
                .ok_or_else(|| temporal_cast_error(t, "TIME", super::date::CANONICAL_TIME_FORMAT)),
            Value::DateTime(dt) => super::date::datetime_time_part(dt)
                .map(Value::Time)
                .ok_or_else(|| {
                    temporal_cast_error(dt, "TIME", super::date::CANONICAL_DATETIME_FORMAT)
                }),
            _ => Err(RustqlError::TypeMismatch(format!(
                "Cannot cast {:?} to TIME",
                val
            ))),
        },
    }
}

fn temporal_cast_error(value: &str, target: &str, expected_format: &str) -> RustqlError {
    RustqlError::TypeMismatch(format!(
        "Cannot cast '{}' to {}; expected {}",
        value, target, expected_format
    ))
}
