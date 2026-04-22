use super::*;

pub(super) fn execute_cast(val: Value, target_type: &DataType) -> Result<Value, RustqlError> {
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
            Value::Date(_) => Ok(val),
            Value::DateTime(dt) | Value::Text(dt) => {
                let date_part = dt.split(' ').next().unwrap_or(dt);
                if super::date::parse_date_components(date_part).is_some() {
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
                } else if super::date::parse_date_components(s).is_some() {
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
