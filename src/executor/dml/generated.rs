use super::*;

pub(super) fn coerce_row_to_column_types(
    columns: &[ColumnDefinition],
    row: &mut [Value],
) -> Result<(), RustqlError> {
    for (col_idx, col_def) in columns.iter().enumerate() {
        if col_idx >= row.len() {
            continue;
        }
        if value_already_matches_type(&row[col_idx], &col_def.data_type) {
            continue;
        }
        row[col_idx] = coerce_value_for_type(row[col_idx].clone(), &col_def.data_type)?;
    }
    Ok(())
}

fn value_already_matches_type(value: &Value, data_type: &DataType) -> bool {
    match (value, data_type) {
        (Value::Null, _) => true,
        (Value::Integer(_), DataType::Integer)
        | (Value::Text(_), DataType::Text)
        | (Value::Boolean(_), DataType::Boolean) => true,
        (Value::Float(value), DataType::Float) => value.is_finite(),
        _ => false,
    }
}

pub(super) fn parse_wrapped_select(sql: &str) -> Result<SelectStatement, RustqlError> {
    let tokens = crate::lexer::tokenize(sql)?;
    match crate::parser::parse(tokens)? {
        Statement::Select(select_stmt) => Ok(select_stmt),
        _ => Err(RustqlError::Internal(
            "Wrapped expression did not parse as SELECT".to_string(),
        )),
    }
}

fn evaluate_generated_value(
    select_stmt: &SelectStatement,
    columns: &[ColumnDefinition],
    row: &[Value],
) -> Result<Value, RustqlError> {
    match select_stmt.columns.first() {
        Some(Column::Expression { expr, .. }) => evaluate_value_expression(expr, columns, row),
        Some(Column::Named { name, .. }) => {
            let src_idx = columns
                .iter()
                .position(|column| column.name == *name)
                .ok_or_else(|| RustqlError::ColumnNotFound(name.clone()))?;
            row.get(src_idx).cloned().ok_or_else(|| {
                RustqlError::Internal(format!(
                    "Generated column source '{}' is outside the row",
                    name
                ))
            })
        }
        Some(_) => Err(RustqlError::TypeMismatch(
            "Generated column expression must be scalar".to_string(),
        )),
        None => Err(RustqlError::Internal(
            "Generated column expression produced no output".to_string(),
        )),
    }
}

pub(super) fn evaluate_generated_columns(
    columns: &[ColumnDefinition],
    row: &mut [Value],
    insert_columns: &Option<Vec<String>>,
) -> Result<(), RustqlError> {
    for (col_idx, col_def) in columns.iter().enumerate() {
        if let Some(ref generated) = col_def.generated {
            if generated.always
                && let Some(specified_cols) = insert_columns
                && specified_cols.iter().any(|c| c == &col_def.name)
            {
                return Err(RustqlError::Internal(format!(
                    "Cannot insert into generated column '{}'",
                    col_def.name
                )));
            }

            let wrapped = format!("SELECT {} FROM _dummy", generated.expr_sql);
            let select_stmt = parse_wrapped_select(&wrapped)?;
            if col_idx < row.len() {
                let value = evaluate_generated_value(&select_stmt, columns, row)?;
                row[col_idx] = value;
            }
        }
    }
    Ok(())
}

pub(super) fn evaluate_generated_columns_update(
    columns: &[ColumnDefinition],
    row: &mut [Value],
) -> Result<(), RustqlError> {
    for (col_idx, col_def) in columns.iter().enumerate() {
        if let Some(ref generated) = col_def.generated {
            let wrapped = format!("SELECT {} FROM _dummy", generated.expr_sql);
            let select_stmt = parse_wrapped_select(&wrapped)?;
            if col_idx < row.len() {
                let value = evaluate_generated_value(&select_stmt, columns, row)?;
                row[col_idx] = value;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matching_values_skip_coercion() {
        assert!(value_already_matches_type(
            &Value::Integer(7),
            &DataType::Integer
        ));
        assert!(value_already_matches_type(
            &Value::Text("ready".to_string()),
            &DataType::Text
        ));
        assert!(value_already_matches_type(
            &Value::Boolean(true),
            &DataType::Boolean
        ));
        assert!(value_already_matches_type(
            &Value::Float(1.25),
            &DataType::Float
        ));
        assert!(value_already_matches_type(&Value::Null, &DataType::Date));
    }

    #[test]
    fn values_requiring_validation_or_conversion_do_not_skip_coercion() {
        assert!(!value_already_matches_type(
            &Value::Text("7".to_string()),
            &DataType::Integer
        ));
        assert!(!value_already_matches_type(
            &Value::Float(f64::NAN),
            &DataType::Float
        ));
        assert!(!value_already_matches_type(
            &Value::Date("2024-01-01".to_string()),
            &DataType::Date
        ));
    }
}
