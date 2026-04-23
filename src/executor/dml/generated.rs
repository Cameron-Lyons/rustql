use super::*;

pub(super) fn coerce_row_to_column_types(
    columns: &[ColumnDefinition],
    row: &mut [Value],
) -> Result<(), RustqlError> {
    for (col_idx, col_def) in columns.iter().enumerate() {
        if col_idx >= row.len() {
            continue;
        }
        row[col_idx] = coerce_value_for_type(row[col_idx].clone(), &col_def.data_type)?;
    }
    Ok(())
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
