use super::*;

pub fn evaluate_select_order_expression(
    expr: &Expression,
    row: &[Value],
    projected_row: &[Value],
    projected_lookup: &HashMap<String, usize>,
    base_lookup: &HashMap<String, usize>,
    allow_ordinal: bool,
) -> Result<Value, RustqlError> {
    match expr {
        Expression::Column(name) => {
            if let Some(&idx) = projected_lookup.get(name) {
                return Ok(projected_row[idx].clone());
            }

            let column_name = if name.contains('.') {
                name.split('.').next_back().unwrap_or(name)
            } else {
                name.as_str()
            };

            let idx = base_lookup
                .get(column_name)
                .copied()
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
                    row,
                    projected_row,
                    projected_lookup,
                    base_lookup,
                    false,
                )?;
                let right_val = evaluate_select_order_expression(
                    right,
                    row,
                    projected_row,
                    projected_lookup,
                    base_lookup,
                    false,
                )?;
                super::compare::apply_arithmetic(&left_val, &right_val, op)
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
