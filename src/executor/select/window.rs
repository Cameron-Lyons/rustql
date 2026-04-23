use super::*;

pub(crate) fn resolve_window_definitions(stmt: &mut SelectStatement) {
    if stmt.window_definitions.is_empty() {
        return;
    }
    let defs = stmt.window_definitions.clone();
    for col in &mut stmt.columns {
        if let Column::Expression {
            expr:
                Expression::WindowFunction {
                    partition_by,
                    order_by,
                    frame,
                    ..
                },
            ..
        } = col
            && partition_by.len() == 1
            && let Expression::Column(ref name) = partition_by[0]
            && let Some(ref_name) = name.strip_prefix("__window_ref:")
            && let Some(def) = defs.iter().find(|d| d.name == ref_name)
        {
            *partition_by = def.partition_by.clone();
            *order_by = def.order_by.clone();
            *frame = def.frame.clone();
        }
    }
}
