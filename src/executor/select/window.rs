use super::*;

pub(crate) fn resolve_window_definitions(stmt: &mut SelectStatement) {
    if stmt.window_definitions.is_empty() {
        return;
    }
    let defs = &stmt.window_definitions;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn select_with_windows(
        columns: Vec<Column>,
        window_definitions: Vec<WindowDefinition>,
    ) -> SelectStatement {
        SelectStatement {
            ctes: Vec::new(),
            distinct: false,
            distinct_on: None,
            columns,
            from: "sales".to_string(),
            from_alias: None,
            from_subquery: None,
            from_function: None,
            joins: Vec::new(),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            fetch: None,
            set_op: None,
            window_definitions,
            from_values: None,
        }
    }

    fn window_ref(name: &str) -> Column {
        Column::Expression {
            expr: Expression::WindowFunction {
                function: WindowFunctionType::RowNumber,
                args: Vec::new(),
                partition_by: vec![Expression::Column(format!("__window_ref:{}", name))],
                order_by: Vec::new(),
                frame: None,
            },
            alias: Some("rn".to_string()),
        }
    }

    #[test]
    fn resolve_window_definitions_copies_named_definition_parts() {
        let ignored_definition = WindowDefinition {
            name: "ignored".to_string(),
            partition_by: vec![Expression::Column("unused".to_string())],
            order_by: Vec::new(),
            frame: None,
        };
        let regional_definition = WindowDefinition {
            name: "regional".to_string(),
            partition_by: vec![Expression::Column("region".to_string())],
            order_by: vec![OrderByExpr {
                expr: Expression::Column("amount".to_string()),
                asc: false,
            }],
            frame: Some(WindowFrame {
                mode: WindowFrameMode::Rows,
                start: WindowFrameBound::UnboundedPreceding,
                end: WindowFrameBound::CurrentRow,
            }),
        };
        let mut stmt = select_with_windows(
            vec![window_ref("regional")],
            vec![ignored_definition, regional_definition.clone()],
        );

        resolve_window_definitions(&mut stmt);

        let Column::Expression {
            expr:
                Expression::WindowFunction {
                    partition_by,
                    order_by,
                    frame,
                    ..
                },
            ..
        } = &stmt.columns[0]
        else {
            panic!("expected a window expression");
        };
        assert_eq!(partition_by, &regional_definition.partition_by);
        assert_eq!(order_by, &regional_definition.order_by);
        assert_eq!(frame, &regional_definition.frame);
        assert_eq!(stmt.window_definitions[1], regional_definition);
    }
}
