use super::*;

const MAX_RECURSIVE_CTE_ITERATIONS: usize = 1000;

pub(super) fn join_requires_source_materialization(join: &Join) -> bool {
    !join.lateral && join.subquery.is_some()
}

#[derive(Default)]
pub(super) struct TempTableScope {
    originals: HashMap<String, Option<Table>>,
}

impl TempTableScope {
    pub(super) fn insert_or_replace(&mut self, db: &mut Database, name: String, table: Table) {
        let previous = db.tables.insert(name.clone(), table);
        self.originals.entry(name).or_insert(previous);
    }

    pub(super) fn cleanup(self, db: &mut Database) {
        for (name, original) in self.originals {
            match original {
                Some(table) => {
                    db.tables.insert(name, table);
                }
                None => {
                    db.tables.remove(&name);
                }
            }
        }
    }
}

pub(super) fn select_requires_source_materialization(
    db: &dyn DatabaseCatalog,
    stmt: &SelectStatement,
) -> bool {
    stmt.from_subquery.is_some()
        || stmt.joins.iter().any(join_requires_source_materialization)
        || !stmt.ctes.is_empty()
        || (!stmt.from.is_empty() && !db.contains_table(&stmt.from) && db.contains_view(&stmt.from))
}

pub(super) fn rewrite_select_sources_for_scope(
    context: Option<&ExecutionContext>,
    mut stmt: SelectStatement,
    db: &mut Database,
    temp_scope: &mut TempTableScope,
) -> Result<SelectStatement, RustqlError> {
    if !stmt.ctes.is_empty() {
        materialize_ctes_for_scope(context, &mut stmt, db, temp_scope)?;
    }

    if let Some((subquery, alias)) = stmt.from_subquery.clone() {
        let subquery_result = execute_select_in_db(context, *subquery, db)?;
        temp_scope.insert_or_replace(
            db,
            alias.clone(),
            table_from_select_result(&subquery_result),
        );
        stmt.from = alias;
        stmt.from_subquery = None;
    }

    if let Some((values_rows, alias, col_aliases)) = stmt.from_values.clone() {
        let table = table_from_values_rows(&values_rows, &col_aliases)?;
        temp_scope.insert_or_replace(db, alias.clone(), table);
        stmt.from = alias;
        stmt.from_values = None;
    }

    for join in &mut stmt.joins {
        if join_requires_source_materialization(join)
            && let Some((subquery, alias)) = join.subquery.clone()
        {
            let subquery_result = execute_select_in_db(context, *subquery, db)?;
            temp_scope.insert_or_replace(
                db,
                alias.clone(),
                table_from_select_result(&subquery_result),
            );
            join.table = alias;
            join.subquery = None;
        }
    }

    if !stmt.from.is_empty()
        && !db.contains_table(&stmt.from)
        && let Some(view) = db.get_view(&stmt.from).cloned()
    {
        let view_result = execute_view_select_in_db(context, &view.query_sql, db)?;
        temp_scope.insert_or_replace(
            db,
            view.name.clone(),
            table_from_select_result(&view_result),
        );
    }

    Ok(stmt)
}

pub(super) fn execute_view_select_in_db(
    context: Option<&ExecutionContext>,
    view_sql: &str,
    db: &mut Database,
) -> Result<SelectResult, RustqlError> {
    let tokens = crate::lexer::tokenize(view_sql)?;
    let view_stmt = crate::parser::parse(tokens)?;
    let view_select = match view_stmt {
        crate::ast::Statement::Select(s) => s,
        _ => {
            return Err(RustqlError::Internal(
                "View definition is not a SELECT statement".to_string(),
            ));
        }
    };
    execute_select_in_db(context, view_select, db)
}

pub(super) fn materialize_ctes_for_scope(
    context: Option<&ExecutionContext>,
    stmt: &mut SelectStatement,
    db: &mut Database,
    temp_scope: &mut TempTableScope,
) -> Result<(), RustqlError> {
    let ctes = std::mem::take(&mut stmt.ctes);

    for cte in ctes {
        if cte.recursive {
            materialize_recursive_cte_for_scope(context, &cte, db, temp_scope)?;
        } else {
            let cte_result = execute_select_in_db(context, cte.query.clone(), db)?;
            temp_scope.insert_or_replace(
                db,
                cte.name.clone(),
                table_from_select_result(&cte_result),
            );
        }
    }

    Ok(())
}

pub(super) fn materialize_recursive_cte_for_scope(
    context: Option<&ExecutionContext>,
    cte: &Cte,
    db: &mut Database,
    temp_scope: &mut TempTableScope,
) -> Result<(), RustqlError> {
    let cte_query = &cte.query;
    if let Some((ref set_op_type, ref recursive_part)) = cte_query.set_op {
        if !matches!(set_op_type, SetOperation::UnionAll | SetOperation::Union) {
            return Err(RustqlError::Internal(
                "Recursive CTE requires UNION or UNION ALL".to_string(),
            ));
        }
        let is_union_all = matches!(set_op_type, SetOperation::UnionAll);

        let mut base_stmt = cte_query.clone();
        base_stmt.set_op = None;
        let base_result = execute_select_in_db(context, base_stmt, db)?;
        let columns = column_definitions_from_result(&base_result.headers, &base_result.rows);

        let mut all_rows: Vec<Vec<Value>> = base_result.rows.clone();
        let mut working_rows: Vec<Vec<Value>> = base_result.rows;
        let mut seen: Option<std::collections::BTreeSet<Vec<Value>>> = if is_union_all {
            None
        } else {
            let mut set = std::collections::BTreeSet::new();
            for row in &all_rows {
                set.insert(row.clone());
            }
            Some(set)
        };

        let mut converged = false;
        for _ in 0..MAX_RECURSIVE_CTE_ITERATIONS {
            if working_rows.is_empty() {
                converged = true;
                break;
            }

            temp_scope.insert_or_replace(
                db,
                cte.name.clone(),
                Table::new(columns.clone(), working_rows, vec![]),
            );

            let recursive_result = execute_select_in_db(context, *recursive_part.clone(), db)?;

            let mut new_rows: Vec<Vec<Value>> = Vec::new();
            for row in recursive_result.rows {
                if let Some(ref mut seen_set) = seen {
                    if seen_set.insert(row.clone()) {
                        new_rows.push(row);
                    }
                } else {
                    new_rows.push(row);
                }
            }

            if new_rows.is_empty() {
                converged = true;
                break;
            }

            all_rows.extend(new_rows.clone());
            working_rows = new_rows;
        }

        if !converged {
            return Err(RustqlError::Internal(format!(
                "Recursive CTE '{}' exceeded the iteration limit of {}",
                cte.name, MAX_RECURSIVE_CTE_ITERATIONS
            )));
        }

        temp_scope.insert_or_replace(db, cte.name.clone(), Table::new(columns, all_rows, vec![]));
    } else {
        let cte_result = execute_select_in_db(context, cte.query.clone(), db)?;
        temp_scope.insert_or_replace(db, cte.name.clone(), table_from_select_result(&cte_result));
    }

    Ok(())
}

pub(super) fn column_definitions_from_result(
    headers: &[String],
    rows: &[Vec<Value>],
) -> Vec<ColumnDefinition> {
    headers
        .iter()
        .enumerate()
        .map(|(idx, name)| ColumnDefinition {
            name: name.clone(),
            data_type: rows
                .first()
                .and_then(|row| row.get(idx))
                .map(value_data_type)
                .unwrap_or(DataType::Text),
            nullable: true,
            primary_key: false,
            unique: false,
            default_value: None,
            foreign_key: None,
            check: None,
            auto_increment: false,
            generated: None,
        })
        .collect()
}

pub(super) fn table_from_select_result(result: &SelectResult) -> Table {
    Table::new(
        column_definitions_from_result(&result.headers, &result.rows),
        result.rows.clone(),
        vec![],
    )
}

pub(super) fn table_from_values_rows(
    values_rows: &[Vec<Expression>],
    col_aliases: &[String],
) -> Result<Table, RustqlError> {
    let empty_columns: Vec<ColumnDefinition> = Vec::new();
    let empty_row: Vec<Value> = Vec::new();

    let mut evaluated_rows: Vec<Vec<Value>> = Vec::new();
    for expr_row in values_rows {
        let mut row: Vec<Value> = Vec::new();
        for expr in expr_row {
            row.push(evaluate_value_expression(expr, &empty_columns, &empty_row)?);
        }
        evaluated_rows.push(row);
    }

    let num_cols = evaluated_rows.first().map(|row| row.len()).unwrap_or(0);
    let headers: Vec<String> = (0..num_cols)
        .map(|idx| {
            col_aliases
                .get(idx)
                .cloned()
                .unwrap_or_else(|| format!("column{}", idx + 1))
        })
        .collect();

    Ok(Table::new(
        column_definitions_from_result(&headers, &evaluated_rows),
        evaluated_rows,
        vec![],
    ))
}

pub(super) fn value_data_type(value: &Value) -> DataType {
    match value {
        Value::Integer(_) => DataType::Integer,
        Value::Float(_) => DataType::Float,
        Value::Boolean(_) => DataType::Boolean,
        Value::Date(_) => DataType::Date,
        Value::Time(_) => DataType::Time,
        Value::DateTime(_) => DataType::DateTime,
        Value::Null | Value::Text(_) => DataType::Text,
    }
}
