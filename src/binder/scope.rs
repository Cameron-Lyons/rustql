use super::types::{column_definition, constant_expression_type, syntactic_column_type};
use super::*;

impl<'a> Binder<'a> {
    pub(super) fn bind_select_sources(
        &mut self,
        stmt: &mut SelectStatement,
    ) -> Result<Vec<BoundColumnRef>, RustqlError> {
        let mut columns = Vec::new();

        if let Some((subquery, alias)) = stmt.from_subquery.as_mut() {
            let bound = self.bind_child_select(subquery, Vec::new())?;
            **subquery = bound.statement.clone();
            columns = self.columns_for_relation(&bound.output_columns, alias, true);
        } else if let Some((rows, alias, column_aliases)) = stmt.from_values.as_ref() {
            columns = self.bind_values_source(rows, alias, column_aliases)?;
        } else if let Some(function) = stmt.from_function.as_mut() {
            let source = self.bind_table_function(function)?;
            columns = source;
        } else if !stmt.from.is_empty() {
            let label = stmt.from_alias.as_deref().unwrap_or(&stmt.from);
            columns = self.source_columns_for_name(&stmt.from, label)?;
        }

        for join in &mut stmt.joins {
            let right_columns = if let Some((subquery, alias)) = join.subquery.as_mut() {
                let outer = if join.lateral {
                    self.outer_for_child(&columns)
                } else {
                    Vec::new()
                };
                let bound = self.bind_child_select(subquery, outer)?;
                **subquery = bound.statement.clone();
                self.columns_for_relation(&bound.output_columns, alias, true)
            } else {
                let label = join.table_alias.as_deref().unwrap_or(&join.table);
                self.source_columns_for_name(&join.table, label)?
            };

            self.validate_join_using(join, &columns, &right_columns)?;

            let mut combined = columns.clone();
            combined.extend(right_columns.clone());
            let join_scope = NameScope { columns: combined };
            if let Some(on_expr) = join.on.as_ref() {
                let bound = self.bind_predicate_expr(on_expr, &join_scope, "JOIN ON clause")?;
                join.on = Some(bound.expr);
            }

            columns.extend(right_columns);
        }

        Ok(columns)
    }

    pub(super) fn resolve_column(
        &self,
        name: &str,
        scope: &NameScope,
    ) -> Result<BoundColumnRef, RustqlError> {
        let local_matches = matching_columns(name, &scope.columns);
        match local_matches.len() {
            1 => return Ok(local_matches[0].clone()),
            n if n > 1 => return Err(RustqlError::AmbiguousColumn(name.to_string())),
            _ => {}
        }

        let outer_matches = matching_columns(name, &self.outer_columns);
        match outer_matches.len() {
            1 => {
                let mut column = outer_matches[0].clone();
                column.outer = true;
                Ok(column)
            }
            n if n > 1 => Err(RustqlError::AmbiguousColumn(name.to_string())),
            _ => Err(RustqlError::ColumnNotFound(name.to_string())),
        }
    }

    pub(super) fn source_columns_for_name(
        &mut self,
        source_name: &str,
        relation_label: &str,
    ) -> Result<Vec<BoundColumnRef>, RustqlError> {
        if let Some(cte) = self
            .ctes
            .iter()
            .rev()
            .find(|cte| cte.name == source_name)
            .cloned()
        {
            let columns = self.cte_output_columns(&cte)?;
            return Ok(self.columns_for_relation(&columns, relation_label, true));
        }

        if let Some(table) = self.db.get_table(source_name) {
            return Ok(table
                .columns
                .iter()
                .map(|column| {
                    let column = self.semantic_column_definition(column);
                    bound_column(relation_label, &column, false)
                })
                .collect());
        }

        if let Some(view) = self.db.get_view(source_name) {
            let view_select = self.parse_view_query(&view.query_sql)?;
            let bound = self.bind_child_select(&view_select, Vec::new())?;
            return Ok(self.columns_for_relation(&bound.output_columns, relation_label, true));
        }

        Err(RustqlError::TableNotFound(source_name.to_string()))
    }

    fn cte_output_columns(&mut self, cte: &Cte) -> Result<Vec<ColumnDefinition>, RustqlError> {
        if self.binding_ctes.iter().any(|name| name == &cte.name) {
            return Ok(self.syntactic_output_columns(&cte.query));
        }

        let mut child = Binder {
            db: self.db,
            ctes: self.ctes.clone(),
            outer_columns: Vec::new(),
            binding_ctes: self.binding_ctes.clone(),
        };
        child.binding_ctes.push(cte.name.clone());
        child
            .bind_select(&cte.query)
            .map(|bound| bound.output_columns)
    }

    fn syntactic_output_columns(&self, stmt: &SelectStatement) -> Vec<ColumnDefinition> {
        let mut left = stmt.clone();
        if stmt.set_op.is_some() {
            left.set_op = None;
            return self.syntactic_output_columns(&left);
        }

        if matches!(left.columns.first(), Some(Column::All)) {
            return Vec::new();
        }

        left.columns
            .iter()
            .map(|column| {
                let name = match column {
                    Column::Named { name, alias } => alias.clone().unwrap_or_else(|| name.clone()),
                    Column::Expression { alias, .. } => {
                        alias.clone().unwrap_or_else(|| "<expression>".to_string())
                    }
                    Column::Function(aggregate) => format_aggregate_header(aggregate),
                    Column::Subquery(_) => "<subquery>".to_string(),
                    Column::All => "*".to_string(),
                };
                column_definition(name, syntactic_column_type(column), true)
            })
            .collect()
    }

    pub(super) fn columns_for_relation(
        &self,
        columns: &[ColumnDefinition],
        relation_label: &str,
        use_unqualified_output_names: bool,
    ) -> Vec<BoundColumnRef> {
        columns
            .iter()
            .map(|column| {
                let mut column = column.clone();
                if use_unqualified_output_names {
                    column.name = unqualified_column_name(&column.name).to_string();
                }
                bound_column(relation_label, &column, false)
            })
            .collect()
    }

    fn semantic_column_definition(&self, column: &ColumnDefinition) -> ColumnDefinition {
        let Some((relation, name)) = column.name.split_once('.') else {
            return column.clone();
        };
        let Some(table) = self.db.get_table(relation) else {
            return column.clone();
        };
        let Some(source_column) = table
            .columns
            .iter()
            .find(|candidate| candidate.name == name)
        else {
            return column.clone();
        };

        let mut semantic = column.clone();
        semantic.data_type = source_column.data_type.clone();
        semantic.nullable = source_column.nullable;
        semantic
    }

    fn bind_values_source(
        &self,
        rows: &[Vec<Expression>],
        alias: &str,
        column_aliases: &[String],
    ) -> Result<Vec<BoundColumnRef>, RustqlError> {
        let width = rows.first().map_or(0, Vec::len);
        for row in rows {
            if row.len() != width {
                return Err(RustqlError::TypeMismatch(format!(
                    "VALUES rows must all have the same number of columns: expected {}, got {}",
                    width,
                    row.len()
                )));
            }
        }
        Ok((0..width)
            .map(|idx| {
                let data_type = rows
                    .iter()
                    .filter_map(|row| row.get(idx))
                    .filter_map(constant_expression_type)
                    .next()
                    .unwrap_or(DataType::Text);
                let column = column_definition(
                    column_aliases
                        .get(idx)
                        .cloned()
                        .unwrap_or_else(|| format!("column{}", idx + 1)),
                    data_type,
                    true,
                );
                bound_column(alias, &column, false)
            })
            .collect())
    }

    fn bind_table_function(
        &mut self,
        function: &mut TableFunction,
    ) -> Result<Vec<BoundColumnRef>, RustqlError> {
        let empty_scope = NameScope::default();
        for arg in &function.args {
            self.bind_expr(arg, &empty_scope)?;
        }

        match function.name.as_str() {
            "generate_series" => {
                let relation_label = function.alias.as_deref().unwrap_or(&function.name);
                let column = column_definition(
                    function
                        .alias
                        .clone()
                        .unwrap_or_else(|| "generate_series".to_string()),
                    DataType::Integer,
                    false,
                );
                Ok(vec![bound_column(relation_label, &column, false)])
            }
            other => Err(RustqlError::TypeMismatch(format!(
                "Unsupported table function '{}'",
                other
            ))),
        }
    }

    fn validate_join_using(
        &self,
        join: &Join,
        left_columns: &[BoundColumnRef],
        right_columns: &[BoundColumnRef],
    ) -> Result<(), RustqlError> {
        if let Some(using_columns) = join.using_columns.as_ref() {
            for column in using_columns {
                if !left_columns.iter().any(|candidate| candidate.name == *column)
                    || !right_columns.iter().any(|candidate| candidate.name == *column)
                {
                    return Err(RustqlError::ColumnNotFound(column.clone()));
                }
            }
        }
        Ok(())
    }

    pub(super) fn star_output_columns(
        &self,
        stmt: &SelectStatement,
        scope: &NameScope,
    ) -> Vec<ColumnDefinition> {
        let qualify = !stmt.joins.is_empty();
        scope
            .columns
            .iter()
            .map(|column| {
                column_definition(
                    if qualify {
                        column.qualified_name.clone()
                    } else {
                        column.name.clone()
                    },
                    column.data_type.clone(),
                    column.nullable,
                )
            })
            .collect()
    }

    pub(super) fn bind_child_select(
        &self,
        stmt: &SelectStatement,
        outer_columns: Vec<BoundColumnRef>,
    ) -> Result<BoundSelectStatement, RustqlError> {
        let mut child = self.child_binder(outer_columns);
        child.bind_select(stmt)
    }

    pub(super) fn bind_correlated_select(
        &self,
        stmt: &SelectStatement,
        scope: &NameScope,
    ) -> Result<BoundSelectStatement, RustqlError> {
        self.bind_child_select(stmt, self.outer_for_child(&scope.columns))
    }

    fn child_binder(&self, outer_columns: Vec<BoundColumnRef>) -> Binder<'a> {
        Binder {
            db: self.db,
            ctes: self.ctes.clone(),
            outer_columns,
            binding_ctes: self.binding_ctes.clone(),
        }
    }

    fn outer_for_child(&self, local_columns: &[BoundColumnRef]) -> Vec<BoundColumnRef> {
        let mut outer = local_columns.to_vec();
        outer.extend(self.outer_columns.clone());
        outer
    }

    fn parse_view_query(&self, query_sql: &str) -> Result<SelectStatement, RustqlError> {
        let tokens = crate::lexer::tokenize(query_sql)?;
        match crate::parser::parse(tokens)? {
            Statement::Select(select) => Ok(select),
            _ => Err(RustqlError::TypeMismatch(
                "View definition is not a SELECT statement".to_string(),
            )),
        }
    }
}

pub(super) fn ensure_single_column_subquery(
    subquery: &BoundSelectStatement,
    context: &str,
) -> Result<(), RustqlError> {
    if subquery.output_columns.len() != 1 {
        return Err(RustqlError::TypeMismatch(format!(
            "{} must return exactly one column",
            context
        )));
    }
    Ok(())
}

pub(super) fn bound_column(
    relation: &str,
    column: &ColumnDefinition,
    outer: bool,
) -> BoundColumnRef {
    BoundColumnRef {
        relation: Some(relation.to_string()),
        name: column.name.clone(),
        qualified_name: format!("{}.{}", relation, column.name),
        data_type: column.data_type.clone(),
        nullable: column.nullable,
        outer,
    }
}

fn matching_columns(name: &str, columns: &[BoundColumnRef]) -> Vec<BoundColumnRef> {
    if let Some((relation, column)) = name.split_once('.') {
        columns
            .iter()
            .filter(|candidate| {
                candidate.name == name
                    || (candidate.relation.as_deref() == Some(relation)
                        && (candidate.name == column
                            || unqualified_column_name(&candidate.name) == column))
            })
            .cloned()
            .collect()
    } else {
        columns
            .iter()
            .filter(|candidate| {
                candidate.name == name || unqualified_column_name(&candidate.name) == name
            })
            .cloned()
            .collect()
    }
}

fn unqualified_column_name(name: &str) -> &str {
    name.split('.').next_back().unwrap_or(name)
}
