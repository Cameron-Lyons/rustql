use super::*;

impl<'a> QueryPlanner<'a> {
    pub(super) fn aggregate_output_group_by(&self, group_by: &GroupByClause) -> Vec<Expression> {
        match group_by {
            GroupByClause::Simple(exprs)
            | GroupByClause::Rollup(exprs)
            | GroupByClause::Cube(exprs) => exprs.clone(),
            GroupByClause::GroupingSets(sets) => {
                let mut output = Vec::new();
                for set in sets {
                    for expr in set {
                        if !output.iter().any(|existing| existing == expr) {
                            output.push(expr.clone());
                        }
                    }
                }
                output
            }
        }
    }

    pub(super) fn expand_grouping_sets(
        &self,
        group_by: &GroupByClause,
    ) -> Option<Vec<Vec<Expression>>> {
        match group_by {
            GroupByClause::Simple(_) => None,
            GroupByClause::Rollup(exprs) => {
                let mut sets = Vec::new();
                for i in (0..=exprs.len()).rev() {
                    sets.push(exprs[..i].to_vec());
                }
                Some(sets)
            }
            GroupByClause::Cube(exprs) => {
                let n = exprs.len();
                let mut sets = Vec::new();
                for mask in (0..(1u32 << n)).rev() {
                    let mut set = Vec::new();
                    for (i, expr) in exprs.iter().enumerate() {
                        if mask & (1u32 << (n - 1 - i)) != 0 {
                            set.push(expr.clone());
                        }
                    }
                    sets.push(set);
                }
                Some(sets)
            }
            GroupByClause::GroupingSets(sets) => Some(sets.clone()),
        }
    }

    pub(super) fn infer_select_output_columns(
        &self,
        stmt: &SelectStatement,
    ) -> Result<Vec<ColumnDefinition>, RustqlError> {
        let source_columns = self.infer_select_source_columns(stmt)?;

        let mut output_columns = Vec::new();
        for column in &stmt.columns {
            if matches!(column, Column::All) {
                output_columns.extend(source_columns.iter().cloned());
                continue;
            }

            let name = match column {
                Column::Named { name, alias } => alias.clone().unwrap_or_else(|| name.clone()),
                Column::Expression { alias, .. } => {
                    alias.clone().unwrap_or_else(|| "<expression>".to_string())
                }
                Column::Function(agg) => format_aggregate_header(agg),
                Column::Subquery(_) => "<subquery>".to_string(),
                Column::All => unreachable!("wildcard projection handled before inference"),
            };

            let data_type = match column {
                Column::Named { name, .. } => source_columns
                    .iter()
                    .find(|column| {
                        column.name == *name
                            || self.unqualified_column_name(&column.name)
                                == self.unqualified_column_name(name)
                    })
                    .map(|column| column.data_type.clone())
                    .unwrap_or(DataType::Text),
                _ => DataType::Text,
            };

            output_columns.push(ColumnDefinition {
                name,
                data_type,
                nullable: true,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            });
        }

        Ok(output_columns)
    }

    pub(super) fn infer_select_source_columns(
        &self,
        stmt: &SelectStatement,
    ) -> Result<Vec<ColumnDefinition>, RustqlError> {
        let mut columns = self.infer_base_source_columns(stmt)?;

        for join in &stmt.joins {
            columns.extend(self.infer_join_source_columns(join)?);
        }

        Ok(columns)
    }

    pub(super) fn infer_base_source_columns(
        &self,
        stmt: &SelectStatement,
    ) -> Result<Vec<ColumnDefinition>, RustqlError> {
        if let Some(function) = stmt.from_function.as_ref() {
            return self.infer_table_function_columns(function);
        }

        if let Some((subquery, _)) = stmt.from_subquery.as_ref() {
            return self.infer_select_output_columns(subquery);
        }

        if let Some((rows, _, aliases)) = stmt.from_values.as_ref() {
            let width = rows.first().map(|row| row.len()).unwrap_or(0);
            return Ok((0..width)
                .map(|idx| ColumnDefinition {
                    name: aliases
                        .get(idx)
                        .cloned()
                        .unwrap_or_else(|| format!("column{}", idx + 1)),
                    data_type: DataType::Text,
                    nullable: true,
                    primary_key: false,
                    unique: false,
                    default_value: None,
                    foreign_key: None,
                    check: None,
                    auto_increment: false,
                    generated: None,
                })
                .collect());
        }

        if stmt.from.is_empty() {
            return Ok(Vec::new());
        }

        self.infer_named_source_columns(&stmt.from)
    }

    pub(super) fn infer_join_source_columns(
        &self,
        join: &Join,
    ) -> Result<Vec<ColumnDefinition>, RustqlError> {
        if let Some((subquery, _)) = join.subquery.as_ref() {
            return self.infer_select_output_columns(subquery);
        }

        self.infer_named_source_columns(&join.table)
    }

    pub(super) fn infer_named_source_columns(
        &self,
        source_name: &str,
    ) -> Result<Vec<ColumnDefinition>, RustqlError> {
        if let Some((_, cte)) = self.find_cte(source_name) {
            return self.infer_select_output_columns(&cte.query);
        }

        if let Some(table) = self.db.get_table(source_name) {
            return Ok(table.columns.clone());
        }

        if let Some(view) = self.db.get_view(source_name) {
            return self.infer_select_output_columns(&self.parse_view_query(&view.query_sql)?);
        }

        Err(RustqlError::TableNotFound(source_name.to_string()))
    }

    pub(super) fn infer_table_function_columns(
        &self,
        function: &TableFunction,
    ) -> Result<Vec<ColumnDefinition>, RustqlError> {
        match function.name.as_str() {
            "generate_series" => Ok(vec![ColumnDefinition {
                name: function
                    .alias
                    .clone()
                    .unwrap_or_else(|| "generate_series".to_string()),
                data_type: DataType::Integer,
                nullable: false,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            }]),
            other => Err(RustqlError::Internal(format!(
                "Unsupported table function in planner: {}",
                other
            ))),
        }
    }

    pub(super) fn parse_view_query(&self, query_sql: &str) -> Result<SelectStatement, RustqlError> {
        let tokens = crate::lexer::tokenize(query_sql)?;
        match crate::parser::parse(tokens)? {
            Statement::Select(select) => Ok(select),
            _ => Err(RustqlError::Internal(
                "View definition is not a SELECT statement".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::{Database, Table};

    fn column(name: &str, data_type: DataType) -> ColumnDefinition {
        ColumnDefinition {
            name: name.to_string(),
            data_type,
            nullable: true,
            primary_key: false,
            unique: false,
            default_value: None,
            foreign_key: None,
            check: None,
            auto_increment: false,
            generated: None,
        }
    }

    fn select_from_users(columns: Vec<Column>) -> SelectStatement {
        SelectStatement {
            columns,
            from: "users".to_string(),
            from_alias: None,
            from_subquery: None,
            from_values: None,
            from_function: None,
            joins: Vec::new(),
            where_clause: None,
            group_by: None,
            having: None,
            order_by: None,
            limit: None,
            offset: None,
            fetch: None,
            distinct: false,
            distinct_on: None,
            ctes: Vec::new(),
            set_op: None,
            window_definitions: Vec::new(),
        }
    }

    fn users_database() -> Database {
        let mut db = Database::new();
        db.tables.insert(
            "users".to_string(),
            Table::new(
                vec![
                    column("id", DataType::Integer),
                    column("name", DataType::Text),
                ],
                Vec::new(),
                Vec::new(),
            ),
        );
        db
    }

    #[test]
    fn output_inference_expands_leading_wildcard_and_later_projection() {
        let db = users_database();
        let planner = QueryPlanner::new(&db);
        let stmt = select_from_users(vec![
            Column::All,
            Column::Named {
                name: "id".to_string(),
                alias: Some("user_id".to_string()),
            },
        ]);

        let columns = planner
            .infer_select_output_columns(&stmt)
            .expect("output inference should succeed");

        assert_eq!(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["id", "name", "user_id"]
        );
        assert_eq!(columns[2].data_type, DataType::Integer);
    }

    #[test]
    fn output_inference_expands_trailing_wildcard() {
        let db = users_database();
        let planner = QueryPlanner::new(&db);
        let stmt = select_from_users(vec![
            Column::Named {
                name: "name".to_string(),
                alias: Some("display_name".to_string()),
            },
            Column::All,
        ]);

        let columns = planner
            .infer_select_output_columns(&stmt)
            .expect("output inference should succeed");

        assert_eq!(
            columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            vec!["display_name", "id", "name"]
        );
        assert_eq!(columns[0].data_type, DataType::Text);
    }
}
