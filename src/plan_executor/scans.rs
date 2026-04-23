use super::*;
use crate::database::{CompositeIndex, Index, Table, View};

impl<'a> PlanExecutor<'a> {
    pub(super) fn execute_values_scan(
        &self,
        values: &[Vec<Expression>],
        columns: &[String],
        filter: Option<&Expression>,
    ) -> Result<ExecutionResult, RustqlError> {
        let empty_columns: Vec<ColumnDefinition> = Vec::new();
        let empty_row: Vec<Value> = Vec::new();
        let output_columns: Vec<ColumnDefinition> = columns
            .iter()
            .map(|name| ColumnDefinition {
                name: name.clone(),
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
            .collect();

        let mut rows = Vec::with_capacity(values.len());
        for value_row in values {
            let row: Vec<Value> = value_row
                .iter()
                .map(|expr| self.evaluate_value_expression(expr, &empty_columns, &empty_row))
                .collect::<Result<_, _>>()?;
            let include = if let Some(filter_expr) = filter {
                self.evaluate_expression(filter_expr, &output_columns, &row)?
            } else {
                true
            };
            if include {
                rows.push(row);
            }
        }

        Ok(ExecutionResult {
            columns: columns.to_vec(),
            rows,
        })
    }

    pub(super) fn execute_seq_scan(
        &self,
        table_name: &str,
        output_label: Option<&str>,
        filter: Option<&Expression>,
    ) -> Result<ExecutionResult, RustqlError> {
        let table = self
            .db
            .get_table(table_name)
            .ok_or_else(|| RustqlError::TableNotFound(table_name.to_string()))?;

        let mut rows = Vec::new();

        for row in &table.rows {
            let include = if let Some(filter_expr) = filter {
                self.evaluate_expression(filter_expr, &table.columns, row)?
            } else {
                true
            };

            if include {
                rows.push(row.clone());
            }
        }

        let columns = qualify_column_names(&table.columns, output_label);

        Ok(ExecutionResult { columns, rows })
    }

    pub(super) fn execute_index_scan(
        &self,
        table_name: &str,
        index_name: &str,
        output_label: Option<&str>,
        filter: Option<&Expression>,
    ) -> Result<ExecutionResult, RustqlError> {
        let table = self
            .db
            .get_table(table_name)
            .ok_or_else(|| RustqlError::TableNotFound(table_name.to_string()))?;

        let row_ids = if let Some(filter_expr) = filter {
            if let Some(index_usage) =
                crate::executor::ddl::find_index_usage(self.db, table_name, filter_expr)
                    .filter(|usage| usage.index_name() == index_name)
            {
                crate::executor::ddl::get_indexed_rows(self.db, table, &index_usage)?
            } else {
                self.all_row_ids_for_index(index_name)?
            }
        } else {
            self.all_row_ids_for_index(index_name)?
        };

        let mut rows = Vec::new();
        for row_id in row_ids {
            if let Some(row) = table.row_by_id(row_id) {
                let include = if let Some(filter_expr) = filter {
                    self.evaluate_expression(filter_expr, &table.columns, row)?
                } else {
                    true
                };

                if include {
                    rows.push(row.clone());
                }
            }
        }

        let columns = qualify_column_names(&table.columns, output_label);

        Ok(ExecutionResult { columns, rows })
    }

    fn all_row_ids_for_index(&self, index_name: &str) -> Result<HashSet<RowId>, RustqlError> {
        if let Some(index) = self.db.get_index(index_name) {
            let mut row_ids = HashSet::new();
            for rows in index.entries.values() {
                row_ids.extend(rows.iter().copied());
            }
            return Ok(row_ids);
        }

        if let Some(index) = self.db.get_composite_index(index_name) {
            let mut row_ids = HashSet::new();
            for rows in index.entries.values() {
                row_ids.extend(rows.iter().copied());
            }
            return Ok(row_ids);
        }

        Err(RustqlError::IndexNotFound {
            name: index_name.to_string(),
        })
    }

    pub(super) fn execute_function_scan(
        &self,
        function: &TableFunction,
        output_label: Option<&str>,
        filter: Option<&Expression>,
    ) -> Result<ExecutionResult, RustqlError> {
        match function.name.as_str() {
            "generate_series" => {
                let empty_columns: Vec<ColumnDefinition> = Vec::new();
                let empty_row: Vec<Value> = Vec::new();

                let start = match self.evaluate_value_expression(
                    &function.args[0],
                    &empty_columns,
                    &empty_row,
                )? {
                    Value::Integer(value) => value,
                    _ => {
                        return Err(RustqlError::TypeMismatch(
                            "GENERATE_SERIES arguments must be integers".to_string(),
                        ));
                    }
                };
                let stop = match self.evaluate_value_expression(
                    &function.args[1],
                    &empty_columns,
                    &empty_row,
                )? {
                    Value::Integer(value) => value,
                    _ => {
                        return Err(RustqlError::TypeMismatch(
                            "GENERATE_SERIES arguments must be integers".to_string(),
                        ));
                    }
                };
                let step = if function.args.len() > 2 {
                    match self.evaluate_value_expression(
                        &function.args[2],
                        &empty_columns,
                        &empty_row,
                    )? {
                        Value::Integer(value) => value,
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "GENERATE_SERIES step must be an integer".to_string(),
                            ));
                        }
                    }
                } else if start <= stop {
                    1
                } else {
                    -1
                };

                if step == 0 {
                    return Err(RustqlError::Internal(
                        "GENERATE_SERIES step cannot be zero".to_string(),
                    ));
                }

                let column_name = qualified_column_name(
                    output_label,
                    function.alias.as_deref().unwrap_or("generate_series"),
                );
                let columns = vec![ColumnDefinition {
                    name: column_name.clone(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    default_value: None,
                    foreign_key: None,
                    check: None,
                    auto_increment: false,
                    generated: None,
                }];

                let mut rows = Vec::new();
                let mut current = start;
                if step > 0 {
                    while current <= stop {
                        let row = vec![Value::Integer(current)];
                        let include = if let Some(filter_expr) = filter {
                            self.evaluate_expression(filter_expr, &columns, &row)?
                        } else {
                            true
                        };
                        if include {
                            rows.push(row);
                        }
                        current += step;
                    }
                } else {
                    while current >= stop {
                        let row = vec![Value::Integer(current)];
                        let include = if let Some(filter_expr) = filter {
                            self.evaluate_expression(filter_expr, &columns, &row)?
                        } else {
                            true
                        };
                        if include {
                            rows.push(row);
                        }
                        current += step;
                    }
                }

                Ok(ExecutionResult {
                    columns: vec![column_name],
                    rows,
                })
            }
            other => Err(RustqlError::Internal(format!(
                "Unsupported table function in plan executor: {}",
                other
            ))),
        }
    }

    pub(super) fn execute_source_scan(
        &self,
        input: &PlanNode,
        select: &SelectStatement,
        output_label: Option<&str>,
    ) -> Result<ExecutionResult, RustqlError> {
        let mut result = self.execute(input, select)?;
        if let Some(label) = output_label {
            result.columns = result
                .columns
                .iter()
                .map(|column| qualified_column_name(Some(label), unqualified_column_name(column)))
                .collect();
        }
        Ok(result)
    }

    pub(super) fn execute_recursive_cte_scan(
        &self,
        cte: &str,
        base: &PlanNode,
        base_select: &SelectStatement,
        recursive_select: &SelectStatement,
        union_all: bool,
        output_label: Option<&str>,
    ) -> Result<ExecutionResult, RustqlError> {
        let base_result = self.execute(base, base_select)?;
        let cte_columns = column_definitions_from_result(&base_result);

        let mut all_rows = base_result.rows.clone();
        let mut working_rows = base_result.rows;
        let mut seen: Option<BTreeSet<Vec<Value>>> = if union_all {
            None
        } else {
            let mut set = BTreeSet::new();
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

            let working_table = Table::new(cte_columns.clone(), working_rows, Vec::new());
            let scoped_db = ScopedTableDatabase::new(self.db, cte.to_string(), working_table);
            let recursive_result = execute_planned_select(&scoped_db, recursive_select)?;

            let mut new_rows = Vec::new();
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
                cte, MAX_RECURSIVE_CTE_ITERATIONS
            )));
        }

        let mut result = ExecutionResult {
            columns: base_result.columns,
            rows: all_rows,
        };
        if let Some(label) = output_label {
            result.columns = result
                .columns
                .iter()
                .map(|column| qualified_column_name(Some(label), unqualified_column_name(column)))
                .collect();
        }
        Ok(result)
    }
}

struct ScopedTableDatabase<'a> {
    base: &'a dyn DatabaseCatalog,
    table_name: String,
    table: Table,
}

impl<'a> ScopedTableDatabase<'a> {
    fn new(base: &'a dyn DatabaseCatalog, table_name: String, table: Table) -> Self {
        Self {
            base,
            table_name,
            table,
        }
    }
}

impl DatabaseCatalog for ScopedTableDatabase<'_> {
    fn get_table(&self, name: &str) -> Option<&Table> {
        if name == self.table_name {
            Some(&self.table)
        } else {
            self.base.get_table(name)
        }
    }

    fn get_index(&self, name: &str) -> Option<&Index> {
        self.base.get_index(name)
    }

    fn get_view(&self, name: &str) -> Option<&View> {
        self.base.get_view(name)
    }

    fn get_composite_index(&self, name: &str) -> Option<&CompositeIndex> {
        self.base.get_composite_index(name)
    }

    fn indexes_iter(&self) -> Box<dyn Iterator<Item = &Index> + '_> {
        self.base.indexes_iter()
    }

    fn composite_indexes_iter(&self) -> Box<dyn Iterator<Item = &CompositeIndex> + '_> {
        self.base.composite_indexes_iter()
    }
}
