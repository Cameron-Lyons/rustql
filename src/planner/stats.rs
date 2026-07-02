use super::*;

pub struct TableStats {
    pub row_count: usize,
    pub column_stats: HashMap<String, ColumnStats>,
    pub has_index: bool,
}

#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub distinct_count: usize,
    pub null_count: usize,
    pub min_value: Option<Value>,
    pub max_value: Option<Value>,
}

impl<'a> QueryPlanner<'a> {
    pub(super) fn collect_table_stats(
        &self,
        table_name: &str,
        table: &Table,
        db: &dyn DatabaseCatalog,
    ) -> TableStats {
        let row_count = table.rows.len();
        let mut column_stats = HashMap::with_capacity(table.columns.len());

        let has_index = db.indexes_iter().any(|idx| idx.table == table_name)
            || db
                .composite_indexes_iter()
                .any(|idx| idx.table == table_name);

        for (col_idx, col_def) in table.columns.iter().enumerate() {
            let mut distinct_values = BTreeSet::new();
            let mut null_count = 0;
            let mut min_val: Option<Value> = None;
            let mut max_val: Option<Value> = None;

            for row in &table.rows {
                if let Some(val) = row.get(col_idx) {
                    if matches!(val, Value::Null) {
                        null_count += 1;
                    } else {
                        distinct_values.insert(val.clone());
                        if match min_val.as_ref() {
                            Some(current_min) => {
                                compare_values_same_type(val, current_min) == Ordering::Less
                            }
                            None => true,
                        } {
                            min_val = Some(val.clone());
                        }
                        if match max_val.as_ref() {
                            Some(current_max) => {
                                compare_values_same_type(val, current_max) == Ordering::Greater
                            }
                            None => true,
                        } {
                            max_val = Some(val.clone());
                        }
                    }
                }
            }

            column_stats.insert(
                col_def.name.clone(),
                ColumnStats {
                    distinct_count: distinct_values.len(),
                    null_count,
                    min_value: min_val,
                    max_value: max_val,
                },
            );
        }

        TableStats {
            row_count,
            column_stats,
            has_index,
        }
    }

    pub(super) fn find_best_index(
        &self,
        table_name: &str,
        where_expr: &Expression,
        db: &dyn DatabaseCatalog,
    ) -> Option<IndexUsage> {
        find_index_usage(db, table_name, where_expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;

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

    #[test]
    fn empty_table_stats_include_each_column() {
        let db = Database::new();
        let table = Table::new(
            vec![
                column("id", DataType::Integer),
                column("name", DataType::Text),
            ],
            Vec::new(),
            Vec::new(),
        );
        let planner = QueryPlanner::new(&db);

        let stats = planner.collect_table_stats("users", &table, &db);

        assert_eq!(stats.row_count, 0);
        assert_eq!(stats.column_stats.len(), 2);
        for column_name in ["id", "name"] {
            let column_stats = stats
                .column_stats
                .get(column_name)
                .expect("missing empty table column stats");
            assert_eq!(column_stats.distinct_count, 0);
            assert_eq!(column_stats.null_count, 0);
            assert_eq!(column_stats.min_value, None);
            assert_eq!(column_stats.max_value, None);
        }
    }
}
