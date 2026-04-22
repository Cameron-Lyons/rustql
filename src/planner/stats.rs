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
        let mut column_stats = HashMap::new();

        let has_index = db.indexes_iter().any(|idx| idx.table == table_name)
            || db
                .composite_indexes_iter()
                .any(|idx| idx.table == table_name);

        for (col_idx, col_def) in table.columns.iter().enumerate() {
            if !table.rows.is_empty() {
                let mut distinct_values = BTreeSet::new();
                let mut null_count = 0;
                let mut min_val: Option<Value> = None;
                let mut max_val: Option<Value> = None;

                for row in &table.rows {
                    if col_idx < row.len() {
                        let val = &row[col_idx];
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
