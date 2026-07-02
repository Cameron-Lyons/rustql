use super::*;

pub struct TableStats {
    pub row_count: usize,
}

impl<'a> QueryPlanner<'a> {
    pub(super) fn collect_table_stats(&self, table: &Table) -> TableStats {
        let row_count = table.rows.len();

        TableStats { row_count }
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
