use crate::ast::*;
use crate::database::{Database, Index, Table};
use std::collections::{BTreeSet, HashMap};
use std::fmt;

#[derive(Debug, Clone)]
pub enum PlanNode {
    SeqScan {
        table: String,
        filter: Option<Expression>,
        cost: f64,
        rows: usize,
    },

    IndexScan {
        table: String,
        index: String,
        filter: Option<Expression>,
        cost: f64,
        rows: usize,
    },

    NestedLoopJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        condition: Expression,
        cost: f64,
        rows: usize,
    },

    HashJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        condition: Expression,
        cost: f64,
        rows: usize,
    },

    Filter {
        input: Box<PlanNode>,
        condition: Expression,
        cost: f64,
        rows: usize,
    },

    Sort {
        input: Box<PlanNode>,
        order_by: Vec<OrderByExpr>,
        cost: f64,
        rows: usize,
    },

    Limit {
        input: Box<PlanNode>,
        limit: usize,
        offset: usize,
        cost: f64,
        rows: usize,
    },

    Aggregate {
        input: Box<PlanNode>,
        group_by: Vec<String>,
        aggregates: Vec<AggregateFunction>,
        having: Option<Expression>,
        cost: f64,
        rows: usize,
    },
}

#[derive(Debug, Clone)]
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

pub struct QueryPlanner {
    db: *const Database,
}

unsafe impl Send for QueryPlanner {}
unsafe impl Sync for QueryPlanner {}

impl QueryPlanner {
    pub fn new(db: &Database) -> Self {
        QueryPlanner { db }
    }

    pub fn plan_select(&self, stmt: &SelectStatement) -> Result<PlanNode, String> {
        let db = unsafe { &*self.db };

        let base_table = db
            .tables
            .get(&stmt.from)
            .ok_or_else(|| format!("Table '{}' does not exist", stmt.from))?;

        let base_stats = self.collect_table_stats(&stmt.from, base_table, db);
        let mut plan = self.plan_table_access(
            &stmt.from,
            base_table,
            &base_stats,
            stmt.where_clause.as_ref(),
            db,
        )?;

        if !stmt.joins.is_empty() {
            plan = self.plan_joins(plan, stmt, db)?;
        }

        if let Some(ref where_expr) = stmt.where_clause {
            if !self.is_filter_applied(&plan, where_expr) {
                plan = self.plan_filter(plan, where_expr.clone());
            }
        }

        if let Some(ref group_by) = stmt.group_by {
            let aggregates: Vec<AggregateFunction> = stmt
                .columns
                .iter()
                .filter_map(|col| {
                    if let Column::Function(agg) = col {
                        Some(agg.clone())
                    } else {
                        None
                    }
                })
                .collect();

            plan = self.plan_aggregate(plan, group_by.clone(), aggregates, stmt.having.clone());
        }

        if let Some(ref order_by) = stmt.order_by {
            plan = self.plan_sort(plan, order_by.clone());
        }

        let limit = stmt.limit.unwrap_or(usize::MAX);
        let offset = stmt.offset.unwrap_or(0);
        if limit != usize::MAX || offset != 0 {
            plan = self.plan_limit(plan, limit, offset);
        }

        Ok(plan)
    }

    fn plan_table_access(
        &self,
        table_name: &str,
        _table: &Table,
        stats: &TableStats,
        where_clause: Option<&Expression>,
        db: &Database,
    ) -> Result<PlanNode, String> {
        if let Some(ref where_expr) = where_clause {
            if let Some(index_usage) = self.find_best_index(table_name, where_expr, db) {
                let index = db.indexes.get(&index_usage.index_name).unwrap();
                let estimated_rows = self.estimate_index_selectivity(&index_usage, index, stats);
                let cost = self.estimate_index_scan_cost(stats.row_count, estimated_rows);

                return Ok(PlanNode::IndexScan {
                    table: table_name.to_string(),
                    index: index_usage.index_name,
                    filter: Some((*where_expr).clone()),
                    cost,
                    rows: estimated_rows,
                });
            }
        }

        let cost = self.estimate_seq_scan_cost(stats.row_count);
        let rows = stats.row_count;

        Ok(PlanNode::SeqScan {
            table: table_name.to_string(),
            filter: where_clause.cloned(),
            cost,
            rows,
        })
    }

    fn plan_joins(
        &self,
        left_plan: PlanNode,
        stmt: &SelectStatement,
        db: &Database,
    ) -> Result<PlanNode, String> {
        let mut current_plan = left_plan;
        let mut remaining_joins = stmt.joins.clone();

        remaining_joins.sort_by(|a, b| {
            let a_size = db.tables.get(&a.table).map(|t| t.rows.len()).unwrap_or(0);
            let b_size = db.tables.get(&b.table).map(|t| t.rows.len()).unwrap_or(0);
            a_size.cmp(&b_size)
        });

        for join in remaining_joins {
            let right_table = db
                .tables
                .get(&join.table)
                .ok_or_else(|| format!("Table '{}' does not exist", join.table))?;

            let right_stats = self.collect_table_stats(&join.table, right_table, db);
            let right_plan =
                self.plan_table_access(&join.table, right_table, &right_stats, None, db)?;

            let join_plan = if self.is_equality_join(&join.on) {
                self.plan_hash_join(current_plan, right_plan, join.on.clone())
            } else {
                self.plan_nested_loop_join(current_plan, right_plan, join.on.clone())
            };

            current_plan = join_plan;
        }

        Ok(current_plan)
    }

    fn plan_hash_join(&self, left: PlanNode, right: PlanNode, condition: Expression) -> PlanNode {
        let left_rows = self.estimate_rows(&left);
        let right_rows = self.estimate_rows(&right);
        let cost = self.estimate_hash_join_cost(&left, &right);
        let estimated_output_rows = self.estimate_join_rows(left_rows, right_rows, &condition);

        PlanNode::HashJoin {
            left: Box::new(left),
            right: Box::new(right),
            condition,
            cost,
            rows: estimated_output_rows,
        }
    }

    fn plan_nested_loop_join(
        &self,
        left: PlanNode,
        right: PlanNode,
        condition: Expression,
    ) -> PlanNode {
        let left_rows = self.estimate_rows(&left);
        let right_rows = self.estimate_rows(&right);
        let cost = self.estimate_nested_loop_join_cost(&left, &right);
        let estimated_output_rows = self.estimate_join_rows(left_rows, right_rows, &condition);

        PlanNode::NestedLoopJoin {
            left: Box::new(left),
            right: Box::new(right),
            condition,
            cost,
            rows: estimated_output_rows,
        }
    }

    fn plan_filter(&self, input: PlanNode, condition: Expression) -> PlanNode {
        let input_rows = self.estimate_rows(&input);
        let input_cost = self.estimate_cost(&input);
        let selectivity = self.estimate_selectivity(&condition, input_rows);
        let filtered_rows = (input_rows as f64 * selectivity) as usize;
        let cost = input_rows as f64 * 0.1; // Filter is relatively cheap

        PlanNode::Filter {
            input: Box::new(input),
            condition,
            cost: input_cost + cost,
            rows: filtered_rows,
        }
    }

    fn plan_sort(&self, input: PlanNode, order_by: Vec<OrderByExpr>) -> PlanNode {
        let input_rows = self.estimate_rows(&input);
        let base_cost = self.estimate_cost(&input);
        let cost = self.estimate_sort_cost(input_rows);

        PlanNode::Sort {
            input: Box::new(input),
            order_by,
            cost: base_cost + cost,
            rows: input_rows,
        }
    }

    fn plan_limit(&self, input: PlanNode, limit: usize, offset: usize) -> PlanNode {
        let input_rows = self.estimate_rows(&input);
        let output_rows = (input_rows.saturating_sub(offset)).min(limit);
        let cost = self.estimate_cost(&input) + (offset + limit) as f64 * 0.01;

        PlanNode::Limit {
            input: Box::new(input),
            limit,
            offset,
            cost,
            rows: output_rows,
        }
    }

    fn plan_aggregate(
        &self,
        input: PlanNode,
        group_by: Vec<String>,
        aggregates: Vec<AggregateFunction>,
        having: Option<Expression>,
    ) -> PlanNode {
        let input_rows = self.estimate_rows(&input);
        let base_cost = self.estimate_cost(&input);

        let output_rows = (input_rows as f64 * 0.1).max(1.0) as usize;
        let cost = self.estimate_aggregate_cost(input_rows, group_by.len(), aggregates.len());

        PlanNode::Aggregate {
            input: Box::new(input),
            group_by,
            aggregates,
            having,
            cost: base_cost + cost,
            rows: output_rows,
        }
    }

    fn estimate_cost(&self, plan: &PlanNode) -> f64 {
        match plan {
            PlanNode::SeqScan { cost, .. } => *cost,
            PlanNode::IndexScan { cost, .. } => *cost,
            PlanNode::NestedLoopJoin { cost, .. } => *cost,
            PlanNode::HashJoin { cost, .. } => *cost,
            PlanNode::Filter { cost, .. } => *cost,
            PlanNode::Sort { cost, .. } => *cost,
            PlanNode::Limit { cost, .. } => *cost,
            PlanNode::Aggregate { cost, .. } => *cost,
        }
    }

    fn estimate_rows(&self, plan: &PlanNode) -> usize {
        match plan {
            PlanNode::SeqScan { rows, .. } => *rows,
            PlanNode::IndexScan { rows, .. } => *rows,
            PlanNode::NestedLoopJoin { rows, .. } => *rows,
            PlanNode::HashJoin { rows, .. } => *rows,
            PlanNode::Filter { rows, .. } => *rows,
            PlanNode::Sort { rows, .. } => *rows,
            PlanNode::Limit { rows, .. } => *rows,
            PlanNode::Aggregate { rows, .. } => *rows,
        }
    }

    fn estimate_seq_scan_cost(&self, row_count: usize) -> f64 {
        row_count as f64 * 1.0
    }

    fn estimate_index_scan_cost(&self, total_rows: usize, selected_rows: usize) -> f64 {
        (total_rows as f64).ln() * 2.0 + selected_rows as f64 * 0.5
    }

    fn estimate_hash_join_cost(&self, left: &PlanNode, right: &PlanNode) -> f64 {
        let left_rows = self.estimate_rows(left);
        let right_rows = self.estimate_rows(right);
        let left_cost = self.estimate_cost(left);
        let right_cost = self.estimate_cost(right);

        let build_cost = left_rows.min(right_rows) as f64 * 1.5;
        let probe_cost = left_rows.max(right_rows) as f64 * 0.5;

        left_cost + right_cost + build_cost + probe_cost
    }

    fn estimate_nested_loop_join_cost(&self, left: &PlanNode, right: &PlanNode) -> f64 {
        let left_rows = self.estimate_rows(left);
        let right_rows = self.estimate_rows(right);
        let left_cost = self.estimate_cost(left);
        let right_cost = self.estimate_cost(right);

        left_cost + right_cost + (left_rows * right_rows) as f64
    }

    fn estimate_sort_cost(&self, row_count: usize) -> f64 {
        row_count as f64 * (row_count as f64).ln() * 0.5
    }

    fn estimate_aggregate_cost(
        &self,
        input_rows: usize,
        group_by_cols: usize,
        agg_count: usize,
    ) -> f64 {
        input_rows as f64 * (1.0 + (group_by_cols + agg_count) as f64 * 0.1)
    }

    fn estimate_selectivity(&self, condition: &Expression, total_rows: usize) -> f64 {
        match condition {
            Expression::BinaryOp { op, .. } => {
                match op {
                    BinaryOperator::Equal => 0.1, // Assume 10% selectivity for equality
                    BinaryOperator::NotEqual => 0.9,
                    BinaryOperator::LessThan | BinaryOperator::LessThanOrEqual => 0.5,
                    BinaryOperator::GreaterThan | BinaryOperator::GreaterThanOrEqual => 0.5,
                    BinaryOperator::And => 0.3, // AND reduces selectivity
                    BinaryOperator::Or => 0.7,  // OR increases selectivity
                    BinaryOperator::Like => 0.2, // LIKE is somewhat selective
                    BinaryOperator::Between => 0.3,
                    _ => 0.5,
                }
            }
            Expression::In { values, .. } => {
                (values.len() as f64 / total_rows.max(1) as f64).min(1.0)
            }
            Expression::IsNull { .. } => 0.1,
            _ => 0.5,
        }
    }

    fn estimate_join_rows(
        &self,
        left_rows: usize,
        right_rows: usize,
        condition: &Expression,
    ) -> usize {
        if self.is_equality_join(condition) {
            ((left_rows * right_rows) as f64 * 0.1) as usize
        } else {
            ((left_rows * right_rows) as f64 * 0.01) as usize
        }
    }

    fn estimate_index_selectivity(
        &self,
        index_usage: &IndexUsage,
        index: &Index,
        stats: &TableStats,
    ) -> usize {
        match &index_usage.operation {
            IndexOperation::Equality(_) => index
                .entries
                .get(&index_usage.value.clone().unwrap())
                .map(|v| v.len())
                .unwrap_or(0),
            IndexOperation::Range { .. } => (stats.row_count as f64 * 0.1) as usize,
            IndexOperation::In(values) => values
                .iter()
                .map(|v| index.entries.get(v).map(|rows| rows.len()).unwrap_or(0))
                .sum(),
        }
    }

    fn is_equality_join(&self, condition: &Expression) -> bool {
        if let Expression::BinaryOp { op, .. } = condition {
            matches!(op, BinaryOperator::Equal)
        } else {
            false
        }
    }

    fn is_filter_applied(&self, plan: &PlanNode, filter: &Expression) -> bool {
        match plan {
            PlanNode::SeqScan { filter: f, .. } | PlanNode::IndexScan { filter: f, .. } => {
                f.as_ref().map(|f| f == filter).unwrap_or(false)
            }
            PlanNode::Filter { condition, .. } => condition == filter,
            _ => false,
        }
    }

    fn collect_table_stats(&self, table_name: &str, table: &Table, db: &Database) -> TableStats {
        let row_count = table.rows.len();
        let mut column_stats = HashMap::new();

        let has_index = db.indexes.values().any(|idx| idx.table == table_name);

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
                            if min_val.is_none()
                                || self.compare_values(val, min_val.as_ref().unwrap()) < 0
                            {
                                min_val = Some(val.clone());
                            }
                            if max_val.is_none()
                                || self.compare_values(val, max_val.as_ref().unwrap()) > 0
                            {
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

    fn compare_values(&self, a: &Value, b: &Value) -> i32 {
        match (a, b) {
            (Value::Integer(i1), Value::Integer(i2)) => i1.cmp(i2) as i32,
            (Value::Float(f1), Value::Float(f2)) => {
                f1.partial_cmp(f2).unwrap_or(std::cmp::Ordering::Equal) as i32
            }
            (Value::Text(s1), Value::Text(s2)) => s1.cmp(s2) as i32,
            (Value::Boolean(b1), Value::Boolean(b2)) => b1.cmp(b2) as i32,
            _ => 0,
        }
    }

    fn find_best_index(
        &self,
        table_name: &str,
        where_expr: &Expression,
        db: &Database,
    ) -> Option<IndexUsage> {
        self.find_index_usage_in_expression(table_name, where_expr, db)
    }

    fn find_index_usage_in_expression(
        &self,
        table_name: &str,
        expr: &Expression,
        db: &Database,
    ) -> Option<IndexUsage> {
        match expr {
            Expression::BinaryOp { left, op, right } => {
                if let Expression::Column(col_name) = left.as_ref() {
                    let normalized_col = if col_name.contains('.') {
                        col_name.split('.').next_back().unwrap_or(col_name)
                    } else {
                        col_name
                    };

                    if let Some(value) = self.extract_value(right) {
                        for (idx_name, idx) in db.indexes.iter() {
                            if idx.table == table_name && idx.column == normalized_col {
                                return Some(IndexUsage {
                                    index_name: idx_name.clone(),
                                    column: normalized_col.to_string(),
                                    operation: match op {
                                        BinaryOperator::Equal => {
                                            IndexOperation::Equality(value.clone())
                                        }
                                        BinaryOperator::LessThan
                                        | BinaryOperator::LessThanOrEqual => {
                                            IndexOperation::Range {
                                                min: None,
                                                max: Some(value.clone()),
                                            }
                                        }
                                        BinaryOperator::GreaterThan
                                        | BinaryOperator::GreaterThanOrEqual => {
                                            IndexOperation::Range {
                                                min: Some(value.clone()),
                                                max: None,
                                            }
                                        }
                                        _ => continue,
                                    },
                                    value: Some(value),
                                });
                            }
                        }
                    }
                }
                None
            }
            Expression::In { left, values } => {
                if let Expression::Column(col_name) = left.as_ref() {
                    let normalized_col = if col_name.contains('.') {
                        col_name.split('.').next_back().unwrap_or(col_name)
                    } else {
                        col_name
                    };

                    for (idx_name, idx) in db.indexes.iter() {
                        if idx.table == table_name && idx.column == normalized_col {
                            return Some(IndexUsage {
                                index_name: idx_name.clone(),
                                column: normalized_col.to_string(),
                                operation: IndexOperation::In(values.clone()),
                                value: None,
                            });
                        }
                    }
                }
                None
            }
            _ => None,
        }
    }

    fn extract_value(&self, expr: &Expression) -> Option<Value> {
        match expr {
            Expression::Value(v) => Some(v.clone()),
            _ => None,
        }
    }
}

impl fmt::Display for PlanNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_indent(f, 0)
    }
}

impl PlanNode {
    fn fmt_with_indent(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let indent_str = "  ".repeat(indent);
        match self {
            PlanNode::SeqScan {
                table,
                filter,
                cost,
                rows,
            } => {
                write!(f, "{}Seq Scan on {}\n", indent_str, table)?;
                if let Some(_) = filter {
                    write!(f, "{}  Filter: [WHERE clause]\n", indent_str)?;
                }
                write!(f, "{}  Cost: {:.2}, Rows: {}\n", indent_str, cost, rows)
            }
            PlanNode::IndexScan {
                table,
                index,
                filter,
                cost,
                rows,
            } => {
                write!(f, "{}Index Scan using {} on {}\n", indent_str, index, table)?;
                if let Some(_) = filter {
                    write!(f, "{}  Filter: [WHERE clause]\n", indent_str)?;
                }
                write!(f, "{}  Cost: {:.2}, Rows: {}\n", indent_str, cost, rows)
            }
            PlanNode::NestedLoopJoin {
                left,
                right,
                condition: _,
                cost,
                rows,
            } => {
                write!(f, "{}Nested Loop Join\n", indent_str)?;
                write!(f, "{}  Cost: {:.2}, Rows: {}\n", indent_str, cost, rows)?;
                left.fmt_with_indent(f, indent + 1)?;
                right.fmt_with_indent(f, indent + 1)
            }
            PlanNode::HashJoin {
                left,
                right,
                condition: _,
                cost,
                rows,
            } => {
                write!(f, "{}Hash Join\n", indent_str)?;
                write!(f, "{}  Cost: {:.2}, Rows: {}\n", indent_str, cost, rows)?;
                left.fmt_with_indent(f, indent + 1)?;
                right.fmt_with_indent(f, indent + 1)
            }
            PlanNode::Filter {
                input,
                condition: _,
                cost,
                rows,
            } => {
                write!(f, "{}Filter\n", indent_str)?;
                write!(f, "{}  Cost: {:.2}, Rows: {}\n", indent_str, cost, rows)?;
                input.fmt_with_indent(f, indent + 1)
            }
            PlanNode::Sort {
                input,
                order_by: _,
                cost,
                rows,
            } => {
                write!(f, "{}Sort\n", indent_str)?;
                write!(f, "{}  Cost: {:.2}, Rows: {}\n", indent_str, cost, rows)?;
                input.fmt_with_indent(f, indent + 1)
            }
            PlanNode::Limit {
                input,
                limit,
                offset,
                cost,
                rows,
            } => {
                write!(f, "{}Limit: {} Offset: {}\n", indent_str, limit, offset)?;
                write!(f, "{}  Cost: {:.2}, Rows: {}\n", indent_str, cost, rows)?;
                input.fmt_with_indent(f, indent + 1)
            }
            PlanNode::Aggregate {
                input,
                group_by,
                aggregates: _,
                having: _,
                cost,
                rows,
            } => {
                write!(
                    f,
                    "{}Aggregate (Group By: {})\n",
                    indent_str,
                    group_by.join(", ")
                )?;
                write!(f, "{}  Cost: {:.2}, Rows: {}\n", indent_str, cost, rows)?;
                input.fmt_with_indent(f, indent + 1)
            }
        }
    }
}

pub fn explain_query(db: &Database, stmt: &SelectStatement) -> Result<String, String> {
    let planner = QueryPlanner::new(db);
    let plan = planner.plan_select(stmt)?;
    Ok(format!("Query Plan:\n{}", plan))
}

#[derive(Debug, Clone)]
struct IndexUsage {
    index_name: String,
    #[allow(dead_code)]
    column: String,
    operation: IndexOperation,
    #[allow(dead_code)]
    value: Option<Value>,
}

#[derive(Debug, Clone)]
enum IndexOperation {
    #[allow(dead_code)]
    Equality(Value),
    Range {
        #[allow(dead_code)]
        min: Option<Value>,
        #[allow(dead_code)]
        max: Option<Value>,
    },
    In(Vec<Value>),
}
