use crate::ast::*;
use crate::database::{Database, Table};
use crate::error::RustqlError;
use crate::executor::ddl::{IndexUsage, find_index_usage};
use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap, HashSet};
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
        group_by: Vec<Expression>,
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

pub struct QueryPlanner<'a> {
    db: &'a Database,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(db: &'a Database) -> Self {
        QueryPlanner { db }
    }

    pub fn plan_select(&self, stmt: &SelectStatement) -> Result<PlanNode, RustqlError> {
        let db = self.db;

        let base_table = db
            .tables
            .get(&stmt.from)
            .ok_or_else(|| RustqlError::TableNotFound(stmt.from.clone()))?;

        let join_tables: HashSet<String> = stmt.joins.iter().map(|j| j.table.clone()).collect();

        let (base_filter, remaining_predicates) = if let Some(ref where_expr) = stmt.where_clause {
            let conjuncts = self.extract_conjuncts(where_expr);
            let mut base_preds = Vec::new();
            let mut rest = Vec::new();

            for conj in conjuncts {
                let refs = self.referenced_tables(&conj);
                if refs.is_empty()
                    || (refs.len() == 1 && refs.contains(&stmt.from))
                    || (!refs.iter().any(|r| join_tables.contains(r)) && refs.len() <= 1)
                {
                    base_preds.push(conj);
                } else {
                    rest.push(conj);
                }
            }

            (self.combine_conjuncts(base_preds), rest)
        } else {
            (None, Vec::new())
        };

        let base_stats = self.collect_table_stats(&stmt.from, base_table, db);
        let mut plan = self.plan_table_access(
            &stmt.from,
            base_table,
            &base_stats,
            base_filter.as_ref(),
            db,
        )?;

        if !stmt.joins.is_empty() {
            plan = self.plan_joins(plan, stmt, db, remaining_predicates)?;
        } else if !remaining_predicates.is_empty()
            && let Some(filter_expr) = self.combine_conjuncts(remaining_predicates)
        {
            plan = self.plan_filter(plan, filter_expr);
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

            plan = self.plan_aggregate(
                plan,
                group_by.exprs().to_vec(),
                aggregates,
                stmt.having.clone(),
            );
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
    ) -> Result<PlanNode, RustqlError> {
        if let Some(where_expr) = where_clause
            && let Some(index_usage) = self.find_best_index(table_name, where_expr, db)
        {
            let estimated_rows = self.estimate_index_selectivity(&index_usage, db, stats);
            let cost = self.estimate_index_scan_cost(stats.row_count, estimated_rows);

            return Ok(PlanNode::IndexScan {
                table: table_name.to_string(),
                index: index_usage.index_name().to_string(),
                filter: Some((*where_expr).clone()),
                cost,
                rows: estimated_rows,
            });
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
        mut remaining_predicates: Vec<Expression>,
    ) -> Result<PlanNode, RustqlError> {
        let mut current_plan = left_plan;
        let mut remaining_joins = stmt.joins.clone();

        let mut joined_tables: HashSet<String> = HashSet::new();
        joined_tables.insert(stmt.from.clone());

        while !remaining_joins.is_empty() {
            let (best_idx, _) = remaining_joins
                .iter()
                .enumerate()
                .map(|(idx, join)| (idx, self.join_priority(join, &joined_tables, db)))
                .max_by_key(|(_, priority)| *priority)
                .unwrap();
            let join = remaining_joins.remove(best_idx);

            let right_table = db
                .tables
                .get(&join.table)
                .ok_or_else(|| RustqlError::TableNotFound(join.table.clone()))?;

            let mut pushable = Vec::new();
            let mut kept = Vec::new();
            for pred in remaining_predicates {
                let refs = self.referenced_tables(&pred);
                if refs.len() == 1 && refs.contains(&join.table) {
                    pushable.push(pred);
                } else {
                    kept.push(pred);
                }
            }
            remaining_predicates = kept;

            let right_filter = self.combine_conjuncts(pushable);
            let right_stats = self.collect_table_stats(&join.table, right_table, db);
            let right_plan = self.plan_table_access(
                &join.table,
                right_table,
                &right_stats,
                right_filter.as_ref(),
                db,
            )?;

            let join_plan = if let Some(ref on_expr) = join.on {
                if self.is_equality_join(on_expr) {
                    self.plan_hash_join(current_plan, right_plan, on_expr.clone())
                } else {
                    self.plan_nested_loop_join(current_plan, right_plan, on_expr.clone())
                }
            } else {
                self.plan_nested_loop_join(
                    current_plan,
                    right_plan,
                    Expression::Value(Value::Boolean(true)),
                )
            };

            current_plan = join_plan;
            joined_tables.insert(join.table.clone());
        }

        if !remaining_predicates.is_empty()
            && let Some(filter_expr) = self.combine_conjuncts(remaining_predicates)
        {
            current_plan = self.plan_filter(current_plan, filter_expr);
        }

        Ok(current_plan)
    }

    fn join_priority(
        &self,
        join: &Join,
        joined_tables: &HashSet<String>,
        db: &Database,
    ) -> (bool, bool, bool, Reverse<usize>) {
        let connects = join.on.as_ref().is_some_and(|on_expr| {
            let refs = self.referenced_tables(on_expr);
            refs.contains(&join.table) && refs.iter().any(|t| joined_tables.contains(t))
        });
        let has_on = join.on.is_some();
        let eq_join = join.on.as_ref().is_some_and(|on| self.is_equality_join(on));
        let table_size = db
            .tables
            .get(&join.table)
            .map(|t| t.rows.len())
            .unwrap_or(0);
        (connects, eq_join, has_on, Reverse(table_size))
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
        group_by: Vec<Expression>,
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
                    BinaryOperator::Like | BinaryOperator::ILike => 0.2,
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
        db: &Database,
        stats: &TableStats,
    ) -> usize {
        match index_usage {
            IndexUsage::Equality {
                index_name, value, ..
            } => db
                .indexes
                .get(index_name)
                .and_then(|index| index.entries.get(value))
                .map(|v| v.len())
                .unwrap_or(0),
            IndexUsage::In {
                index_name, values, ..
            } => db
                .indexes
                .get(index_name)
                .map(|index| {
                    values
                        .iter()
                        .map(|v| index.entries.get(v).map(|rows| rows.len()).unwrap_or(0))
                        .sum()
                })
                .unwrap_or(0),
            IndexUsage::RangeGreater { .. }
            | IndexUsage::RangeLess { .. }
            | IndexUsage::RangeBetween { .. } => (stats.row_count as f64 * 0.1) as usize,
            IndexUsage::CompositePrefix {
                index_name, values, ..
            } => db
                .composite_indexes
                .get(index_name)
                .map(|index| {
                    if values.len() == index.columns.len() {
                        index
                            .entries
                            .get(values)
                            .map(|rows| rows.len())
                            .unwrap_or(0)
                    } else {
                        index
                            .entries
                            .iter()
                            .filter(|(key, _)| key.starts_with(values))
                            .map(|(_, rows)| rows.len())
                            .sum()
                    }
                })
                .unwrap_or(0),
        }
    }

    fn is_equality_join(&self, condition: &Expression) -> bool {
        if let Expression::BinaryOp { op, .. } = condition {
            matches!(op, BinaryOperator::Equal)
        } else {
            false
        }
    }

    fn collect_table_stats(&self, table_name: &str, table: &Table, db: &Database) -> TableStats {
        let row_count = table.rows.len();
        let mut column_stats = HashMap::new();

        let has_index = db.indexes.values().any(|idx| idx.table == table_name)
            || db
                .composite_indexes
                .values()
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
        find_index_usage(db, table_name, where_expr)
    }

    fn extract_conjuncts(&self, expr: &Expression) -> Vec<Expression> {
        match expr {
            Expression::BinaryOp {
                left,
                op: BinaryOperator::And,
                right,
            } => {
                let mut result = self.extract_conjuncts(left);
                result.extend(self.extract_conjuncts(right));
                result
            }
            other => vec![other.clone()],
        }
    }

    fn referenced_tables(&self, expr: &Expression) -> HashSet<String> {
        let mut tables = HashSet::new();
        self.collect_table_refs(expr, &mut tables);
        tables
    }

    fn collect_table_refs(&self, expr: &Expression, tables: &mut HashSet<String>) {
        match expr {
            Expression::Column(name) => {
                if let Some(dot_pos) = name.find('.') {
                    tables.insert(name[..dot_pos].to_string());
                }
            }
            Expression::BinaryOp { left, right, .. } => {
                self.collect_table_refs(left, tables);
                self.collect_table_refs(right, tables);
            }
            Expression::UnaryOp { expr, .. } => {
                self.collect_table_refs(expr, tables);
            }
            Expression::In { left, .. } => {
                self.collect_table_refs(left, tables);
            }
            Expression::IsNull { expr, .. } => {
                self.collect_table_refs(expr, tables);
            }
            Expression::Function(agg) => {
                self.collect_table_refs(&agg.expr, tables);
            }
            Expression::Case {
                operand,
                when_clauses,
                else_clause,
            } => {
                if let Some(op) = operand {
                    self.collect_table_refs(op, tables);
                }
                for (cond, result) in when_clauses {
                    self.collect_table_refs(cond, tables);
                    self.collect_table_refs(result, tables);
                }
                if let Some(el) = else_clause {
                    self.collect_table_refs(el, tables);
                }
            }
            Expression::ScalarFunction { args, .. } => {
                for arg in args {
                    self.collect_table_refs(arg, tables);
                }
            }
            Expression::WindowFunction {
                args,
                partition_by,
                order_by,
                ..
            } => {
                for arg in args {
                    self.collect_table_refs(arg, tables);
                }
                for expr in partition_by {
                    self.collect_table_refs(expr, tables);
                }
                for ob in order_by {
                    self.collect_table_refs(&ob.expr, tables);
                }
            }
            Expression::Cast { expr, .. } => {
                self.collect_table_refs(expr, tables);
            }
            Expression::Any { left, .. } | Expression::All { left, .. } => {
                self.collect_table_refs(left, tables);
            }
            Expression::IsDistinctFrom { left, right, .. } => {
                self.collect_table_refs(left, tables);
                self.collect_table_refs(right, tables);
            }
            Expression::Subquery(_) | Expression::Exists(_) | Expression::Value(_) => {}
        }
    }

    fn combine_conjuncts(&self, exprs: Vec<Expression>) -> Option<Expression> {
        let mut iter = exprs.into_iter();
        let first = iter.next()?;
        Some(iter.fold(first, |acc, e| Expression::BinaryOp {
            left: Box::new(acc),
            op: BinaryOperator::And,
            right: Box::new(e),
        }))
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
                writeln!(f, "{}Seq Scan on {}", indent_str, table)?;
                if filter.is_some() {
                    writeln!(f, "{}  Filter: [WHERE clause]", indent_str)?;
                }
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)
            }
            PlanNode::IndexScan {
                table,
                index,
                filter,
                cost,
                rows,
            } => {
                writeln!(f, "{}Index Scan using {} on {}", indent_str, index, table)?;
                if filter.is_some() {
                    writeln!(f, "{}  Filter: [WHERE clause]", indent_str)?;
                }
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)
            }
            PlanNode::NestedLoopJoin {
                left,
                right,
                condition: _,
                cost,
                rows,
            } => {
                writeln!(f, "{}Nested Loop Join", indent_str)?;
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
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
                writeln!(f, "{}Hash Join", indent_str)?;
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
                left.fmt_with_indent(f, indent + 1)?;
                right.fmt_with_indent(f, indent + 1)
            }
            PlanNode::Filter {
                input,
                condition: _,
                cost,
                rows,
            } => {
                writeln!(f, "{}Filter", indent_str)?;
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
                input.fmt_with_indent(f, indent + 1)
            }
            PlanNode::Sort {
                input,
                order_by: _,
                cost,
                rows,
            } => {
                writeln!(f, "{}Sort", indent_str)?;
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
                input.fmt_with_indent(f, indent + 1)
            }
            PlanNode::Limit {
                input,
                limit,
                offset,
                cost,
                rows,
            } => {
                writeln!(f, "{}Limit: {} Offset: {}", indent_str, limit, offset)?;
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
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
                let group_by_strs: Vec<String> =
                    group_by.iter().map(|e| format!("{:?}", e)).collect();
                writeln!(
                    f,
                    "{}Aggregate (Group By: {})",
                    indent_str,
                    group_by_strs.join(", ")
                )?;
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
                input.fmt_with_indent(f, indent + 1)
            }
        }
    }
}

pub fn explain_query(db: &Database, stmt: &SelectStatement) -> Result<String, RustqlError> {
    let planner = QueryPlanner::new(db);
    let plan = planner.plan_select(stmt)?;
    Ok(format!("Query Plan:\n{}", plan))
}
