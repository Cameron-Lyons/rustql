use crate::ast::*;
use crate::database::{DatabaseCatalog, Table};
use crate::error::RustqlError;
use crate::executor::aggregate::format_aggregate_header;
use crate::executor::ddl::{IndexUsage, find_index_usage};
use crate::executor::expr::compare_values_same_type;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;

const DEFAULT_GENERATE_SERIES_ROWS: usize = 100;
const DEFAULT_LATERAL_ROWS: usize = 10;
const FUNCTION_SCAN_ROW_COST: f64 = 0.2;
const FILTER_ROW_COST: f64 = 0.1;
const LIMIT_ROW_COST: f64 = 0.01;
const DISTINCT_ON_ROW_COST: f64 = 0.05;
const DISTINCT_ON_ROW_REDUCTION_DIVISOR: usize = 2;
const AGGREGATE_GROUP_OUTPUT_SELECTIVITY: f64 = 0.1;
const AGGREGATE_PER_STATE_COST: f64 = 0.1;
const INDEX_SCAN_SEEK_COST_MULTIPLIER: f64 = 2.0;
const INDEX_SCAN_ROW_COST: f64 = 0.5;
const HASH_JOIN_BUILD_ROW_COST: f64 = 1.5;
const HASH_JOIN_PROBE_ROW_COST: f64 = 0.5;
const SORT_COMPLEXITY_COST: f64 = 0.5;
const LATERAL_ROW_COST: f64 = 0.5;
const LATERAL_FIXED_COST: f64 = 0.5;
const SELECTIVITY_EQUAL: f64 = 0.1;
const SELECTIVITY_NOT_EQUAL: f64 = 0.9;
const SELECTIVITY_ORDERED_COMPARISON: f64 = 0.5;
const SELECTIVITY_AND: f64 = 0.3;
const SELECTIVITY_OR: f64 = 0.7;
const SELECTIVITY_LIKE: f64 = 0.2;
const SELECTIVITY_BETWEEN: f64 = 0.3;
const SELECTIVITY_IS_NULL: f64 = 0.1;
const SELECTIVITY_DEFAULT: f64 = 0.5;
const SELECTIVITY_EQUAL_JOIN: f64 = 0.1;
const SELECTIVITY_NON_EQUAL_JOIN: f64 = 0.01;
const INDEX_RANGE_SELECTIVITY: f64 = 0.1;

#[derive(Debug, Clone, PartialEq)]
pub enum PlanNode {
    OneRow {
        cost: f64,
        rows: usize,
    },

    SeqScan {
        table: String,
        output_label: Option<String>,
        filter: Option<Expression>,
        cost: f64,
        rows: usize,
    },

    IndexScan {
        table: String,
        index: String,
        output_label: Option<String>,
        filter: Option<Expression>,
        cost: f64,
        rows: usize,
    },

    FunctionScan {
        function: TableFunction,
        output_label: Option<String>,
        filter: Option<Expression>,
        cost: f64,
        rows: usize,
    },

    ValuesScan {
        values: Vec<Vec<Expression>>,
        columns: Vec<String>,
        filter: Option<Expression>,
        cost: f64,
        rows: usize,
    },

    NestedLoopJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        join_type: JoinType,
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

    LateralJoin {
        left: Box<PlanNode>,
        subquery: Box<SelectStatement>,
        alias: String,
        right_columns: Vec<String>,
        join_type: JoinType,
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

    DistinctOn {
        input: Box<PlanNode>,
        distinct_on: Vec<Expression>,
        cost: f64,
        rows: usize,
    },

    Limit {
        input: Box<PlanNode>,
        limit: usize,
        offset: usize,
        with_ties: bool,
        order_by: Vec<OrderByExpr>,
        cost: f64,
        rows: usize,
    },

    Aggregate {
        input: Box<PlanNode>,
        group_by: Vec<Expression>,
        grouping_sets: Option<Vec<Vec<Expression>>>,
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
    db: &'a dyn DatabaseCatalog,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(db: &'a dyn DatabaseCatalog) -> Self {
        QueryPlanner { db }
    }

    pub fn plan_select(&self, stmt: &SelectStatement) -> Result<PlanNode, RustqlError> {
        if stmt.from.is_empty() && stmt.from_function.is_none() {
            return Ok(self.plan_constant_select(stmt));
        }

        let db = self.db;
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

        let base_output_label = if stmt.joins.is_empty() {
            None
        } else {
            Some(stmt.from_alias.clone().unwrap_or_else(|| stmt.from.clone()))
        };

        let mut plan = if let Some((values, _alias, column_aliases)) = stmt.from_values.as_ref() {
            self.plan_values_access(values, column_aliases, base_filter.as_ref())
        } else if let Some(function) = stmt.from_function.as_ref() {
            self.plan_table_function_access(
                function,
                base_output_label.clone(),
                base_filter.as_ref(),
            )?
        } else {
            let base_table = db
                .get_table(&stmt.from)
                .ok_or_else(|| RustqlError::TableNotFound(stmt.from.clone()))?;
            let base_stats = self.collect_table_stats(&stmt.from, base_table, db);
            self.plan_table_access(
                &stmt.from,
                base_table,
                &base_stats,
                base_output_label,
                base_filter.as_ref(),
                db,
            )?
        };

        if !stmt.joins.is_empty() {
            plan = self.plan_joins(plan, stmt, db, remaining_predicates)?;
        } else if !remaining_predicates.is_empty()
            && let Some(filter_expr) = self.combine_conjuncts(remaining_predicates)
        {
            plan = self.plan_filter(plan, filter_expr);
        }

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

        if let Some(ref group_by) = stmt.group_by {
            plan = self.plan_aggregate(
                plan,
                self.aggregate_output_group_by(group_by),
                self.expand_grouping_sets(group_by),
                aggregates.clone(),
                stmt.having.clone(),
            );
        } else if !aggregates.is_empty() {
            plan = self.plan_aggregate(plan, Vec::new(), None, aggregates, stmt.having.clone());
        }

        let planned_order_by = stmt
            .order_by
            .as_ref()
            .map(|order_by| self.resolve_order_by_aliases(stmt, order_by));

        if let Some(ref order_by) = planned_order_by {
            plan = self.plan_sort(plan, order_by.clone());
        }

        if let Some(ref distinct_on) = stmt.distinct_on {
            plan = self.plan_distinct_on(plan, distinct_on.clone());
        }

        let (limit, with_ties) = match stmt.fetch.as_ref() {
            Some(fetch) => (fetch.count, fetch.with_ties),
            None => (stmt.limit.unwrap_or(usize::MAX), false),
        };
        let offset = stmt.offset.unwrap_or(0);
        if limit != usize::MAX || offset != 0 {
            plan = self.plan_limit(
                plan,
                limit,
                offset,
                with_ties,
                planned_order_by.clone().unwrap_or_default(),
            );
        }

        Ok(plan)
    }

    fn plan_constant_select(&self, stmt: &SelectStatement) -> PlanNode {
        let mut plan = PlanNode::OneRow {
            cost: 0.01,
            rows: 1,
        };

        if let Some(ref where_clause) = stmt.where_clause {
            plan = self.plan_filter(plan, where_clause.clone());
        }

        let (limit, with_ties) = match stmt.fetch.as_ref() {
            Some(fetch) => (fetch.count, fetch.with_ties),
            None => (stmt.limit.unwrap_or(usize::MAX), false),
        };
        let offset = stmt.offset.unwrap_or(0);
        if limit != usize::MAX || offset != 0 {
            plan = self.plan_limit(
                plan,
                limit,
                offset,
                with_ties,
                stmt.order_by.clone().unwrap_or_default(),
            );
        }

        plan
    }

    fn plan_values_access(
        &self,
        values: &[Vec<Expression>],
        column_aliases: &[String],
        where_clause: Option<&Expression>,
    ) -> PlanNode {
        let width = values.first().map_or(0, Vec::len);
        let columns: Vec<String> = (0..width)
            .map(|idx| {
                column_aliases
                    .get(idx)
                    .cloned()
                    .unwrap_or_else(|| format!("column{}", idx + 1))
            })
            .collect();
        let input_rows = values.len();
        let rows = if let Some(condition) = where_clause {
            (input_rows as f64 * self.estimate_selectivity(condition, input_rows)) as usize
        } else {
            input_rows
        };
        let cost = input_rows as f64 * FUNCTION_SCAN_ROW_COST;

        PlanNode::ValuesScan {
            values: values.to_vec(),
            columns,
            filter: where_clause.cloned(),
            cost,
            rows,
        }
    }

    fn plan_table_access(
        &self,
        table_name: &str,
        _table: &Table,
        stats: &TableStats,
        output_label: Option<String>,
        where_clause: Option<&Expression>,
        db: &dyn DatabaseCatalog,
    ) -> Result<PlanNode, RustqlError> {
        if let Some(where_expr) = where_clause
            && let Some(index_usage) = self.find_best_index(table_name, where_expr, db)
        {
            let estimated_rows = self.estimate_index_selectivity(&index_usage, db, stats);
            let cost = self.estimate_index_scan_cost(stats.row_count, estimated_rows);

            return Ok(PlanNode::IndexScan {
                table: table_name.to_string(),
                index: index_usage.index_name().to_string(),
                output_label: output_label.clone(),
                filter: Some((*where_expr).clone()),
                cost,
                rows: estimated_rows,
            });
        }

        let cost = self.estimate_seq_scan_cost(stats.row_count);
        let rows = stats.row_count;

        Ok(PlanNode::SeqScan {
            table: table_name.to_string(),
            output_label,
            filter: where_clause.cloned(),
            cost,
            rows,
        })
    }

    fn plan_table_function_access(
        &self,
        function: &TableFunction,
        output_label: Option<String>,
        where_clause: Option<&Expression>,
    ) -> Result<PlanNode, RustqlError> {
        match function.name.as_str() {
            "generate_series" => {
                let input_rows = self
                    .estimate_generate_series_rows(function)
                    .unwrap_or(DEFAULT_GENERATE_SERIES_ROWS);
                let rows = if let Some(condition) = where_clause {
                    (input_rows as f64 * self.estimate_selectivity(condition, input_rows)) as usize
                } else {
                    input_rows
                };
                let cost = input_rows as f64 * FUNCTION_SCAN_ROW_COST;

                Ok(PlanNode::FunctionScan {
                    function: function.clone(),
                    output_label,
                    filter: where_clause.cloned(),
                    cost,
                    rows,
                })
            }
            other => Err(RustqlError::Internal(format!(
                "Unsupported table function in planner: {}",
                other
            ))),
        }
    }

    fn plan_joins(
        &self,
        left_plan: PlanNode,
        stmt: &SelectStatement,
        db: &dyn DatabaseCatalog,
        mut remaining_predicates: Vec<Expression>,
    ) -> Result<PlanNode, RustqlError> {
        let mut current_plan = left_plan;
        let base_label = stmt.from_alias.clone().unwrap_or_else(|| stmt.from.clone());
        let mut left_columns = self
            .db
            .get_table(&stmt.from)
            .map(|table| self.qualified_column_definitions(&table.columns, &base_label))
            .unwrap_or_default();

        for join in stmt.joins.clone() {
            if join.lateral {
                let (subquery, alias) = join.subquery.clone().ok_or_else(|| {
                    RustqlError::Internal(
                        "Planner expected LATERAL join to reference a subquery".to_string(),
                    )
                })?;
                let right_columns = self.infer_select_output_columns(&subquery)?;
                let join_condition =
                    self.join_condition_for_plan(&join, &left_columns, &right_columns, &alias);

                current_plan = self.plan_lateral_join(
                    current_plan,
                    *subquery,
                    alias.clone(),
                    self.qualified_column_names(&right_columns, &alias),
                    join.join_type.clone(),
                    join_condition,
                );
                left_columns.extend(self.qualified_column_definitions(&right_columns, &alias));
                continue;
            }

            let right_table = db
                .get_table(&join.table)
                .ok_or_else(|| RustqlError::TableNotFound(join.table.clone()))?;
            let right_label = join
                .table_alias
                .clone()
                .unwrap_or_else(|| join.table.clone());

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
                Some(right_label.clone()),
                right_filter.as_ref(),
                db,
            )?;

            let join_condition = self.join_condition_for_plan(
                &join,
                &left_columns,
                &right_table.columns,
                &right_label,
            );
            let join_plan = if stmt.joins.len() == 1
                && matches!(join.join_type, JoinType::Inner)
                && join.using_columns.is_none()
                && !matches!(join.join_type, JoinType::Natural)
                && self.is_equality_join(&join_condition)
            {
                self.plan_hash_join(current_plan, right_plan, join_condition.clone())
            } else {
                self.plan_nested_loop_join(
                    current_plan,
                    right_plan,
                    join.join_type.clone(),
                    join_condition,
                )
            };

            current_plan = join_plan;
            left_columns
                .extend(self.qualified_column_definitions(&right_table.columns, &right_label));
        }

        if !remaining_predicates.is_empty()
            && let Some(filter_expr) = self.combine_conjuncts(remaining_predicates)
        {
            current_plan = self.plan_filter(current_plan, filter_expr);
        }

        Ok(current_plan)
    }

    fn join_condition_for_plan(
        &self,
        join: &Join,
        left_columns: &[ColumnDefinition],
        right_columns: &[ColumnDefinition],
        right_label: &str,
    ) -> Expression {
        if let Some(ref on_expr) = join.on {
            return on_expr.clone();
        }

        let column_pairs: Vec<(String, String)> = if let Some(ref using_columns) =
            join.using_columns
        {
            using_columns
                .iter()
                .filter_map(|column| {
                    left_columns
                        .iter()
                        .find(|left| self.unqualified_column_name(&left.name) == column.as_str())
                        .and_then(|left_col| {
                            right_columns
                                .iter()
                                .find(|right| right.name == *column)
                                .map(|right_col| {
                                    (
                                        left_col.name.clone(),
                                        format!("{}.{}", right_label, right_col.name),
                                    )
                                })
                        })
                })
                .collect()
        } else if matches!(join.join_type, JoinType::Natural) {
            left_columns
                .iter()
                .filter_map(|left_col| {
                    right_columns
                        .iter()
                        .find(|right| right.name == self.unqualified_column_name(&left_col.name))
                        .map(|right_col| {
                            (
                                left_col.name.clone(),
                                format!("{}.{}", right_label, right_col.name),
                            )
                        })
                })
                .collect()
        } else {
            Vec::new()
        };

        let mut conditions: Vec<Expression> = column_pairs
            .into_iter()
            .map(|(left, right)| Expression::BinaryOp {
                left: Box::new(Expression::Column(left)),
                op: BinaryOperator::Equal,
                right: Box::new(Expression::Column(right)),
            })
            .collect();

        if conditions.is_empty() {
            Expression::Value(Value::Boolean(true))
        } else {
            let mut condition = conditions.remove(0);
            for next in conditions {
                condition = Expression::BinaryOp {
                    left: Box::new(condition),
                    op: BinaryOperator::And,
                    right: Box::new(next),
                };
            }
            condition
        }
    }

    fn qualified_column_definitions(
        &self,
        columns: &[ColumnDefinition],
        label: &str,
    ) -> Vec<ColumnDefinition> {
        columns
            .iter()
            .map(|column| {
                let mut qualified = column.clone();
                qualified.name = format!("{}.{}", label, column.name);
                qualified
            })
            .collect()
    }

    fn qualified_column_names(&self, columns: &[ColumnDefinition], label: &str) -> Vec<String> {
        columns
            .iter()
            .map(|column| format!("{}.{}", label, column.name))
            .collect()
    }

    fn unqualified_column_name<'b>(&self, name: &'b str) -> &'b str {
        name.split('.').next_back().unwrap_or(name)
    }

    fn resolve_order_by_aliases(
        &self,
        stmt: &SelectStatement,
        order_by: &[OrderByExpr],
    ) -> Vec<OrderByExpr> {
        order_by
            .iter()
            .map(|item| OrderByExpr {
                expr: self.resolve_order_by_alias(stmt, &item.expr),
                asc: item.asc,
            })
            .collect()
    }

    fn resolve_order_by_alias(&self, stmt: &SelectStatement, expr: &Expression) -> Expression {
        if let Expression::Column(name) = expr
            && let Some(resolved) = self.select_alias_expression(stmt, name)
        {
            return resolved;
        }

        expr.clone()
    }

    fn select_alias_expression(&self, stmt: &SelectStatement, name: &str) -> Option<Expression> {
        stmt.columns.iter().find_map(|column| match column {
            Column::Named {
                name: column_name,
                alias: Some(alias),
            } if alias == name => Some(Expression::Column(column_name.clone())),
            Column::Expression {
                expr,
                alias: Some(alias),
            } if alias == name => Some(expr.clone()),
            _ => None,
        })
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
        join_type: JoinType,
        condition: Expression,
    ) -> PlanNode {
        let left_rows = self.estimate_rows(&left);
        let right_rows = self.estimate_rows(&right);
        let cost = self.estimate_nested_loop_join_cost(&left, &right);
        let estimated_output_rows = self.estimate_join_rows(left_rows, right_rows, &condition);

        PlanNode::NestedLoopJoin {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            condition,
            cost,
            rows: estimated_output_rows,
        }
    }

    fn plan_lateral_join(
        &self,
        left: PlanNode,
        subquery: SelectStatement,
        alias: String,
        right_columns: Vec<String>,
        join_type: JoinType,
        condition: Expression,
    ) -> PlanNode {
        let left_rows = self.estimate_rows(&left);
        let per_left_rows = self.estimate_lateral_rows(&subquery);
        let joined_rows = left_rows.saturating_mul(per_left_rows);
        let estimated_output_rows = if matches!(join_type, JoinType::Left | JoinType::Full) {
            joined_rows.max(left_rows)
        } else {
            joined_rows
        };
        let cost = self.estimate_cost(&left)
            + left_rows as f64 * (per_left_rows as f64 * LATERAL_ROW_COST + LATERAL_FIXED_COST);

        PlanNode::LateralJoin {
            left: Box::new(left),
            subquery: Box::new(subquery),
            alias,
            right_columns,
            join_type,
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
        let cost = input_rows as f64 * FILTER_ROW_COST;

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

    fn plan_limit(
        &self,
        input: PlanNode,
        limit: usize,
        offset: usize,
        with_ties: bool,
        order_by: Vec<OrderByExpr>,
    ) -> PlanNode {
        let input_rows = self.estimate_rows(&input);
        let output_rows = (input_rows.saturating_sub(offset)).min(limit);
        let visited_rows = if limit == usize::MAX {
            input_rows
        } else {
            offset.saturating_add(limit).min(input_rows)
        };
        let cost = self.estimate_cost(&input) + visited_rows as f64 * LIMIT_ROW_COST;

        PlanNode::Limit {
            input: Box::new(input),
            limit,
            offset,
            with_ties,
            order_by,
            cost,
            rows: output_rows,
        }
    }

    fn plan_distinct_on(&self, input: PlanNode, distinct_on: Vec<Expression>) -> PlanNode {
        let input_rows = self.estimate_rows(&input);
        let output_rows = if input_rows == 0 {
            0
        } else {
            (input_rows / DISTINCT_ON_ROW_REDUCTION_DIVISOR).max(1)
        };
        let cost = self.estimate_cost(&input) + input_rows as f64 * DISTINCT_ON_ROW_COST;

        PlanNode::DistinctOn {
            input: Box::new(input),
            distinct_on,
            cost,
            rows: output_rows,
        }
    }

    fn plan_aggregate(
        &self,
        input: PlanNode,
        group_by: Vec<Expression>,
        grouping_sets: Option<Vec<Vec<Expression>>>,
        aggregates: Vec<AggregateFunction>,
        having: Option<Expression>,
    ) -> PlanNode {
        let input_rows = self.estimate_rows(&input);
        let base_cost = self.estimate_cost(&input);

        let grouping_multiplier = grouping_sets
            .as_ref()
            .map(|sets| sets.len().max(1))
            .unwrap_or(1);
        let output_rows = ((input_rows as f64 * AGGREGATE_GROUP_OUTPUT_SELECTIVITY).max(1.0)
            as usize)
            * grouping_multiplier;
        let cost = self.estimate_aggregate_cost(input_rows, group_by.len(), aggregates.len());

        PlanNode::Aggregate {
            input: Box::new(input),
            group_by,
            grouping_sets,
            aggregates,
            having,
            cost: base_cost + cost,
            rows: output_rows,
        }
    }

    fn estimate_cost(&self, plan: &PlanNode) -> f64 {
        match plan {
            PlanNode::OneRow { cost, .. } => *cost,
            PlanNode::SeqScan { cost, .. } => *cost,
            PlanNode::IndexScan { cost, .. } => *cost,
            PlanNode::FunctionScan { cost, .. } => *cost,
            PlanNode::ValuesScan { cost, .. } => *cost,
            PlanNode::NestedLoopJoin { cost, .. } => *cost,
            PlanNode::HashJoin { cost, .. } => *cost,
            PlanNode::LateralJoin { cost, .. } => *cost,
            PlanNode::Filter { cost, .. } => *cost,
            PlanNode::Sort { cost, .. } => *cost,
            PlanNode::DistinctOn { cost, .. } => *cost,
            PlanNode::Limit { cost, .. } => *cost,
            PlanNode::Aggregate { cost, .. } => *cost,
        }
    }

    fn estimate_rows(&self, plan: &PlanNode) -> usize {
        match plan {
            PlanNode::OneRow { rows, .. } => *rows,
            PlanNode::SeqScan { rows, .. } => *rows,
            PlanNode::IndexScan { rows, .. } => *rows,
            PlanNode::FunctionScan { rows, .. } => *rows,
            PlanNode::ValuesScan { rows, .. } => *rows,
            PlanNode::NestedLoopJoin { rows, .. } => *rows,
            PlanNode::HashJoin { rows, .. } => *rows,
            PlanNode::LateralJoin { rows, .. } => *rows,
            PlanNode::Filter { rows, .. } => *rows,
            PlanNode::Sort { rows, .. } => *rows,
            PlanNode::DistinctOn { rows, .. } => *rows,
            PlanNode::Limit { rows, .. } => *rows,
            PlanNode::Aggregate { rows, .. } => *rows,
        }
    }

    fn estimate_seq_scan_cost(&self, row_count: usize) -> f64 {
        row_count as f64
    }

    fn estimate_index_scan_cost(&self, total_rows: usize, selected_rows: usize) -> f64 {
        (total_rows as f64).ln() * INDEX_SCAN_SEEK_COST_MULTIPLIER
            + selected_rows as f64 * INDEX_SCAN_ROW_COST
    }

    fn estimate_hash_join_cost(&self, left: &PlanNode, right: &PlanNode) -> f64 {
        let left_rows = self.estimate_rows(left);
        let right_rows = self.estimate_rows(right);
        let left_cost = self.estimate_cost(left);
        let right_cost = self.estimate_cost(right);

        let build_cost = left_rows.min(right_rows) as f64 * HASH_JOIN_BUILD_ROW_COST;
        let probe_cost = left_rows.max(right_rows) as f64 * HASH_JOIN_PROBE_ROW_COST;

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
        row_count as f64 * (row_count as f64).ln() * SORT_COMPLEXITY_COST
    }

    fn estimate_generate_series_rows(&self, function: &TableFunction) -> Option<usize> {
        let start = self.constant_integer(function.args.first()?)?;
        let stop = self.constant_integer(function.args.get(1)?)?;
        let step = if let Some(step_expr) = function.args.get(2) {
            self.constant_integer(step_expr)?
        } else if start <= stop {
            1
        } else {
            -1
        };

        if step == 0 {
            return None;
        }

        let span = if step > 0 {
            if start > stop {
                return Some(0);
            }
            stop.checked_sub(start)?
        } else {
            if start < stop {
                return Some(0);
            }
            start.checked_sub(stop)?
        };

        let step_width = step.checked_abs()?;
        let rows = span.checked_div(step_width)? + 1;
        usize::try_from(rows).ok()
    }

    fn constant_integer(&self, expr: &Expression) -> Option<i64> {
        match expr {
            Expression::Value(Value::Integer(value)) => Some(*value),
            Expression::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } => self.constant_integer(expr).map(|value| -value),
            _ => None,
        }
    }

    fn estimate_aggregate_cost(
        &self,
        input_rows: usize,
        group_by_cols: usize,
        agg_count: usize,
    ) -> f64 {
        input_rows as f64 * (1.0 + (group_by_cols + agg_count) as f64 * AGGREGATE_PER_STATE_COST)
    }

    fn estimate_selectivity(&self, condition: &Expression, total_rows: usize) -> f64 {
        match condition {
            Expression::BinaryOp { op, .. } => match op {
                BinaryOperator::Equal => SELECTIVITY_EQUAL,
                BinaryOperator::NotEqual => SELECTIVITY_NOT_EQUAL,
                BinaryOperator::LessThan | BinaryOperator::LessThanOrEqual => {
                    SELECTIVITY_ORDERED_COMPARISON
                }
                BinaryOperator::GreaterThan | BinaryOperator::GreaterThanOrEqual => {
                    SELECTIVITY_ORDERED_COMPARISON
                }
                BinaryOperator::And => SELECTIVITY_AND,
                BinaryOperator::Or => SELECTIVITY_OR,
                BinaryOperator::Like | BinaryOperator::ILike => SELECTIVITY_LIKE,
                BinaryOperator::Between => SELECTIVITY_BETWEEN,
                _ => SELECTIVITY_DEFAULT,
            },
            Expression::In { values, .. } => {
                (values.len() as f64 / total_rows.max(1) as f64).min(1.0)
            }
            Expression::IsNull { .. } => SELECTIVITY_IS_NULL,
            _ => SELECTIVITY_DEFAULT,
        }
    }

    fn estimate_join_rows(
        &self,
        left_rows: usize,
        right_rows: usize,
        condition: &Expression,
    ) -> usize {
        if self.is_equality_join(condition) {
            ((left_rows * right_rows) as f64 * SELECTIVITY_EQUAL_JOIN) as usize
        } else {
            ((left_rows * right_rows) as f64 * SELECTIVITY_NON_EQUAL_JOIN) as usize
        }
    }

    fn estimate_index_selectivity(
        &self,
        index_usage: &IndexUsage,
        db: &dyn DatabaseCatalog,
        stats: &TableStats,
    ) -> usize {
        match index_usage {
            IndexUsage::Equality { index_name, value } => db
                .get_index(index_name)
                .and_then(|index| index.entries.get(value))
                .map(|rows| rows.len())
                .unwrap_or(0),
            IndexUsage::In { index_name, values } => db
                .get_index(index_name)
                .map(|index| {
                    values
                        .iter()
                        .map(|value| index.entries.get(value).map(|rows| rows.len()).unwrap_or(0))
                        .sum()
                })
                .unwrap_or(0),
            IndexUsage::RangeGreater { .. }
            | IndexUsage::RangeLess { .. }
            | IndexUsage::RangeBetween { .. } => {
                (stats.row_count as f64 * INDEX_RANGE_SELECTIVITY) as usize
            }
            IndexUsage::CompositePrefix { index_name, values } => db
                .get_composite_index(index_name)
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

    fn collect_table_stats(
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

    fn find_best_index(
        &self,
        table_name: &str,
        where_expr: &Expression,
        db: &dyn DatabaseCatalog,
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

    fn aggregate_output_group_by(&self, group_by: &GroupByClause) -> Vec<Expression> {
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

    fn expand_grouping_sets(&self, group_by: &GroupByClause) -> Option<Vec<Vec<Expression>>> {
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

    fn estimate_lateral_rows(&self, stmt: &SelectStatement) -> usize {
        if stmt.group_by.is_some()
            || stmt
                .columns
                .iter()
                .any(|column| matches!(column, Column::Function(_)))
        {
            return 1;
        }

        stmt.fetch
            .as_ref()
            .map(|fetch| fetch.count.max(1))
            .or(stmt.limit.map(|limit| limit.max(1)))
            .unwrap_or(DEFAULT_LATERAL_ROWS)
    }

    fn infer_select_output_columns(
        &self,
        stmt: &SelectStatement,
    ) -> Result<Vec<ColumnDefinition>, RustqlError> {
        let source_columns = self.infer_select_source_columns(stmt)?;

        if matches!(stmt.columns.first(), Some(Column::All)) {
            return Ok(source_columns);
        }

        stmt.columns
            .iter()
            .map(|column| {
                let name = match column {
                    Column::Named { name, alias } => alias.clone().unwrap_or_else(|| name.clone()),
                    Column::Expression { alias, .. } => {
                        alias.clone().unwrap_or_else(|| "<expression>".to_string())
                    }
                    Column::Function(agg) => format_aggregate_header(agg),
                    Column::Subquery(_) => "<subquery>".to_string(),
                    Column::All => {
                        return Err(RustqlError::Internal(
                            "Wildcard projection must be expanded before output inference"
                                .to_string(),
                        ));
                    }
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

                Ok(ColumnDefinition {
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
                })
            })
            .collect()
    }

    fn infer_select_source_columns(
        &self,
        stmt: &SelectStatement,
    ) -> Result<Vec<ColumnDefinition>, RustqlError> {
        let mut columns = if let Some(function) = stmt.from_function.as_ref() {
            match function.name.as_str() {
                "generate_series" => vec![ColumnDefinition {
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
                }],
                other => {
                    return Err(RustqlError::Internal(format!(
                        "Unsupported table function in planner: {}",
                        other
                    )));
                }
            }
        } else if let Some((subquery, _)) = stmt.from_subquery.as_ref() {
            self.infer_select_output_columns(subquery)?
        } else if let Some((rows, _, aliases)) = stmt.from_values.as_ref() {
            let width = rows.first().map(|row| row.len()).unwrap_or(0);
            (0..width)
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
                .collect()
        } else if stmt.from.is_empty() {
            Vec::new()
        } else if let Some(table) = self.db.get_table(&stmt.from) {
            table.columns.clone()
        } else if let Some(cte) = stmt.ctes.iter().find(|cte| cte.name == stmt.from) {
            self.infer_select_output_columns(&cte.query)?
        } else if let Some(view) = self.db.get_view(&stmt.from) {
            self.infer_select_output_columns(&self.parse_view_query(&view.query_sql)?)?
        } else {
            return Err(RustqlError::TableNotFound(stmt.from.clone()));
        };

        for join in &stmt.joins {
            if let Some((subquery, _)) = join.subquery.as_ref() {
                columns.extend(self.infer_select_output_columns(subquery)?);
            } else if let Some(table) = self.db.get_table(&join.table) {
                columns.extend(table.columns.clone());
            } else if let Some(view) = self.db.get_view(&join.table) {
                columns.extend(
                    self.infer_select_output_columns(&self.parse_view_query(&view.query_sql)?)?,
                );
            } else {
                return Err(RustqlError::TableNotFound(join.table.clone()));
            }
        }

        Ok(columns)
    }

    fn parse_view_query(&self, query_sql: &str) -> Result<SelectStatement, RustqlError> {
        let tokens = crate::lexer::tokenize(query_sql)?;
        match crate::parser::parse(tokens)? {
            Statement::Select(select) => Ok(select),
            _ => Err(RustqlError::Internal(
                "View definition is not a SELECT statement".to_string(),
            )),
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
            PlanNode::OneRow { cost, rows } => {
                writeln!(f, "{}Result", indent_str)?;
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)
            }
            PlanNode::SeqScan {
                table,
                filter,
                cost,
                rows,
                ..
            } => {
                writeln!(f, "{}Seq Scan on {}", indent_str, table)?;
                if filter.is_some() {
                    writeln!(f, "{}  Filter: [WHERE clause]", indent_str)?;
                }
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)
            }
            PlanNode::FunctionScan {
                function,
                filter,
                cost,
                rows,
                ..
            } => {
                writeln!(f, "{}Function Scan on {}", indent_str, function.name)?;
                if filter.is_some() {
                    writeln!(f, "{}  Filter: [WHERE clause]", indent_str)?;
                }
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)
            }
            PlanNode::ValuesScan {
                columns,
                filter,
                cost,
                rows,
                ..
            } => {
                writeln!(f, "{}Values Scan", indent_str)?;
                if !columns.is_empty() {
                    writeln!(f, "{}  Output: {}", indent_str, columns.join(", "))?;
                }
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
                ..
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
                join_type,
                condition: _,
                cost,
                rows,
            } => {
                let join_label = match join_type {
                    JoinType::Left => "Nested Loop Left Join",
                    JoinType::Right => "Nested Loop Right Join",
                    JoinType::Full => "Nested Loop Full Join",
                    JoinType::Natural => "Nested Loop Natural Join",
                    JoinType::Cross => "Nested Loop Cross Join",
                    _ => "Nested Loop Join",
                };
                writeln!(f, "{}{}", indent_str, join_label)?;
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
            PlanNode::LateralJoin {
                left,
                alias,
                right_columns,
                join_type,
                condition,
                cost,
                rows,
                ..
            } => {
                let join_label = match join_type {
                    JoinType::Left => "Lateral Left Join",
                    JoinType::Right => "Lateral Right Join",
                    JoinType::Full => "Lateral Full Join",
                    JoinType::Cross => "Lateral Cross Join",
                    JoinType::Natural => "Lateral Natural Join",
                    JoinType::Inner => "Lateral Join",
                };
                writeln!(f, "{}{}", indent_str, join_label)?;
                writeln!(f, "{}  Alias: {}", indent_str, alias)?;
                if !matches!(condition, Expression::Value(Value::Boolean(true))) {
                    writeln!(f, "{}  Condition: {:?}", indent_str, condition)?;
                }
                if !right_columns.is_empty() {
                    writeln!(f, "{}  Output: {}", indent_str, right_columns.join(", "))?;
                }
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
                left.fmt_with_indent(f, indent + 1)
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
            PlanNode::DistinctOn {
                input,
                distinct_on,
                cost,
                rows,
            } => {
                let distinct_strs: Vec<String> = distinct_on
                    .iter()
                    .map(|expr| format!("{:?}", expr))
                    .collect();
                writeln!(
                    f,
                    "{}Distinct On ({})",
                    indent_str,
                    distinct_strs.join(", ")
                )?;
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
                input.fmt_with_indent(f, indent + 1)
            }
            PlanNode::Limit {
                input,
                limit,
                offset,
                with_ties,
                order_by,
                cost,
                rows,
            } => {
                let with_ties_suffix = if *with_ties && !order_by.is_empty() {
                    " With Ties"
                } else {
                    ""
                };
                writeln!(
                    f,
                    "{}Limit: {} Offset: {}{}",
                    indent_str, limit, offset, with_ties_suffix
                )?;
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
                input.fmt_with_indent(f, indent + 1)
            }
            PlanNode::Aggregate {
                input,
                group_by,
                grouping_sets,
                aggregates: _,
                having: _,
                cost,
                rows,
            } => {
                if let Some(sets) = grouping_sets {
                    let grouping_set_strs: Vec<String> = sets
                        .iter()
                        .map(|set| {
                            if set.is_empty() {
                                "()".to_string()
                            } else {
                                format!(
                                    "({})",
                                    set.iter()
                                        .map(|expr| format!("{:?}", expr))
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                )
                            }
                        })
                        .collect();
                    writeln!(
                        f,
                        "{}Aggregate (Grouping Sets: {})",
                        indent_str,
                        grouping_set_strs.join(", ")
                    )?;
                } else {
                    let group_by_strs: Vec<String> =
                        group_by.iter().map(|e| format!("{:?}", e)).collect();
                    writeln!(
                        f,
                        "{}Aggregate (Group By: {})",
                        indent_str,
                        group_by_strs.join(", ")
                    )?;
                }
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
                input.fmt_with_indent(f, indent + 1)
            }
        }
    }
}

pub fn explain_query(
    db: &dyn DatabaseCatalog,
    stmt: &SelectStatement,
) -> Result<String, RustqlError> {
    let planner = QueryPlanner::new(db);
    let plan = planner.plan_select(stmt)?;
    Ok(format!("Query Plan:\n{}", plan))
}

pub fn plan_query(
    db: &dyn DatabaseCatalog,
    stmt: &SelectStatement,
) -> Result<PlanNode, RustqlError> {
    QueryPlanner::new(db).plan_select(stmt)
}

pub(crate) fn infer_select_output_columns(
    db: &dyn DatabaseCatalog,
    stmt: &SelectStatement,
) -> Result<Vec<ColumnDefinition>, RustqlError> {
    QueryPlanner::new(db).infer_select_output_columns(stmt)
}
