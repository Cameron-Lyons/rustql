use crate::ast::*;
use crate::database::{DatabaseCatalog, Table};
use crate::error::RustqlError;
use crate::executor::aggregate::format_aggregate_header;
use crate::executor::ddl::{IndexUsage, find_index_usage};
use crate::executor::expr::compare_values_same_type;
use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet};

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
const LATERAL_OUTER_TABLE_PREFIX: &str = "__lateral_outer_";

mod access;
mod cost;
mod expressions;
mod joins;
mod operators;
mod order;
mod output;
mod plan_node;
mod set_ops;
mod stats;

pub use plan_node::PlanNode;
#[allow(unused_imports)]
pub use stats::{ColumnStats, TableStats};

pub struct QueryPlanner<'a> {
    db: &'a dyn DatabaseCatalog,
    ctes: Vec<Cte>,
}

impl<'a> QueryPlanner<'a> {
    pub fn new(db: &'a dyn DatabaseCatalog) -> Self {
        QueryPlanner {
            db,
            ctes: Vec::new(),
        }
    }

    pub fn plan_select(&self, stmt: &SelectStatement) -> Result<PlanNode, RustqlError> {
        if stmt.ctes.is_empty() {
            return self.plan_select_body(stmt);
        }

        let mut scoped_ctes = self.ctes.clone();
        scoped_ctes.extend(stmt.ctes.clone());
        let scoped_planner = QueryPlanner {
            db: self.db,
            ctes: scoped_ctes,
        };
        let mut stmt_without_ctes = stmt.clone();
        stmt_without_ctes.ctes.clear();
        scoped_planner.plan_select_body(&stmt_without_ctes)
    }

    fn plan_select_body(&self, stmt: &SelectStatement) -> Result<PlanNode, RustqlError> {
        if let Some((ref set_op, ref right_stmt)) = stmt.set_op {
            return self.plan_set_operation(stmt, right_stmt, set_op);
        }

        if stmt.from.is_empty() && stmt.from_function.is_none() {
            return Ok(self.plan_constant_select(stmt));
        }

        let db = self.db;
        let mut join_tables: HashSet<String> = HashSet::new();
        for join in &stmt.joins {
            join_tables.insert(join.table.clone());
            if let Some(alias) = join.table_alias.as_ref() {
                join_tables.insert(alias.clone());
            }
        }
        let mut base_tables: HashSet<String> = HashSet::new();
        if !stmt.from.is_empty() {
            base_tables.insert(stmt.from.clone());
        }
        if let Some(alias) = stmt.from_alias.as_ref() {
            base_tables.insert(alias.clone());
        }
        if let Some((_, alias, _)) = stmt.from_values.as_ref() {
            base_tables.insert(alias.clone());
        }
        if let Some(function) = stmt.from_function.as_ref()
            && let Some(alias) = function.alias.as_ref()
        {
            base_tables.insert(alias.clone());
        }
        let base_column_names = self.base_column_names(stmt)?;

        let (base_filter, remaining_predicates) = if let Some(ref where_expr) = stmt.where_clause {
            let conjuncts = self.extract_conjuncts(where_expr);
            let mut base_preds = Vec::new();
            let mut rest = Vec::new();

            for conj in conjuncts {
                let refs = self.referenced_tables(&conj);
                let pushable_unqualified = refs.is_empty()
                    && (stmt.joins.is_empty()
                        || self.unqualified_columns_resolve_to_base(&conj, &base_column_names));
                let pushable_base_refs = !refs.is_empty()
                    && refs.iter().all(|reference| base_tables.contains(reference))
                    && self.unqualified_columns_resolve_to_base(&conj, &base_column_names);
                if pushable_unqualified || pushable_base_refs {
                    base_preds.push(conj);
                } else {
                    rest.push(conj);
                }
            }

            (self.combine_conjuncts(base_preds), rest)
        } else {
            (None, Vec::new())
        };

        let base_output_label =
            if stmt.joins.is_empty() || stmt.from.starts_with(LATERAL_OUTER_TABLE_PREFIX) {
                None
            } else {
                Some(stmt.from_alias.clone().unwrap_or_else(|| stmt.from.clone()))
            };

        let mut plan = self.plan_base_source(stmt, base_output_label, base_filter.as_ref(), db)?;

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
