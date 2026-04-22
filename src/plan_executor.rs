use crate::ast::*;
use crate::database::{DatabaseCatalog, RowId, ScopedDatabase};
use crate::error::RustqlError;
use crate::executor::aggregate::{DEFAULT_PERCENTILE_FRACTION, format_aggregate_header};
use crate::executor::expr::{
    apply_arithmetic, compare_values, compare_values_same_type, evaluate_expression,
    evaluate_value_expression_with_db, format_value,
};
use crate::planner::{self, PlanNode};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashSet};

const MAX_RECURSIVE_CTE_ITERATIONS: usize = 1000;

mod aggregate;
mod filter;
mod joins;
mod order;
mod projection;
mod scans;
mod set_ops;
mod subquery;
mod support;

pub(crate) use subquery::{
    evaluate_planned_scalar_subquery_with_outer, evaluate_planned_subquery_exists_with_outer,
    evaluate_planned_subquery_values_with_outer,
};
use subquery::{execute_planned_select, lateral_subquery_with_outer_scope};
use support::*;

#[derive(Debug)]
pub struct ExecutionResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

pub struct PlanExecutor<'a> {
    db: &'a dyn DatabaseCatalog,
}

impl<'a> PlanExecutor<'a> {
    pub fn new(db: &'a dyn DatabaseCatalog) -> Self {
        PlanExecutor { db }
    }

    pub fn execute(
        &self,
        plan: &PlanNode,
        select_stmt: &SelectStatement,
    ) -> Result<ExecutionResult, RustqlError> {
        let result = self.execute_plan_node(plan)?;

        if matches!(plan, PlanNode::SetOperation { .. }) {
            return Ok(result);
        }

        let projected = self.apply_projection(&result, select_stmt)?;
        let distincted = if select_stmt.distinct {
            self.apply_distinct(projected)?
        } else {
            projected
        };

        Ok(distincted)
    }

    fn execute_plan_node(&self, plan: &PlanNode) -> Result<ExecutionResult, RustqlError> {
        match plan {
            PlanNode::OneRow { .. } => Ok(ExecutionResult {
                columns: Vec::new(),
                rows: vec![Vec::new()],
            }),
            PlanNode::SeqScan {
                table,
                output_label,
                filter,
                ..
            } => self.execute_seq_scan(table, output_label.as_deref(), filter.as_ref()),
            PlanNode::IndexScan {
                table,
                index,
                output_label,
                filter,
                ..
            } => self.execute_index_scan(table, index, output_label.as_deref(), filter.as_ref()),
            PlanNode::FunctionScan {
                function,
                output_label,
                filter,
                ..
            } => self.execute_function_scan(function, output_label.as_deref(), filter.as_ref()),
            PlanNode::ValuesScan {
                values,
                columns,
                filter,
                ..
            } => self.execute_values_scan(values, columns, filter.as_ref()),
            PlanNode::SubqueryScan {
                input,
                select,
                output_label,
                ..
            }
            | PlanNode::ViewScan {
                input,
                select,
                output_label,
                ..
            }
            | PlanNode::CteScan {
                input,
                select,
                output_label,
                ..
            } => self.execute_source_scan(input, select, output_label.as_deref()),
            PlanNode::RecursiveCteScan {
                cte,
                base,
                base_select,
                recursive_select,
                union_all,
                output_label,
                ..
            } => self.execute_recursive_cte_scan(
                cte,
                base,
                base_select,
                recursive_select,
                *union_all,
                output_label.as_deref(),
            ),
            PlanNode::Filter {
                input, condition, ..
            } => {
                let input_result = self.execute_plan_node(input)?;
                self.execute_filter(input_result, condition)
            }
            PlanNode::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
                ..
            } => {
                let left_result = self.execute_plan_node(left)?;
                let right_result = self.execute_plan_node(right)?;
                self.execute_nested_loop_join(left_result, right_result, join_type, condition)
            }
            PlanNode::HashJoin {
                left,
                right,
                condition,
                ..
            } => {
                let left_result = self.execute_plan_node(left)?;
                let right_result = self.execute_plan_node(right)?;
                self.execute_hash_join(left_result, right_result, condition)
            }
            PlanNode::LateralJoin {
                left,
                subquery,
                alias,
                right_columns,
                join_type,
                condition,
                ..
            } => {
                let left_result = self.execute_plan_node(left)?;
                self.execute_lateral_join(
                    left_result,
                    subquery,
                    alias,
                    right_columns,
                    join_type,
                    condition,
                )
            }
            PlanNode::Sort {
                input, order_by, ..
            } => {
                let input_result = self.execute_plan_node(input)?;
                self.execute_sort(input_result, order_by)
            }
            PlanNode::DistinctOn {
                input, distinct_on, ..
            } => {
                let input_result = self.execute_plan_node(input)?;
                self.execute_distinct_on(input_result, distinct_on)
            }
            PlanNode::Limit {
                input,
                limit,
                offset,
                with_ties,
                order_by,
                ..
            } => {
                let input_result = self.execute_plan_node(input)?;
                self.execute_limit(input_result, *limit, *offset, *with_ties, order_by)
            }
            PlanNode::Aggregate {
                input,
                group_by,
                grouping_sets,
                aggregates,
                having,
                ..
            } => {
                let input_result = self.execute_plan_node(input)?;
                self.execute_aggregate(
                    input_result,
                    group_by,
                    grouping_sets.as_deref(),
                    aggregates,
                    having.as_ref(),
                )
            }
            PlanNode::SetOperation {
                left,
                right,
                left_select,
                right_select,
                op,
                ..
            } => {
                let left_result = self.execute(left, left_select)?;
                let right_result = self.execute(right, right_select)?;
                self.execute_set_operation(left_result, right_result, op)
            }
        }
    }
}

impl<'a> PlanExecutor<'a> {
    pub(super) fn evaluate_expression(
        &self,
        expr: &Expression,
        columns: &[ColumnDefinition],
        row: &[Value],
    ) -> Result<bool, RustqlError> {
        evaluate_expression(Some(self.db), expr, columns, row)
    }

    pub(super) fn evaluate_value_expression(
        &self,
        expr: &Expression,
        columns: &[ColumnDefinition],
        row: &[Value],
    ) -> Result<Value, RustqlError> {
        evaluate_value_expression_with_db(expr, columns, row, Some(self.db))
    }
}
