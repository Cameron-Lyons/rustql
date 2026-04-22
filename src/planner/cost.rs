use super::*;

impl<'a> QueryPlanner<'a> {
    pub(super) fn estimate_cost(&self, plan: &PlanNode) -> f64 {
        match plan {
            PlanNode::OneRow { cost, .. } => *cost,
            PlanNode::SeqScan { cost, .. } => *cost,
            PlanNode::IndexScan { cost, .. } => *cost,
            PlanNode::FunctionScan { cost, .. } => *cost,
            PlanNode::ValuesScan { cost, .. } => *cost,
            PlanNode::SubqueryScan { cost, .. } => *cost,
            PlanNode::ViewScan { cost, .. } => *cost,
            PlanNode::CteScan { cost, .. } => *cost,
            PlanNode::RecursiveCteScan { cost, .. } => *cost,
            PlanNode::NestedLoopJoin { cost, .. } => *cost,
            PlanNode::HashJoin { cost, .. } => *cost,
            PlanNode::LateralJoin { cost, .. } => *cost,
            PlanNode::Filter { cost, .. } => *cost,
            PlanNode::Sort { cost, .. } => *cost,
            PlanNode::DistinctOn { cost, .. } => *cost,
            PlanNode::Limit { cost, .. } => *cost,
            PlanNode::Aggregate { cost, .. } => *cost,
            PlanNode::SetOperation { cost, .. } => *cost,
        }
    }

    pub(super) fn estimate_rows(&self, plan: &PlanNode) -> usize {
        match plan {
            PlanNode::OneRow { rows, .. } => *rows,
            PlanNode::SeqScan { rows, .. } => *rows,
            PlanNode::IndexScan { rows, .. } => *rows,
            PlanNode::FunctionScan { rows, .. } => *rows,
            PlanNode::ValuesScan { rows, .. } => *rows,
            PlanNode::SubqueryScan { rows, .. } => *rows,
            PlanNode::ViewScan { rows, .. } => *rows,
            PlanNode::CteScan { rows, .. } => *rows,
            PlanNode::RecursiveCteScan { rows, .. } => *rows,
            PlanNode::NestedLoopJoin { rows, .. } => *rows,
            PlanNode::HashJoin { rows, .. } => *rows,
            PlanNode::LateralJoin { rows, .. } => *rows,
            PlanNode::Filter { rows, .. } => *rows,
            PlanNode::Sort { rows, .. } => *rows,
            PlanNode::DistinctOn { rows, .. } => *rows,
            PlanNode::Limit { rows, .. } => *rows,
            PlanNode::Aggregate { rows, .. } => *rows,
            PlanNode::SetOperation { rows, .. } => *rows,
        }
    }

    pub(super) fn estimate_seq_scan_cost(&self, row_count: usize) -> f64 {
        row_count as f64
    }

    pub(super) fn estimate_index_scan_cost(&self, total_rows: usize, selected_rows: usize) -> f64 {
        (total_rows as f64).ln() * INDEX_SCAN_SEEK_COST_MULTIPLIER
            + selected_rows as f64 * INDEX_SCAN_ROW_COST
    }

    pub(super) fn estimate_hash_join_cost(&self, left: &PlanNode, right: &PlanNode) -> f64 {
        let left_rows = self.estimate_rows(left);
        let right_rows = self.estimate_rows(right);
        let left_cost = self.estimate_cost(left);
        let right_cost = self.estimate_cost(right);

        let build_cost = left_rows.min(right_rows) as f64 * HASH_JOIN_BUILD_ROW_COST;
        let probe_cost = left_rows.max(right_rows) as f64 * HASH_JOIN_PROBE_ROW_COST;

        left_cost + right_cost + build_cost + probe_cost
    }

    pub(super) fn estimate_nested_loop_join_cost(&self, left: &PlanNode, right: &PlanNode) -> f64 {
        let left_rows = self.estimate_rows(left);
        let right_rows = self.estimate_rows(right);
        let left_cost = self.estimate_cost(left);
        let right_cost = self.estimate_cost(right);

        left_cost + right_cost + (left_rows * right_rows) as f64
    }

    pub(super) fn estimate_sort_cost(&self, row_count: usize) -> f64 {
        row_count as f64 * (row_count as f64).ln() * SORT_COMPLEXITY_COST
    }

    pub(super) fn estimate_generate_series_rows(&self, function: &TableFunction) -> Option<usize> {
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

    pub(super) fn constant_integer(&self, expr: &Expression) -> Option<i64> {
        match expr {
            Expression::Value(Value::Integer(value)) => Some(*value),
            Expression::UnaryOp {
                op: UnaryOperator::Minus,
                expr,
            } => self.constant_integer(expr).map(|value| -value),
            _ => None,
        }
    }

    pub(super) fn estimate_aggregate_cost(
        &self,
        input_rows: usize,
        group_by_cols: usize,
        agg_count: usize,
    ) -> f64 {
        input_rows as f64 * (1.0 + (group_by_cols + agg_count) as f64 * AGGREGATE_PER_STATE_COST)
    }

    pub(super) fn estimate_selectivity(&self, condition: &Expression, total_rows: usize) -> f64 {
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

    pub(super) fn estimate_join_rows(
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

    pub(super) fn estimate_index_selectivity(
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
}
