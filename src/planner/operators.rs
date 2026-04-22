use super::*;

impl<'a> QueryPlanner<'a> {
    pub(super) fn plan_constant_select(&self, stmt: &SelectStatement) -> PlanNode {
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

    pub(super) fn plan_filter(&self, input: PlanNode, condition: Expression) -> PlanNode {
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

    pub(super) fn plan_sort(&self, input: PlanNode, order_by: Vec<OrderByExpr>) -> PlanNode {
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

    pub(super) fn plan_limit(
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

    pub(super) fn plan_distinct_on(
        &self,
        input: PlanNode,
        distinct_on: Vec<Expression>,
    ) -> PlanNode {
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

    pub(super) fn plan_aggregate(
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
}
