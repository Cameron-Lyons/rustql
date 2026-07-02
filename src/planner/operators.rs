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

        let output_rows =
            estimate_aggregate_output_rows(input_rows, &group_by, grouping_sets.as_deref());
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

fn estimate_aggregate_output_rows(
    input_rows: usize,
    group_by: &[Expression],
    grouping_sets: Option<&[Vec<Expression>]>,
) -> usize {
    if input_rows == 0 {
        return match grouping_sets {
            Some(sets) => sets.iter().filter(|set| set.is_empty()).count(),
            None if group_by.is_empty() => 1,
            None => 0,
        };
    }

    let grouping_multiplier = grouping_sets.map(|sets| sets.len().max(1)).unwrap_or(1);
    ((input_rows as f64 * AGGREGATE_GROUP_OUTPUT_SELECTIVITY).max(1.0) as usize)
        * grouping_multiplier
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::Database;

    fn empty_input() -> PlanNode {
        PlanNode::SeqScan {
            table: "items".to_string(),
            output_label: None,
            filter: None,
            cost: 0.0,
            rows: 0,
        }
    }

    fn aggregate_rows(plan: PlanNode) -> usize {
        match plan {
            PlanNode::Aggregate { rows, .. } => rows,
            other => panic!("expected aggregate plan, got {other:?}"),
        }
    }

    #[test]
    fn grouped_aggregate_over_empty_input_estimates_zero_rows() {
        let db = Database::new();
        let planner = QueryPlanner::new(&db);

        let plan = planner.plan_aggregate(
            empty_input(),
            vec![Expression::Column("category".to_string())],
            None,
            Vec::new(),
            None,
        );

        assert_eq!(aggregate_rows(plan), 0);
    }

    #[test]
    fn global_aggregate_over_empty_input_estimates_single_row() {
        let db = Database::new();
        let planner = QueryPlanner::new(&db);

        let plan = planner.plan_aggregate(empty_input(), Vec::new(), None, Vec::new(), None);

        assert_eq!(aggregate_rows(plan), 1);
    }

    #[test]
    fn empty_grouping_set_over_empty_input_estimates_single_row() {
        let db = Database::new();
        let planner = QueryPlanner::new(&db);

        let plan = planner.plan_aggregate(
            empty_input(),
            vec![Expression::Column("category".to_string())],
            Some(vec![
                vec![Expression::Column("category".to_string())],
                vec![],
            ]),
            Vec::new(),
            None,
        );

        assert_eq!(aggregate_rows(plan), 1);
    }
}
