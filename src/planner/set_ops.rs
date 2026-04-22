use super::*;

impl<'a> QueryPlanner<'a> {
    pub(super) fn plan_set_operation(
        &self,
        stmt: &SelectStatement,
        right_stmt: &SelectStatement,
        op: &SetOperation,
    ) -> Result<PlanNode, RustqlError> {
        let mut left_stmt = stmt.clone();
        left_stmt.set_op = None;
        let right_stmt = right_stmt.clone();

        let left_plan = self.plan_select(&left_stmt)?;
        let right_plan = self.plan_select(&right_stmt)?;
        let left_rows = self.estimate_rows(&left_plan);
        let right_rows = self.estimate_rows(&right_plan);
        let rows = match op {
            SetOperation::UnionAll => left_rows.saturating_add(right_rows),
            SetOperation::Union => left_rows.saturating_add(right_rows),
            SetOperation::Intersect | SetOperation::IntersectAll => left_rows.min(right_rows),
            SetOperation::Except | SetOperation::ExceptAll => left_rows,
        };
        let cost = self.estimate_cost(&left_plan) + self.estimate_cost(&right_plan);

        Ok(PlanNode::SetOperation {
            left: Box::new(left_plan),
            right: Box::new(right_plan),
            left_select: Box::new(left_stmt),
            right_select: Box::new(right_stmt),
            op: op.clone(),
            cost,
            rows,
        })
    }
}
