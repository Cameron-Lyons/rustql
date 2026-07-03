use super::*;

impl<'a> PlanExecutor<'a> {
    pub(super) fn execute_set_operation(
        &self,
        left: ExecutionResult,
        right: ExecutionResult,
        op: &SetOperation,
    ) -> Result<ExecutionResult, RustqlError> {
        if left.columns.len() != right.columns.len() {
            return Err(RustqlError::Internal(
                "Set operation inputs must have the same number of columns".to_string(),
            ));
        }

        let rows = match op {
            SetOperation::UnionAll => {
                let mut combined = left.rows;
                combined.extend(right.rows);
                combined
            }
            SetOperation::Union => {
                let mut seen = SqlRowMultiset::new();
                let mut combined = Vec::new();
                for row in left.rows.into_iter().chain(right.rows) {
                    if seen.add(row.clone()) {
                        combined.push(row);
                    }
                }
                combined
            }
            SetOperation::Intersect => {
                let mut right_counts = SqlRowMultiset::new();
                for row in right.rows {
                    right_counts.add(row);
                }
                let mut seen = SqlRowMultiset::new();
                let mut combined = Vec::new();
                for row in left.rows {
                    if right_counts.contains(&row) && seen.add(row.clone()) {
                        combined.push(row);
                    }
                }
                combined
            }
            SetOperation::IntersectAll => {
                let right_counts = row_counts(right.rows);
                let (left_counts, left_order) = ordered_row_counts(left.rows);
                let mut combined = Vec::new();
                for row in left_order {
                    let left_count = left_counts.count(&row);
                    let right_count = right_counts.count(&row);
                    for _ in 0..left_count.min(right_count) {
                        combined.push(row.clone());
                    }
                }
                combined
            }
            SetOperation::Except => {
                let mut right_counts = SqlRowMultiset::new();
                for row in right.rows {
                    right_counts.add(row);
                }
                let mut seen = SqlRowMultiset::new();
                let mut combined = Vec::new();
                for row in left.rows {
                    if !right_counts.contains(&row) && seen.add(row.clone()) {
                        combined.push(row);
                    }
                }
                combined
            }
            SetOperation::ExceptAll => {
                let right_counts = row_counts(right.rows);
                let (left_counts, left_order) = ordered_row_counts(left.rows);
                let mut combined = Vec::new();
                for row in left_order {
                    let left_count = left_counts.count(&row);
                    let right_count = right_counts.count(&row);
                    for _ in 0..left_count.saturating_sub(right_count) {
                        combined.push(row.clone());
                    }
                }
                combined
            }
        };

        Ok(ExecutionResult {
            columns: left.columns,
            rows,
        })
    }
}

fn row_counts(rows: Vec<Vec<Value>>) -> SqlRowMultiset {
    let mut counts = SqlRowMultiset::new();
    for row in rows {
        counts.add(row);
    }
    counts
}

fn ordered_row_counts(rows: Vec<Vec<Value>>) -> (SqlRowMultiset, Vec<Vec<Value>>) {
    let mut counts = SqlRowMultiset::new();
    let mut order = Vec::new();

    for row in rows {
        if counts.add(row.clone()) {
            order.push(row);
        }
    }

    (counts, order)
}
