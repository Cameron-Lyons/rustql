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
                let mut seen = BTreeSet::new();
                let mut combined = Vec::new();
                for row in left.rows.into_iter().chain(right.rows) {
                    if seen.insert(row.clone()) {
                        combined.push(row);
                    }
                }
                combined
            }
            SetOperation::Intersect => {
                let right_set: BTreeSet<Vec<Value>> = right.rows.into_iter().collect();
                let mut seen = BTreeSet::new();
                let mut combined = Vec::new();
                for row in left.rows {
                    if right_set.contains(&row) && seen.insert(row.clone()) {
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
                    let left_count = left_counts.get(&row).copied().unwrap_or(0);
                    let right_count = right_counts.get(&row).copied().unwrap_or(0);
                    for _ in 0..left_count.min(right_count) {
                        combined.push(row.clone());
                    }
                }
                combined
            }
            SetOperation::Except => {
                let right_set: BTreeSet<Vec<Value>> = right.rows.into_iter().collect();
                let mut seen = BTreeSet::new();
                let mut combined = Vec::new();
                for row in left.rows {
                    if !right_set.contains(&row) && seen.insert(row.clone()) {
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
                    let left_count = left_counts.get(&row).copied().unwrap_or(0);
                    let right_count = right_counts.get(&row).copied().unwrap_or(0);
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

fn row_counts(rows: Vec<Vec<Value>>) -> BTreeMap<Vec<Value>, usize> {
    let mut counts = BTreeMap::new();
    for row in rows {
        *counts.entry(row).or_insert(0) += 1;
    }
    counts
}

fn ordered_row_counts(rows: Vec<Vec<Value>>) -> (BTreeMap<Vec<Value>, usize>, Vec<Vec<Value>>) {
    let mut counts = BTreeMap::new();
    let mut order = Vec::new();

    for row in rows {
        if let Some(count) = counts.get_mut(&row) {
            *count += 1;
        } else {
            order.push(row.clone());
            counts.insert(row, 1);
        }
    }

    (counts, order)
}
