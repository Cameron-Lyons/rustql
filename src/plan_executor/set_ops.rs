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
                let mut right_counts: BTreeMap<Vec<Value>, usize> = BTreeMap::new();
                for row in right.rows {
                    *right_counts.entry(row).or_insert(0) += 1;
                }

                let mut left_counts: BTreeMap<Vec<Value>, usize> = BTreeMap::new();
                let mut left_order = Vec::new();
                for row in left.rows {
                    let count = left_counts.entry(row.clone()).or_insert(0);
                    if *count == 0 {
                        left_order.push(row.clone());
                    }
                    *count += 1;
                }

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
                let mut right_counts: BTreeMap<Vec<Value>, usize> = BTreeMap::new();
                for row in right.rows {
                    *right_counts.entry(row).or_insert(0) += 1;
                }

                let mut left_counts: BTreeMap<Vec<Value>, usize> = BTreeMap::new();
                let mut left_order = Vec::new();
                for row in left.rows {
                    let count = left_counts.entry(row.clone()).or_insert(0);
                    if *count == 0 {
                        left_order.push(row.clone());
                    }
                    *count += 1;
                }

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
