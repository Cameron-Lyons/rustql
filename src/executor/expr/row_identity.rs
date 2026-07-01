use crate::ast::{BinaryOperator, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use super::compare::{compare_values, compare_values_for_sort};

pub(crate) struct SqlRowSet {
    non_numeric_seen: BTreeSet<Vec<Value>>,
    numeric_seen: Vec<Vec<Value>>,
}

impl SqlRowSet {
    pub(crate) fn new() -> Self {
        Self {
            non_numeric_seen: BTreeSet::new(),
            numeric_seen: Vec::new(),
        }
    }

    pub(crate) fn insert(&mut self, row: Vec<Value>) -> bool {
        if row_has_finite_numeric_value(&row) {
            if self
                .numeric_seen
                .iter()
                .any(|seen| rows_equal_for_sql_identity(seen, &row))
            {
                false
            } else {
                self.numeric_seen.push(row);
                true
            }
        } else {
            self.non_numeric_seen.insert(row)
        }
    }
}

pub(crate) struct SqlRowMultiset {
    non_numeric_counts: BTreeMap<Vec<Value>, usize>,
    numeric_counts: Vec<(Vec<Value>, usize)>,
}

impl SqlRowMultiset {
    pub(crate) fn new() -> Self {
        Self {
            non_numeric_counts: BTreeMap::new(),
            numeric_counts: Vec::new(),
        }
    }

    pub(crate) fn add(&mut self, row: Vec<Value>) -> bool {
        if row_has_finite_numeric_value(&row) {
            if let Some((_, count)) = self
                .numeric_counts
                .iter_mut()
                .find(|(candidate, _)| rows_equal_for_sql_identity(candidate, &row))
            {
                *count += 1;
                false
            } else {
                self.numeric_counts.push((row, 1));
                true
            }
        } else {
            let count = self.non_numeric_counts.entry(row).or_insert(0);
            let is_new = *count == 0;
            *count += 1;
            is_new
        }
    }

    pub(crate) fn count(&self, row: &[Value]) -> usize {
        if row_has_finite_numeric_value(row) {
            self.numeric_counts
                .iter()
                .find(|(candidate, _)| rows_equal_for_sql_identity(candidate, row))
                .map(|(_, count)| *count)
                .unwrap_or(0)
        } else {
            self.non_numeric_counts.get(row).copied().unwrap_or(0)
        }
    }

    pub(crate) fn contains(&self, row: &[Value]) -> bool {
        self.count(row) > 0
    }
}

pub(crate) fn row_has_finite_numeric_value(row: &[Value]) -> bool {
    row.iter().any(is_finite_numeric_value)
}

pub(crate) fn rows_equal_for_sql_identity(left: &[Value], right: &[Value]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .all(|(left, right)| values_equal_for_sql_identity(left, right))
}

pub(crate) fn values_equal_for_sql_identity(left: &Value, right: &Value) -> bool {
    if is_finite_numeric_value(left) && is_finite_numeric_value(right) {
        compare_values(left, &BinaryOperator::Equal, right).unwrap_or(false)
    } else {
        compare_values_for_sort(left, right) == Ordering::Equal
    }
}

fn is_finite_numeric_value(value: &Value) -> bool {
    match value {
        Value::Integer(_) => true,
        Value::Float(value) => value.is_finite(),
        _ => false,
    }
}
