use crate::ast::{BinaryOperator, Value};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use super::compare::{compare_values, compare_values_for_sort};

/// Canonical form of a row cell under SQL identity: equal cells always map
/// to equal canonical forms is NOT guaranteed (epsilon-close numerics and
/// NaN payloads differ), but equal canonical forms always mean identity-equal
/// cells, so exact index hits are sound.
#[derive(Clone, PartialEq, Eq, Hash)]
enum IdentityCell {
    /// Finite numeric as its canonical f64 bit pattern (-0.0 collapsed onto
    /// 0.0, integers through their f64 cast like the comparison evaluator).
    Numeric(u64),
    /// All NaNs compare equal for sort identity regardless of payload.
    NanFloat,
    InfiniteFloat(bool),
    Text(String),
    Boolean(bool),
    Date(String),
    Time(String),
    DateTime(String),
    Null,
}

fn identity_cells(row: &[Value]) -> Vec<IdentityCell> {
    row.iter()
        .map(|value| match value {
            Value::Integer(value) => IdentityCell::Numeric(((*value as f64) + 0.0).to_bits()),
            Value::Float(value) if value.is_finite() => {
                IdentityCell::Numeric((value + 0.0).to_bits())
            }
            Value::Float(value) if value.is_nan() => IdentityCell::NanFloat,
            Value::Float(value) => IdentityCell::InfiniteFloat(value.is_sign_positive()),
            Value::Text(value) => IdentityCell::Text(value.clone()),
            Value::Boolean(value) => IdentityCell::Boolean(*value),
            Value::Date(value) => IdentityCell::Date(value.clone()),
            Value::Time(value) => IdentityCell::Time(value.clone()),
            Value::DateTime(value) => IdentityCell::DateTime(value.clone()),
            Value::Null => IdentityCell::Null,
        })
        .collect()
}

#[derive(Clone, Copy, PartialEq)]
struct OrderedF64(f64);

impl Eq for OrderedF64 {}

impl PartialOrd for OrderedF64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderedF64 {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.0 == other.0 {
            Ordering::Equal
        } else if self.0 < other.0 {
            Ordering::Less
        } else {
            Ordering::Greater
        }
    }
}

pub(crate) enum IdentityProbe {
    /// The row's canonical form was recorded before, at this position.
    Hit(usize),
    /// No previously recorded row can be identity-equal to this one.
    New,
    /// An epsilon-close candidate exists; the caller must compare linearly.
    MaybeEqual,
}

/// Index over rows compared with `rows_equal_for_sql_identity`. Exact
/// canonical matches resolve through a hash map; rows without any
/// epsilon-close numeric candidate are proven new through per-column value
/// trees, so the linear identity scan runs only for genuinely ambiguous
/// (epsilon-close) keys.
pub(crate) struct RowIdentityIndex {
    exact: HashMap<Vec<IdentityCell>, usize>,
    numeric_columns: HashMap<usize, BTreeSet<OrderedF64>>,
}

impl RowIdentityIndex {
    pub(crate) fn new() -> Self {
        Self {
            exact: HashMap::new(),
            numeric_columns: HashMap::new(),
        }
    }

    pub(crate) fn probe(&self, row: &[Value]) -> IdentityProbe {
        if let Some(&index) = self.exact.get(&identity_cells(row)) {
            return IdentityProbe::Hit(index);
        }
        for (column, value) in row.iter().enumerate() {
            let Some(value) = finite_numeric_cell(value) else {
                continue;
            };
            let has_epsilon_candidate = self.numeric_columns.get(&column).is_some_and(|values| {
                values
                    .range(OrderedF64(value - f64::EPSILON)..=OrderedF64(value + f64::EPSILON))
                    .next()
                    .is_some()
            });
            if !has_epsilon_candidate {
                // This cell differs from every recorded row's cell by at
                // least epsilon, so no recorded row is identity-equal.
                return IdentityProbe::New;
            }
        }
        if self.exact.is_empty() {
            return IdentityProbe::New;
        }
        IdentityProbe::MaybeEqual
    }

    pub(crate) fn record(&mut self, row: &[Value], index: usize) {
        // Keep the first position for a canonical form, matching what a
        // linear scan over insertion order would find.
        self.exact.entry(identity_cells(row)).or_insert(index);
        for (column, value) in row.iter().enumerate() {
            if let Some(value) = finite_numeric_cell(value) {
                self.numeric_columns
                    .entry(column)
                    .or_default()
                    .insert(OrderedF64(value + 0.0));
            }
        }
    }
}

fn finite_numeric_cell(value: &Value) -> Option<f64> {
    match value {
        Value::Integer(value) => Some(*value as f64),
        Value::Float(value) if value.is_finite() => Some(*value),
        _ => None,
    }
}

pub(crate) struct SqlRowSet {
    non_numeric_seen: BTreeSet<Vec<Value>>,
    numeric_seen: Vec<Vec<Value>>,
    numeric_index: RowIdentityIndex,
}

impl SqlRowSet {
    pub(crate) fn new() -> Self {
        Self {
            non_numeric_seen: BTreeSet::new(),
            numeric_seen: Vec::new(),
            numeric_index: RowIdentityIndex::new(),
        }
    }

    pub(crate) fn insert(&mut self, row: Vec<Value>) -> bool {
        if row_has_finite_numeric_value(&row) {
            let existing = match self.numeric_index.probe(&row) {
                IdentityProbe::Hit(_) => return false,
                IdentityProbe::New => None,
                IdentityProbe::MaybeEqual => self
                    .numeric_seen
                    .iter()
                    .position(|seen| rows_equal_for_sql_identity(seen, &row)),
            };
            match existing {
                Some(index) => {
                    self.numeric_index.record(&row, index);
                    false
                }
                None => {
                    self.numeric_index.record(&row, self.numeric_seen.len());
                    self.numeric_seen.push(row);
                    true
                }
            }
        } else {
            self.non_numeric_seen.insert(row)
        }
    }
}

pub(crate) struct SqlRowMultiset {
    non_numeric_counts: BTreeMap<Vec<Value>, usize>,
    numeric_counts: Vec<(Vec<Value>, usize)>,
    numeric_index: RowIdentityIndex,
}

impl SqlRowMultiset {
    pub(crate) fn with_capacity(capacity: usize) -> Self {
        Self {
            non_numeric_counts: BTreeMap::new(),
            numeric_counts: Vec::with_capacity(capacity),
            numeric_index: RowIdentityIndex::new(),
        }
    }

    pub(crate) fn add(&mut self, row: Vec<Value>) -> bool {
        if row_has_finite_numeric_value(&row) {
            let scanned = match self.numeric_index.probe(&row) {
                IdentityProbe::Hit(index) => {
                    self.numeric_counts[index].1 += 1;
                    return false;
                }
                IdentityProbe::New => None,
                IdentityProbe::MaybeEqual => self
                    .numeric_counts
                    .iter()
                    .position(|(candidate, _)| rows_equal_for_sql_identity(candidate, &row)),
            };
            match scanned {
                Some(index) => {
                    self.numeric_index.record(&row, index);
                    self.numeric_counts[index].1 += 1;
                    false
                }
                None => {
                    self.numeric_index.record(&row, self.numeric_counts.len());
                    self.numeric_counts.push((row, 1));
                    true
                }
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
            let index = match self.numeric_index.probe(row) {
                IdentityProbe::Hit(index) => Some(index),
                IdentityProbe::New => None,
                IdentityProbe::MaybeEqual => self
                    .numeric_counts
                    .iter()
                    .position(|(candidate, _)| rows_equal_for_sql_identity(candidate, row)),
            };
            index.map(|index| self.numeric_counts[index].1).unwrap_or(0)
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
