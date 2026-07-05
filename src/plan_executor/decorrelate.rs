use super::*;
use crate::executor::expr::compare_values;
use std::collections::BTreeSet;

/// Decorrelating a filter EXISTS scans the inner table once instead of
/// executing the subquery per outer row; skip the rewrite when the outer side
/// is too small for the scan to amortize against per-row executions.
const DECORRELATE_MIN_OUTER_ROWS: usize = 2;
const DECORRELATE_INNER_ROW_FACTOR: usize = 64;

/// A filter conjunct whose subquery work has been hoisted out of the per-row
/// loop: either an uncorrelated EXISTS evaluated once, or a correlated EXISTS
/// turned into a semi-join key-set probe.
enum PreparedFilterConjunct {
    Constant(bool),
    ExistsProbe(ExistsProbe),
}

impl PreparedFilterConjunct {
    fn matches(&self, row: &[Value]) -> Result<bool, RustqlError> {
        match self {
            Self::Constant(value) => Ok(*value),
            Self::ExistsProbe(probe) => probe.matches(row),
        }
    }
}

struct ExistsProbe {
    probe_index: usize,
    negated: bool,
    keys: JoinKeySet,
}

impl ExistsProbe {
    fn matches(&self, row: &[Value]) -> Result<bool, RustqlError> {
        let probe = row.get(self.probe_index).unwrap_or(&Value::Null);
        Ok(self.keys.contains(probe)? != self.negated)
    }
}

/// A row filter with any decorrelatable EXISTS conjuncts hoisted out of the
/// per-row loop; every other shape falls through to the expression evaluator
/// on the original condition.
pub(super) struct PreparedRowFilter<'cond> {
    condition: &'cond Expression,
    conjuncts: Vec<PreparedFilterConjunct>,
    residual: Option<Expression>,
}

impl<'cond> PreparedRowFilter<'cond> {
    pub(super) fn new(
        db: &dyn DatabaseCatalog,
        condition: &'cond Expression,
        columns: &[ColumnDefinition],
        row_count: usize,
    ) -> Self {
        let (conjuncts, residual) = if row_count == 0 {
            (Vec::new(), None)
        } else {
            prepare_filter_condition(db, condition, columns, row_count)
        };
        Self {
            condition,
            conjuncts,
            residual,
        }
    }

    pub(super) fn include(
        &self,
        executor: &PlanExecutor<'_>,
        columns: &[ColumnDefinition],
        row: &[Value],
    ) -> Result<bool, RustqlError> {
        if self.conjuncts.is_empty() {
            return executor.evaluate_expression(self.condition, columns, row);
        }
        // Mirror the evaluator's eager AND: every part is evaluated so
        // errors surface regardless of other conjuncts.
        let mut include = true;
        for conjunct in &self.conjuncts {
            include &= conjunct.matches(row)?;
        }
        if let Some(residual) = &self.residual {
            include &= executor.evaluate_expression(residual, columns, row)?;
        }
        Ok(include)
    }
}

/// Splits a filter condition into decorrelated EXISTS conjuncts and the
/// residual condition the expression evaluator must still handle per row.
/// Returns an empty conjunct list when nothing was decorrelated, so the
/// caller can keep the original condition untouched.
fn prepare_filter_condition(
    db: &dyn DatabaseCatalog,
    condition: &Expression,
    outer_columns: &[ColumnDefinition],
    outer_row_count: usize,
) -> (Vec<PreparedFilterConjunct>, Option<Expression>) {
    let conjuncts = split_conjuncts(condition);
    let mut prepared = Vec::new();
    let mut residual: Vec<&Expression> = Vec::new();

    for conjunct in conjuncts {
        let (subquery, negated) = match conjunct {
            Expression::Exists(subquery) => (subquery.as_ref(), false),
            Expression::UnaryOp {
                op: UnaryOperator::Not,
                expr,
            } => match expr.as_ref() {
                Expression::Exists(subquery) => (subquery.as_ref(), true),
                _ => {
                    residual.push(conjunct);
                    continue;
                }
            },
            _ => {
                residual.push(conjunct);
                continue;
            }
        };

        match prepare_exists_conjunct(db, subquery, negated, outer_columns, outer_row_count) {
            Some(conjunct) => prepared.push(conjunct),
            None => residual.push(conjunct),
        }
    }

    if prepared.is_empty() {
        return (prepared, None);
    }
    (prepared, combine_conjunct_refs(&residual))
}

fn split_conjuncts(condition: &Expression) -> Vec<&Expression> {
    let mut conjuncts = Vec::new();
    let mut pending = vec![condition];
    while let Some(expr) = pending.pop() {
        if let Expression::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } = expr
        {
            pending.push(right.as_ref());
            pending.push(left.as_ref());
        } else {
            conjuncts.push(expr);
        }
    }
    conjuncts
}

fn combine_conjunct_refs(conjuncts: &[&Expression]) -> Option<Expression> {
    let mut combined: Option<Expression> = None;
    for conjunct in conjuncts {
        combined = Some(match combined {
            None => (*conjunct).clone(),
            Some(existing) => Expression::BinaryOp {
                left: Box::new(existing),
                op: BinaryOperator::And,
                right: Box::new((*conjunct).clone()),
            },
        });
    }
    combined
}

/// Attempts to hoist one EXISTS conjunct out of the per-row filter loop.
/// Uncorrelated subqueries run once and fold to a constant. Correlated
/// subqueries whose only outer reference is one side of a `column = column`
/// WHERE conjunct become a key-set probe: the subquery runs once without the
/// correlated conjunct, and each outer row checks membership of its key.
/// Returns None (leaving the conjunct on the per-row path) for every shape
/// where the rewrite is not known to be safe or profitable.
fn prepare_exists_conjunct(
    db: &dyn DatabaseCatalog,
    subquery: &SelectStatement,
    negated: bool,
    outer_columns: &[ColumnDefinition],
    outer_row_count: usize,
) -> Option<PreparedFilterConjunct> {
    let binding = outer_value_binding(db, subquery, outer_columns)?;

    if !binding.is_correlated() {
        // EXISTS only observes emptiness, so one execution answers every row.
        let result = execute_planned_select(db, subquery).ok()?;
        return Some(PreparedFilterConjunct::Constant(
            result.rows.is_empty() == negated,
        ));
    }

    if outer_row_count < DECORRELATE_MIN_OUTER_ROWS {
        return None;
    }
    // EXISTS ignores output shape and order, so ORDER BY / LIMIT / DISTINCT
    // can be dropped, but only when the remaining clauses cannot change
    // emptiness or observable evaluation behavior.
    if subquery.group_by.is_some()
        || subquery.having.is_some()
        || subquery.distinct_on.is_some()
        || subquery.offset.is_some()
        || subquery.fetch.is_some()
        || subquery.limit == Some(0)
    {
        return None;
    }
    if !subquery.columns.iter().all(|column| {
        matches!(
            column,
            Column::All
                | Column::Named { .. }
                | Column::Expression {
                    expr: Expression::Value(_),
                    ..
                }
        )
    }) {
        return None;
    }

    let inner_rows = db.get_table(&subquery.from)?.rows.len();
    if inner_rows > outer_row_count.saturating_mul(DECORRELATE_INNER_ROW_FACTOR) {
        return None;
    }

    let where_clause = subquery.where_clause.as_ref()?;
    let conjuncts = split_conjuncts(where_clause);
    let (key_position, local_key, probe_index) =
        conjuncts.iter().enumerate().find_map(|(idx, conjunct)| {
            let (local_key, probe_index) = correlated_equality(conjunct, &binding)?;
            Some((idx, local_key, probe_index))
        })?;

    let residual: Vec<&Expression> = conjuncts
        .iter()
        .enumerate()
        .filter(|(idx, _)| *idx != key_position)
        .map(|(_, conjunct)| *conjunct)
        .collect();

    let mut key_select = subquery.clone();
    key_select.columns = vec![Column::Named {
        name: local_key.to_string(),
        alias: None,
    }];
    key_select.where_clause = combine_conjunct_refs(&residual);
    key_select.order_by = None;
    key_select.limit = None;
    key_select.distinct = false;

    // The rewritten subquery must be fully local: any outer reference left
    // outside the dropped equality conjunct makes the rewrite unsound.
    let key_binding = outer_value_binding(db, &key_select, outer_columns)?;
    if key_binding.is_correlated() {
        return None;
    }

    let result = execute_planned_select(db, &key_select).ok()?;
    let keys = JoinKeySet::from_key_values(result.rows.into_iter().map(|mut row| {
        if row.is_empty() {
            Value::Null
        } else {
            row.swap_remove(0)
        }
    }))?;

    Some(PreparedFilterConjunct::ExistsProbe(ExistsProbe {
        probe_index,
        negated,
        keys,
    }))
}

/// Matches a `local_column = outer_column` conjunct (either order) whose
/// outer side is a recorded outer reference and whose other side is not.
fn correlated_equality<'a>(
    conjunct: &'a Expression,
    binding: &OuterValueBinding,
) -> Option<(&'a str, usize)> {
    let Expression::BinaryOp {
        left,
        op: BinaryOperator::Equal,
        right,
    } = conjunct
    else {
        return None;
    };
    let (Expression::Column(left_col), Expression::Column(right_col)) =
        (left.as_ref(), right.as_ref())
    else {
        return None;
    };

    match (
        binding.outer_index_of(left_col),
        binding.outer_index_of(right_col),
    ) {
        (Some(outer_index), None) => Some((right_col.as_str(), outer_index)),
        (None, Some(outer_index)) => Some((left_col.as_str(), outer_index)),
        _ => None,
    }
}

/// Semi-join key set mirroring the hash join's build tables: integer keys by
/// f64 bit pattern, finite float keys in an epsilon-range tree, and
/// non-numeric keys hashed exactly.
struct JoinKeySet {
    integer_keys: HashSet<u64>,
    float_keys: BTreeSet<NumericJoinKey>,
    non_numeric_keys: HashSet<NonNumericJoinKey>,
    /// One key value retained to reproduce the evaluator's type-mismatch
    /// errors when a probe value's type class is incompatible with the keys.
    representative: Option<Value>,
}

impl JoinKeySet {
    /// Returns None when the keys span incompatible type classes; comparing
    /// such a column per row surfaces evaluator errors that depend on scan
    /// order, so those subqueries stay on the per-row path.
    fn from_key_values(values: impl Iterator<Item = Value>) -> Option<Self> {
        let mut set = Self {
            integer_keys: HashSet::new(),
            float_keys: BTreeSet::new(),
            non_numeric_keys: HashSet::new(),
            representative: None,
        };
        for value in values {
            let Some(key) = join_key(&value) else {
                // NULL and non-finite keys never satisfy the equality.
                continue;
            };
            match &set.representative {
                None => set.representative = Some(value.clone()),
                Some(representative) => {
                    if !same_comparison_class(representative, &value) {
                        return None;
                    }
                }
            }
            match key {
                JoinKey::Integer(key) => {
                    set.integer_keys.insert(numeric_key_bits(key));
                }
                JoinKey::Float(key) => {
                    set.float_keys.insert(NumericJoinKey(key));
                }
                JoinKey::NonNumeric(key) => {
                    set.non_numeric_keys.insert(key);
                }
            }
        }
        Some(set)
    }

    fn contains(&self, probe: &Value) -> Result<bool, RustqlError> {
        let Some(representative) = &self.representative else {
            return Ok(false);
        };
        if matches!(probe, Value::Null) {
            return Ok(false);
        }
        if !same_comparison_class(representative, probe) {
            // Reproduce the error the evaluator would raise comparing the
            // probe value against any key of this incompatible class.
            return compare_values(probe, &BinaryOperator::Equal, representative);
        }
        let Some(key) = join_key(probe) else {
            return Ok(false);
        };
        Ok(match key {
            JoinKey::Integer(key) | JoinKey::Float(key) => {
                if self.integer_keys.contains(&numeric_key_bits(key)) {
                    true
                } else {
                    let nearest_integer = key.round();
                    let matches_nearest = (key - nearest_integer).abs() < f64::EPSILON
                        && numeric_key_bits(nearest_integer) != numeric_key_bits(key)
                        && self
                            .integer_keys
                            .contains(&numeric_key_bits(nearest_integer));
                    matches_nearest
                        || (!self.float_keys.is_empty()
                            && self
                                .float_keys
                                .range(
                                    NumericJoinKey(key - f64::EPSILON)
                                        ..=NumericJoinKey(key + f64::EPSILON),
                                )
                                .next()
                                .is_some())
                }
            }
            JoinKey::NonNumeric(key) => self.non_numeric_keys.contains(&key),
        })
    }
}

/// The evaluator compares all numerics through f64 and errors on any other
/// cross-type comparison, so numerics form one class and every other type
/// stands alone.
fn same_comparison_class(left: &Value, right: &Value) -> bool {
    fn class(value: &Value) -> u8 {
        match value {
            Value::Integer(_) | Value::Float(_) => 0,
            Value::Text(_) => 1,
            Value::Boolean(_) => 2,
            Value::Date(_) => 3,
            Value::Time(_) => 4,
            Value::DateTime(_) => 5,
            Value::Null => 6,
        }
    }
    class(left) == class(right)
}
