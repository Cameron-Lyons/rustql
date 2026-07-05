use super::*;
use crate::executor::expr::compare_values;
use std::collections::HashMap;

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
    keys: JoinKeyTable<()>,
}

impl ExistsProbe {
    fn matches(&self, row: &[Value]) -> Result<bool, RustqlError> {
        let probe = row.get(self.probe_index).unwrap_or(&Value::Null);
        Ok(self.keys.lookup(probe)?.is_some() != self.negated)
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
    let keys = JoinKeyTable::from_entries(result.rows.into_iter().map(|mut row| {
        let key = if row.is_empty() {
            Value::Null
        } else {
            row.swap_remove(0)
        };
        (key, ())
    }))?;

    Some(PreparedFilterConjunct::ExistsProbe(ExistsProbe {
        probe_index,
        negated,
        keys,
    }))
}

/// A SELECT-list scalar subquery hoisted out of the per-row projection loop:
/// either an uncorrelated subquery evaluated once, or a correlated
/// single-aggregate subquery turned into a grouped lookup table.
pub(super) enum PreparedScalarSubquery {
    Constant(Value),
    Lookup(ScalarAggregateLookup),
}

impl PreparedScalarSubquery {
    pub(super) fn value_for(&self, row: &[Value]) -> Result<Value, RustqlError> {
        match self {
            Self::Constant(value) => Ok(value.clone()),
            Self::Lookup(lookup) => lookup.value_for(row),
        }
    }
}

pub(super) struct ScalarAggregateLookup {
    probe_index: usize,
    values: JoinKeyTable<Value>,
    /// The aggregate's result over an empty input (e.g. 0 for COUNT, NULL
    /// for MAX), returned for outer keys with no matching inner rows.
    empty_value: Value,
}

impl ScalarAggregateLookup {
    fn value_for(&self, row: &[Value]) -> Result<Value, RustqlError> {
        let probe = row.get(self.probe_index).unwrap_or(&Value::Null);
        Ok(self
            .values
            .lookup(probe)?
            .unwrap_or(&self.empty_value)
            .clone())
    }
}

/// Attempts to hoist a SELECT-list scalar subquery out of the per-row
/// projection loop. Uncorrelated subqueries evaluate once. A correlated
/// subquery of exactly one aggregate whose only outer reference is one side
/// of a `column = column` WHERE conjunct becomes a lookup table computed by
/// one GROUP BY query over the correlation key; missing keys return the
/// aggregate's empty-input value. Returns None (keeping the subquery on the
/// per-row path) for every other shape.
pub(super) fn prepare_scalar_subquery(
    db: &dyn DatabaseCatalog,
    subquery: &SelectStatement,
    outer_columns: &[ColumnDefinition],
    outer_row_count: usize,
) -> Option<PreparedScalarSubquery> {
    if subquery.columns.len() != 1 {
        return None;
    }
    let binding = outer_value_binding(db, subquery, outer_columns)?;

    if !binding.is_correlated() {
        let result = execute_planned_select(db, subquery).ok()?;
        if result.columns.len() != 1 {
            return None;
        }
        return match result.rows.len() {
            0 => Some(PreparedScalarSubquery::Constant(Value::Null)),
            1 => result
                .rows
                .into_iter()
                .next()
                .and_then(|row| row.into_iter().next())
                .map(PreparedScalarSubquery::Constant),
            // More than one row is a per-row error; surface it there.
            _ => None,
        };
    }

    if outer_row_count < DECORRELATE_MIN_OUTER_ROWS {
        return None;
    }
    // A single aggregate without grouping returns exactly one row for any
    // key, so ORDER BY / LIMIT >= 1 / DISTINCT cannot change the result.
    if subquery.group_by.is_some()
        || subquery.having.is_some()
        || subquery.distinct_on.is_some()
        || subquery.offset.is_some()
        || subquery.fetch.is_some()
        || subquery.limit == Some(0)
    {
        return None;
    }
    let Some(Column::Function(aggregate)) = subquery.columns.first() else {
        return None;
    };

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

    let mut grouped_select = subquery.clone();
    grouped_select.columns = vec![
        Column::Named {
            name: local_key.to_string(),
            alias: None,
        },
        Column::Function(aggregate.clone()),
    ];
    grouped_select.where_clause = combine_conjunct_refs(&residual);
    grouped_select.group_by = Some(GroupByClause::Simple(vec![Expression::Column(
        local_key.to_string(),
    )]));
    grouped_select.order_by = None;
    grouped_select.limit = None;
    grouped_select.distinct = false;

    // The rewritten subquery must be fully local: any outer reference left
    // outside the dropped equality conjunct makes the rewrite unsound.
    let grouped_binding = outer_value_binding(db, &grouped_select, outer_columns)?;
    if grouped_binding.is_correlated() {
        return None;
    }

    // The aggregate over an empty input, computed by the real aggregate
    // machinery so COUNT yields 0 while MAX/MIN/SUM/AVG yield NULL.
    let mut empty_select = subquery.clone();
    empty_select.where_clause = Some(Expression::Value(Value::Boolean(false)));
    empty_select.order_by = None;
    empty_select.limit = None;
    empty_select.distinct = false;
    let empty_result = execute_planned_select(db, &empty_select).ok()?;
    let empty_value = match empty_result.rows.len() {
        0 => Value::Null,
        1 => empty_result
            .rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next())?,
        _ => return None,
    };

    let grouped_result = execute_planned_select(db, &grouped_select).ok()?;
    if grouped_result.columns.len() != 2 {
        return None;
    }
    let values = JoinKeyTable::from_entries(grouped_result.rows.into_iter().filter_map(|row| {
        let mut cells = row.into_iter();
        let key = cells.next()?;
        let value = cells.next()?;
        Some((key, value))
    }))?;
    if !values.numeric_keys_unambiguous() {
        return None;
    }

    Some(PreparedScalarSubquery::Lookup(ScalarAggregateLookup {
        probe_index,
        values,
        empty_value,
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

/// Correlation key table mirroring the hash join's build tables: integer
/// keys by f64 bit pattern, finite float keys in an epsilon-range tree, and
/// non-numeric keys hashed exactly. The payload is `()` for EXISTS semi-join
/// sets and the aggregate value for scalar subquery lookups.
struct JoinKeyTable<V> {
    integer_keys: HashMap<u64, V>,
    float_keys: BTreeMap<NumericJoinKey, V>,
    non_numeric_keys: HashMap<NonNumericJoinKey, V>,
    /// One key value retained to reproduce the evaluator's type-mismatch
    /// errors when a probe value's type class is incompatible with the keys.
    representative: Option<Value>,
}

/// Marker error for keys spanning incompatible type classes; comparing such
/// a column per row surfaces evaluator errors that depend on scan order, so
/// those subqueries stay on the per-row path.
struct KeyClassMismatch;

impl<V> JoinKeyTable<V> {
    fn empty() -> Self {
        Self {
            integer_keys: HashMap::new(),
            float_keys: BTreeMap::new(),
            non_numeric_keys: HashMap::new(),
            representative: None,
        }
    }

    /// Returns None when the keys span incompatible type classes.
    fn from_entries(entries: impl Iterator<Item = (Value, V)>) -> Option<Self> {
        let mut table = Self::empty();
        for (value, payload) in entries {
            match table.slot(&value, || payload) {
                Ok(_) => {}
                Err(KeyClassMismatch) => return None,
            }
        }
        Some(table)
    }

    /// Returns the payload slot for a key, creating it with `default` on
    /// first sight. NULL and non-finite keys never satisfy the correlation
    /// equality, so they get no slot.
    fn slot(
        &mut self,
        value: &Value,
        default: impl FnOnce() -> V,
    ) -> Result<Option<&mut V>, KeyClassMismatch> {
        let Some(key) = join_key(value) else {
            return Ok(None);
        };
        match &self.representative {
            None => self.representative = Some(value.clone()),
            Some(representative) => {
                if !same_comparison_class(representative, value) {
                    return Err(KeyClassMismatch);
                }
            }
        }
        Ok(Some(match key {
            JoinKey::Integer(key) => self
                .integer_keys
                .entry(numeric_key_bits(key))
                .or_insert_with(default),
            JoinKey::Float(key) => self
                .float_keys
                .entry(NumericJoinKey(key))
                .or_insert_with(default),
            JoinKey::NonNumeric(key) => self.non_numeric_keys.entry(key).or_insert_with(default),
        }))
    }

    /// A probe value within epsilon of two distinct stored keys would match
    /// both under evaluator equality, making a single-payload lookup
    /// ambiguous. Returns false when any two numeric keys are that close
    /// (including an integer and a float sharing the same f64), so map-style
    /// users can fall back to the per-row path.
    fn numeric_keys_unambiguous(&self) -> bool {
        let mut numeric_keys: Vec<f64> = self
            .integer_keys
            .keys()
            .map(|bits| f64::from_bits(*bits))
            .chain(self.float_keys.keys().map(|key| key.0))
            .collect();
        numeric_keys.sort_by(|left, right| left.partial_cmp(right).unwrap_or(Ordering::Equal));
        numeric_keys
            .windows(2)
            .all(|pair| (pair[1] - pair[0]).abs() >= 2.0 * f64::EPSILON)
    }

    /// Finds the payload for the first key equal to the probe under the
    /// evaluator's comparison semantics, or an error for the cross-type
    /// comparisons the evaluator rejects.
    fn lookup(&self, probe: &Value) -> Result<Option<&V>, RustqlError> {
        let Some(representative) = &self.representative else {
            return Ok(None);
        };
        if matches!(probe, Value::Null) {
            return Ok(None);
        }
        if !same_comparison_class(representative, probe) {
            // Reproduce the error the evaluator would raise comparing the
            // probe value against any key of this incompatible class.
            compare_values(probe, &BinaryOperator::Equal, representative)?;
            return Ok(None);
        }
        let Some(key) = join_key(probe) else {
            return Ok(None);
        };
        Ok(match key {
            JoinKey::Integer(key) | JoinKey::Float(key) => {
                if let Some(payload) = self.integer_keys.get(&numeric_key_bits(key)) {
                    Some(payload)
                } else {
                    let nearest_integer = key.round();
                    let nearest_match = ((key - nearest_integer).abs() < f64::EPSILON
                        && numeric_key_bits(nearest_integer) != numeric_key_bits(key))
                    .then(|| self.integer_keys.get(&numeric_key_bits(nearest_integer)))
                    .flatten();
                    match nearest_match {
                        Some(payload) => Some(payload),
                        None => self
                            .float_keys
                            .range(
                                NumericJoinKey(key - f64::EPSILON)
                                    ..=NumericJoinKey(key + f64::EPSILON),
                            )
                            .map(|(_, payload)| payload)
                            .next(),
                    }
                }
            }
            JoinKey::NonNumeric(key) => self.non_numeric_keys.get(&key),
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

/// Cap on LIMIT k for decorrelated LATERAL top-k subqueries; bounds the
/// lookup table at k rows per key.
const LATERAL_TOP_K_LIMIT: usize = 16;

/// A correlated LATERAL top-k subquery hoisted out of the per-outer-row
/// loop: the subquery runs once without its correlation conjunct, globally
/// ordered by its ORDER BY, and the first k rows per correlation key become
/// the lookup payload.
pub(super) struct LateralTopRows {
    probe_index: usize,
    rows_by_key: JoinKeyTable<Vec<Vec<Value>>>,
}

impl LateralTopRows {
    pub(super) fn rows_for(&self, outer_row: &[Value]) -> Result<&[Vec<Value>], RustqlError> {
        let probe = outer_row.get(self.probe_index).unwrap_or(&Value::Null);
        Ok(self
            .rows_by_key
            .lookup(probe)?
            .map(|rows| rows.as_slice())
            .unwrap_or(&[]))
    }
}

/// Attempts to decorrelate a LATERAL subquery of the shape
/// `SELECT ... WHERE key = outer [AND local ...] ORDER BY local LIMIT k`.
/// The rewritten subquery keeps the ORDER BY, so the engine's own global
/// sort defines per-key ordering and tie behavior; the first k rows per key
/// reproduce the per-outer-row LIMIT. Returns None for every other shape.
pub(super) fn prepare_lateral_top_rows(
    db: &dyn DatabaseCatalog,
    subquery: &SelectStatement,
    outer_columns: &[ColumnDefinition],
    outer_row_count: usize,
) -> Option<LateralTopRows> {
    if outer_row_count < DECORRELATE_MIN_OUTER_ROWS {
        return None;
    }
    // FETCH FIRST k ROWS ONLY is LIMIT k; WITH TIES changes row counts.
    let limit = match (subquery.limit, subquery.fetch.as_ref()) {
        (Some(limit), None) => limit,
        (None, Some(fetch)) if !fetch.with_ties => fetch.count,
        _ => return None,
    };
    if limit == 0 || limit > LATERAL_TOP_K_LIMIT {
        return None;
    }
    if subquery.group_by.is_some()
        || subquery.having.is_some()
        || subquery.distinct
        || subquery.distinct_on.is_some()
        || subquery.offset.is_some()
        || subquery.order_by.is_none()
    {
        return None;
    }
    // Aggregates or window functions in the select list evaluate over a
    // different row set once the correlation conjunct is removed.
    if !subquery.columns.iter().all(|column| match column {
        Column::Named { .. } => true,
        Column::Expression { expr, .. } => !expression_contains_aggregate_or_window(expr),
        Column::All | Column::Function(_) | Column::Subquery(_) => false,
    }) {
        return None;
    }

    let binding = outer_value_binding(db, subquery, outer_columns)?;
    if !binding.is_correlated() {
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

    let mut ordered_select = subquery.clone();
    ordered_select.columns.push(Column::Named {
        name: local_key.to_string(),
        alias: None,
    });
    ordered_select.where_clause = combine_conjunct_refs(&residual);
    ordered_select.limit = None;
    ordered_select.fetch = None;

    // The rewritten subquery must be fully local: any outer reference left
    // outside the dropped equality conjunct makes the rewrite unsound.
    let ordered_binding = outer_value_binding(db, &ordered_select, outer_columns)?;
    if ordered_binding.is_correlated() {
        return None;
    }

    let result = execute_planned_select(db, &ordered_select).ok()?;
    if result.columns.len() != subquery.columns.len() + 1 {
        return None;
    }

    let mut rows_by_key: JoinKeyTable<Vec<Vec<Value>>> = JoinKeyTable::empty();
    for mut row in result.rows {
        let key = row.pop()?;
        let Some(rows) = rows_by_key.slot(&key, Vec::new).ok()? else {
            continue;
        };
        if rows.len() < limit {
            rows.push(row);
        }
    }
    if !rows_by_key.numeric_keys_unambiguous() {
        return None;
    }

    Some(LateralTopRows {
        probe_index,
        rows_by_key,
    })
}

fn expression_contains_aggregate_or_window(expr: &Expression) -> bool {
    match expr {
        Expression::Function(_) | Expression::WindowFunction { .. } => true,
        Expression::BinaryOp { left, right, .. }
        | Expression::IsDistinctFrom { left, right, .. } => {
            expression_contains_aggregate_or_window(left)
                || expression_contains_aggregate_or_window(right)
        }
        Expression::UnaryOp { expr, .. }
        | Expression::IsNull { expr, .. }
        | Expression::Cast { expr, .. } => expression_contains_aggregate_or_window(expr),
        Expression::In { left, values } => {
            expression_contains_aggregate_or_window(left)
                || values.iter().any(expression_contains_aggregate_or_window)
        }
        Expression::Any { left, .. } | Expression::All { left, .. } => {
            expression_contains_aggregate_or_window(left)
        }
        Expression::Case {
            operand,
            when_clauses,
            else_clause,
        } => {
            operand
                .as_deref()
                .is_some_and(expression_contains_aggregate_or_window)
                || when_clauses.iter().any(|(condition, result)| {
                    expression_contains_aggregate_or_window(condition)
                        || expression_contains_aggregate_or_window(result)
                })
                || else_clause
                    .as_deref()
                    .is_some_and(expression_contains_aggregate_or_window)
        }
        Expression::ScalarFunction { args, .. } => {
            args.iter().any(expression_contains_aggregate_or_window)
        }
        Expression::Subquery(_) | Expression::Exists(_) => true,
        Expression::Column(_) | Expression::Value(_) | Expression::Default => false,
    }
}
