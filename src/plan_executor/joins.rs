use super::*;
use crate::executor::expr::compare_values;
use std::collections::HashMap;

impl<'a> PlanExecutor<'a> {
    pub(super) fn execute_nested_loop_join(
        &self,
        left: ExecutionResult,
        right: ExecutionResult,
        join_type: &JoinType,
        condition: &Expression,
    ) -> Result<ExecutionResult, RustqlError> {
        let mut joined_rows = Vec::with_capacity(nested_loop_join_row_capacity(
            left.rows.len(),
            right.rows.len(),
            join_type,
        ));
        let joined_columns = joined_column_names(&left.columns, &right.columns);
        let mut matched_right =
            should_track_unmatched_right_rows(join_type).then(|| vec![false; right.rows.len()]);
        let combined_columns = (!matches!(join_type, JoinType::Cross))
            .then(|| combined_column_definitions(&left.columns, &right.columns));

        for left_row in &left.rows {
            let mut has_match = false;
            for (right_idx, right_row) in right.rows.iter().enumerate() {
                let combined_row = combine_rows(left_row, right_row);

                let include = if matches!(join_type, JoinType::Cross) {
                    true
                } else {
                    let combined_columns = combined_columns.as_ref().ok_or_else(|| {
                        RustqlError::Internal(
                            "Join condition evaluation is missing combined columns".to_string(),
                        )
                    })?;
                    self.evaluate_expression(condition, combined_columns, &combined_row)?
                };

                if include {
                    joined_rows.push(combined_row);
                    has_match = true;
                    if let Some(matched_right) = matched_right.as_mut() {
                        matched_right[right_idx] = true;
                    }
                }
            }

            if matches!(join_type, JoinType::Left | JoinType::Full) && !has_match {
                joined_rows.push(combine_row_with_right_nulls(left_row, right.columns.len()));
            }
        }

        if let Some(matched_right) = matched_right {
            for (right_idx, right_row) in right.rows.iter().enumerate() {
                if !matched_right[right_idx] {
                    joined_rows.push(combine_row_with_left_nulls(left.columns.len(), right_row));
                }
            }
        }

        Ok(ExecutionResult {
            columns: joined_columns,
            rows: joined_rows,
        })
    }

    pub(super) fn execute_lateral_join(
        &self,
        left: ExecutionResult,
        subquery: &SelectStatement,
        alias: &str,
        right_columns: &[String],
        join_type: &JoinType,
        condition: &Expression,
    ) -> Result<ExecutionResult, RustqlError> {
        let outer_scope_columns = column_definitions_from_names(&left.columns);
        let mut joined_rows =
            Vec::with_capacity(lateral_join_row_capacity(left.rows.len(), join_type));
        let joined_columns = joined_column_names(&left.columns, right_columns);
        let combined_columns = combined_column_definitions(&left.columns, right_columns);
        let mut context = LateralMatchContext {
            right_columns,
            combined_columns: &combined_columns,
            condition,
            join_type,
            joined_rows: &mut joined_rows,
        };

        if let Some(binding) = outer_value_binding(self.db, subquery, &outer_scope_columns) {
            self.execute_lateral_join_bound(&left, subquery, &binding, &mut context)?;
        } else {
            self.execute_lateral_join_scoped(
                &left,
                subquery,
                alias,
                outer_scope_columns,
                &mut context,
            )?;
        }

        Ok(ExecutionResult {
            columns: joined_columns,
            rows: joined_rows,
        })
    }

    /// Fast path: outer references are bound to the current outer row's
    /// literal values and the subquery runs against the real catalog, so the
    /// planner can pick index scans on the inner table. An uncorrelated
    /// subquery is executed once and reused for every outer row.
    fn execute_lateral_join_bound(
        &self,
        left: &ExecutionResult,
        subquery: &SelectStatement,
        binding: &OuterValueBinding,
        context: &mut LateralMatchContext<'_, '_>,
    ) -> Result<(), RustqlError> {
        if left.rows.is_empty() {
            return Ok(());
        }

        if !binding.is_correlated() {
            match execute_planned_select(self.db, subquery) {
                Ok(shared_result) => {
                    for left_row in &left.rows {
                        self.append_lateral_matches(left_row, Ok(&shared_result), context)?;
                    }
                }
                Err(err) => {
                    if !matches!(context.join_type, JoinType::Left | JoinType::Full) {
                        return Err(err);
                    }
                    for left_row in &left.rows {
                        context.joined_rows.push(combine_row_with_right_nulls(
                            left_row,
                            context.right_columns.len(),
                        ));
                    }
                }
            }
            return Ok(());
        }

        for left_row in &left.rows {
            match execute_planned_select(self.db, &binding.bind(subquery, left_row)) {
                Ok(result) => self.append_lateral_matches(left_row, Ok(&result), context)?,
                Err(err) => self.append_lateral_matches(left_row, Err(err), context)?,
            }
        }
        Ok(())
    }

    /// Fallback path for subquery shapes the literal binding does not cover:
    /// expose the outer row through a scoped temp table joined into the
    /// subquery.
    fn execute_lateral_join_scoped(
        &self,
        left: &ExecutionResult,
        subquery: &SelectStatement,
        alias: &str,
        outer_scope_columns: Vec<ColumnDefinition>,
        context: &mut LateralMatchContext<'_, '_>,
    ) -> Result<(), RustqlError> {
        let temp_table_name = format!("__lateral_outer_{}", alias);
        let rewritten_subquery = lateral_subquery_with_outer_scope(subquery, &temp_table_name);
        let mut scoped_db = ScopedDatabase::new(self.db, temp_table_name, outer_scope_columns);

        for left_row in &left.rows {
            scoped_db.update_temp_row(left_row);
            match execute_planned_select(&scoped_db, &rewritten_subquery) {
                Ok(result) => self.append_lateral_matches(left_row, Ok(&result), context)?,
                Err(err) => self.append_lateral_matches(left_row, Err(err), context)?,
            }
        }
        Ok(())
    }

    fn append_lateral_matches(
        &self,
        left_row: &[Value],
        subquery_result: Result<&ExecutionResult, RustqlError>,
        context: &mut LateralMatchContext<'_, '_>,
    ) -> Result<(), RustqlError> {
        let subquery_result = match subquery_result {
            Ok(result) => result,
            Err(err) => {
                if matches!(context.join_type, JoinType::Left | JoinType::Full) {
                    context.joined_rows.push(combine_row_with_right_nulls(
                        left_row,
                        context.right_columns.len(),
                    ));
                    return Ok(());
                }
                return Err(err);
            }
        };

        if subquery_result.columns.len() != context.right_columns.len() {
            return Err(RustqlError::Internal(
                "LATERAL subquery output shape changed during execution".to_string(),
            ));
        }

        let mut has_match = false;
        for right_row in &subquery_result.rows {
            let combined_row = combine_rows(left_row, right_row);
            let include = self.evaluate_expression(
                context.condition,
                context.combined_columns,
                &combined_row,
            )?;

            if include {
                context.joined_rows.push(combined_row);
                has_match = true;
            }
        }

        if matches!(context.join_type, JoinType::Left | JoinType::Full) && !has_match {
            context.joined_rows.push(combine_row_with_right_nulls(
                left_row,
                context.right_columns.len(),
            ));
        }

        Ok(())
    }

    pub(super) fn execute_hash_join(
        &self,
        left: ExecutionResult,
        right: ExecutionResult,
        condition: &Expression,
    ) -> Result<ExecutionResult, RustqlError> {
        let left_is_build = left.rows.len() <= right.rows.len();
        let (build, probe, build_cols, probe_cols) = if left_is_build {
            (&left, &right, &left.columns, &right.columns)
        } else {
            (&right, &left, &right.columns, &left.columns)
        };

        let (build_key_idx, probe_key_idx) =
            self.extract_join_keys(condition, build_cols, probe_cols)?;

        let mut integer_table: HashMap<u64, Vec<usize>> = HashMap::new();
        let mut float_table: BTreeMap<NumericJoinKey, Vec<usize>> = BTreeMap::new();
        let mut non_numeric_table: HashMap<NonNumericJoinKey, Vec<usize>> = HashMap::new();
        for (row_idx, row) in build.rows.iter().enumerate() {
            if build_key_idx < row.len() {
                match join_key(&row[build_key_idx]) {
                    Some(JoinKey::Integer(key)) => {
                        integer_table
                            .entry(numeric_key_bits(key))
                            .or_default()
                            .push(row_idx);
                    }
                    Some(JoinKey::Float(key)) => {
                        float_table
                            .entry(NumericJoinKey(key))
                            .or_default()
                            .push(row_idx);
                    }
                    Some(JoinKey::NonNumeric(key)) => {
                        non_numeric_table.entry(key).or_default().push(row_idx);
                    }
                    None => {}
                }
            }
        }

        let mut joined_rows =
            Vec::with_capacity(hash_join_row_capacity(build.rows.len(), probe.rows.len()));
        let joined_columns = joined_column_names(&left.columns, &right.columns);
        let combined_columns = combined_column_definitions(&left.columns, &right.columns);
        let match_context = HashJoinMatchContext {
            build,
            left_is_build,
            condition,
            combined_columns: &combined_columns,
            left_column_count: left.columns.len(),
            key_equality: hash_join_key_equality(condition, &combined_columns),
        };

        for probe_row in &probe.rows {
            if probe_key_idx < probe_row.len() {
                match join_key(&probe_row[probe_key_idx]) {
                    Some(JoinKey::Integer(probe_key) | JoinKey::Float(probe_key)) => {
                        if let Some(build_row_indices) =
                            integer_table.get(&numeric_key_bits(probe_key))
                        {
                            self.append_hash_join_matches(
                                &match_context,
                                probe_row,
                                build_row_indices,
                                &mut joined_rows,
                            )?;
                        }

                        let nearest_integer = probe_key.round();
                        if (probe_key - nearest_integer).abs() < f64::EPSILON
                            && numeric_key_bits(nearest_integer) != numeric_key_bits(probe_key)
                            && let Some(build_row_indices) =
                                integer_table.get(&numeric_key_bits(nearest_integer))
                        {
                            self.append_hash_join_matches(
                                &match_context,
                                probe_row,
                                build_row_indices,
                                &mut joined_rows,
                            )?;
                        }

                        if !float_table.is_empty() {
                            let lower = NumericJoinKey(probe_key - f64::EPSILON);
                            let upper = NumericJoinKey(probe_key + f64::EPSILON);
                            for build_row_indices in float_table
                                .range(lower..=upper)
                                .map(|(_, row_indices)| row_indices)
                            {
                                self.append_hash_join_matches(
                                    &match_context,
                                    probe_row,
                                    build_row_indices,
                                    &mut joined_rows,
                                )?;
                            }
                        }
                    }
                    Some(JoinKey::NonNumeric(probe_key)) => {
                        if let Some(build_row_indices) = non_numeric_table.get(&probe_key) {
                            self.append_hash_join_matches(
                                &match_context,
                                probe_row,
                                build_row_indices,
                                &mut joined_rows,
                            )?;
                        }
                    }
                    None => {}
                }
            }
        }

        Ok(ExecutionResult {
            columns: joined_columns,
            rows: joined_rows,
        })
    }

    fn append_hash_join_matches(
        &self,
        context: &HashJoinMatchContext<'_>,
        probe_row: &[Value],
        build_row_indices: &[usize],
        joined_rows: &mut Vec<Vec<Value>>,
    ) -> Result<(), RustqlError> {
        for &build_row_idx in build_row_indices {
            let build_row = &context.build.rows[build_row_idx];
            let (left_row, right_row) = if context.left_is_build {
                (build_row.as_slice(), probe_row)
            } else {
                (probe_row, build_row.as_slice())
            };

            if let Some((left_value_idx, right_value_idx)) = context.key_equality {
                let left_value = combined_cell(
                    left_row,
                    right_row,
                    context.left_column_count,
                    left_value_idx,
                );
                let right_value = combined_cell(
                    left_row,
                    right_row,
                    context.left_column_count,
                    right_value_idx,
                );
                if !matches!(left_value, Value::Null)
                    && !matches!(right_value, Value::Null)
                    && compare_values(left_value, &BinaryOperator::Equal, right_value)?
                {
                    joined_rows.push(combine_rows(left_row, right_row));
                }
            } else {
                let combined_row = combine_rows(left_row, right_row);
                if self.evaluate_expression(
                    context.condition,
                    context.combined_columns,
                    &combined_row,
                )? {
                    joined_rows.push(combined_row);
                }
            }
        }

        Ok(())
    }

    fn extract_join_keys(
        &self,
        condition: &Expression,
        build_cols: &[String],
        probe_cols: &[String],
    ) -> Result<(usize, usize), RustqlError> {
        if let Expression::BinaryOp {
            left,
            op: BinaryOperator::Equal,
            right,
        } = condition
            && let (Expression::Column(left_col), Expression::Column(right_col)) =
                (left.as_ref(), right.as_ref())
        {
            let build_idx = hash_join_column_index(build_cols, left_col);
            let probe_idx = hash_join_column_index(probe_cols, right_col);

            if let (Some(bi), Some(pi)) = (build_idx, probe_idx) {
                return Ok((bi, pi));
            }

            let swapped_build_idx = hash_join_column_index(build_cols, right_col);
            let swapped_probe_idx = hash_join_column_index(probe_cols, left_col);

            if let (Some(bi), Some(pi)) = (swapped_build_idx, swapped_probe_idx) {
                return Ok((bi, pi));
            }
        }
        Err(RustqlError::Internal(
            "Could not extract join keys from condition".to_string(),
        ))
    }
}

fn nested_loop_join_row_capacity(
    left_row_count: usize,
    right_row_count: usize,
    join_type: &JoinType,
) -> usize {
    match join_type {
        JoinType::Cross => left_row_count
            .checked_mul(right_row_count)
            .unwrap_or_else(|| left_row_count.max(right_row_count)),
        JoinType::Left => left_row_count,
        JoinType::Right => right_row_count,
        JoinType::Full => left_row_count.max(right_row_count),
        JoinType::Inner | JoinType::Natural => left_row_count.min(right_row_count),
    }
}

fn lateral_join_row_capacity(left_row_count: usize, join_type: &JoinType) -> usize {
    if matches!(join_type, JoinType::Left | JoinType::Full) {
        left_row_count
    } else {
        0
    }
}

fn hash_join_row_capacity(build_row_count: usize, probe_row_count: usize) -> usize {
    build_row_count.min(probe_row_count)
}

fn should_track_unmatched_right_rows(join_type: &JoinType) -> bool {
    matches!(join_type, JoinType::Right | JoinType::Full)
}

fn hash_join_column_index(columns: &[String], reference: &str) -> Option<usize> {
    if reference.contains('.') {
        return columns.iter().position(|column| column == reference);
    }

    let mut matches = columns
        .iter()
        .enumerate()
        .filter(|(_, column)| unqualified_column_name(column) == reference)
        .map(|(idx, _)| idx);
    let first = matches.next()?;

    if matches.next().is_some() {
        None
    } else {
        Some(first)
    }
}

fn joined_column_names(left: &[String], right: &[String]) -> Vec<String> {
    let mut columns = Vec::with_capacity(left.len() + right.len());
    columns.extend(left.iter().cloned());
    columns.extend(right.iter().cloned());
    columns
}

struct LateralMatchContext<'a, 'rows> {
    right_columns: &'a [String],
    combined_columns: &'a [ColumnDefinition],
    condition: &'a Expression,
    join_type: &'a JoinType,
    joined_rows: &'rows mut Vec<Vec<Value>>,
}

struct HashJoinMatchContext<'a> {
    build: &'a ExecutionResult,
    left_is_build: bool,
    condition: &'a Expression,
    combined_columns: &'a [ColumnDefinition],
    left_column_count: usize,
    key_equality: Option<(usize, usize)>,
}

fn combined_cell<'a>(
    left_row: &'a [Value],
    right_row: &'a [Value],
    left_column_count: usize,
    index: usize,
) -> &'a Value {
    if index < left_column_count {
        &left_row[index]
    } else {
        &right_row[index - left_column_count]
    }
}

/// Pre-resolves a `column = column` join condition to combined-row cell
/// indices so each candidate pair can be checked without building the
/// combined row or re-resolving column names. Returns None for conditions
/// the generic evaluator must handle.
fn hash_join_key_equality(
    condition: &Expression,
    combined_columns: &[ColumnDefinition],
) -> Option<(usize, usize)> {
    if let Expression::BinaryOp {
        left,
        op: BinaryOperator::Equal,
        right,
    } = condition
        && let (Expression::Column(left_col), Expression::Column(right_col)) =
            (left.as_ref(), right.as_ref())
        && left_col != "*"
        && right_col != "*"
    {
        let left_idx = resolve_combined_column(combined_columns, left_col)?;
        let right_idx = resolve_combined_column(combined_columns, right_col)?;
        return Some((left_idx, right_idx));
    }
    None
}

/// Mirrors the expression evaluator's column resolution (exact name first,
/// then unqualified-suffix matching) so the fast path picks the same cells
/// the evaluator would.
fn resolve_combined_column(columns: &[ColumnDefinition], name: &str) -> Option<usize> {
    if let Some(idx) = columns.iter().position(|c| c.name == name) {
        return Some(idx);
    }
    if name.contains('.') {
        let col_name = name.split('.').next_back().unwrap_or(name);
        columns.iter().position(|c| {
            c.name == col_name || c.name.split('.').next_back().unwrap_or(&c.name) == col_name
        })
    } else {
        columns
            .iter()
            .position(|c| c.name.split('.').next_back().unwrap_or(&c.name) == name)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct NumericJoinKey(f64);

impl Eq for NumericJoinKey {}

impl PartialOrd for NumericJoinKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NumericJoinKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if self.0 == other.0 {
            std::cmp::Ordering::Equal
        } else if self.0 < other.0 {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum NonNumericJoinKey {
    Text(String),
    Boolean(bool),
    Date(String),
    Time(String),
    DateTime(String),
}

enum JoinKey {
    /// Integer key, carried as its `as f64` cast because the expression
    /// evaluator compares all numerics through f64.
    Integer(f64),
    Float(f64),
    NonNumeric(NonNumericJoinKey),
}

/// Hash key for numeric build values: exact f64 bit pattern, with -0.0
/// collapsed onto 0.0 so the two zero encodings share one table entry.
fn numeric_key_bits(value: f64) -> u64 {
    (value + 0.0).to_bits()
}

fn join_key(value: &Value) -> Option<JoinKey> {
    match value {
        Value::Null => None,
        Value::Integer(value) => Some(JoinKey::Integer(*value as f64)),
        Value::Float(value) if value.is_finite() => Some(JoinKey::Float(*value)),
        Value::Float(_) => None,
        Value::Text(value) => Some(JoinKey::NonNumeric(NonNumericJoinKey::Text(value.clone()))),
        Value::Boolean(value) => Some(JoinKey::NonNumeric(NonNumericJoinKey::Boolean(*value))),
        Value::Date(value) => Some(JoinKey::NonNumeric(NonNumericJoinKey::Date(value.clone()))),
        Value::Time(value) => Some(JoinKey::NonNumeric(NonNumericJoinKey::Time(value.clone()))),
        Value::DateTime(value) => Some(JoinKey::NonNumeric(NonNumericJoinKey::DateTime(
            value.clone(),
        ))),
    }
}
