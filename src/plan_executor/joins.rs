use super::*;

impl<'a> PlanExecutor<'a> {
    pub(super) fn execute_nested_loop_join(
        &self,
        left: ExecutionResult,
        right: ExecutionResult,
        join_type: &JoinType,
        condition: &Expression,
    ) -> Result<ExecutionResult, RustqlError> {
        let mut joined_rows = Vec::new();
        let mut joined_columns = left.columns.clone();
        joined_columns.extend(right.columns.clone());
        let mut matched_right = vec![false; right.rows.len()];
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
                    matched_right[right_idx] = true;
                }
            }

            if matches!(join_type, JoinType::Left | JoinType::Full) && !has_match {
                joined_rows.push(combine_row_with_right_nulls(left_row, right.columns.len()));
            }
        }

        if matches!(join_type, JoinType::Right | JoinType::Full) {
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
        let mut joined_rows = Vec::new();
        let mut joined_columns = left.columns.clone();
        joined_columns.extend(right_columns.iter().cloned());
        let temp_table_name = format!("__lateral_outer_{}", alias);
        let rewritten_subquery = lateral_subquery_with_outer_scope(subquery, &temp_table_name);
        let mut scoped_db =
            ScopedDatabase::new(self.db, temp_table_name, outer_scope_columns.clone());
        let combined_columns = combined_column_definitions(&left.columns, right_columns);

        for left_row in &left.rows {
            scoped_db.update_temp_row(left_row);

            let subquery_result = match execute_planned_select(&scoped_db, &rewritten_subquery) {
                Ok(result) => result,
                Err(err) => {
                    if matches!(join_type, JoinType::Left | JoinType::Full) {
                        joined_rows
                            .push(combine_row_with_right_nulls(left_row, right_columns.len()));
                        continue;
                    }
                    return Err(err);
                }
            };

            if subquery_result.columns.len() != right_columns.len() {
                return Err(RustqlError::Internal(
                    "LATERAL subquery output shape changed during execution".to_string(),
                ));
            }

            let mut has_match = false;
            for right_row in &subquery_result.rows {
                let combined_row = combine_rows(left_row, right_row);
                let include =
                    self.evaluate_expression(condition, &combined_columns, &combined_row)?;

                if include {
                    joined_rows.push(combined_row);
                    has_match = true;
                }
            }

            if matches!(join_type, JoinType::Left | JoinType::Full) && !has_match {
                joined_rows.push(combine_row_with_right_nulls(left_row, right_columns.len()));
            }
        }

        Ok(ExecutionResult {
            columns: joined_columns,
            rows: joined_rows,
        })
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

        let mut numeric_table: BTreeMap<NumericJoinKey, Vec<usize>> = BTreeMap::new();
        let mut non_numeric_table: BTreeMap<NonNumericJoinKey, Vec<usize>> = BTreeMap::new();
        for (row_idx, row) in build.rows.iter().enumerate() {
            if build_key_idx < row.len() {
                match join_key(&row[build_key_idx]) {
                    Some(JoinKey::Numeric(key)) => {
                        numeric_table.entry(key).or_default().push(row_idx);
                    }
                    Some(JoinKey::NonNumeric(key)) => {
                        non_numeric_table.entry(key).or_default().push(row_idx);
                    }
                    None => {}
                }
            }
        }

        let mut joined_rows = Vec::new();
        let mut joined_columns = left.columns.clone();
        joined_columns.extend(right.columns.clone());
        let combined_columns = combined_column_definitions(&left.columns, &right.columns);
        let match_context = HashJoinMatchContext {
            build,
            left_is_build,
            condition,
            combined_columns: &combined_columns,
        };

        for probe_row in &probe.rows {
            if probe_key_idx < probe_row.len() {
                match join_key(&probe_row[probe_key_idx]) {
                    Some(JoinKey::Numeric(probe_key)) => {
                        let lower = NumericJoinKey(probe_key.0 - f64::EPSILON);
                        let upper = NumericJoinKey(probe_key.0 + f64::EPSILON);
                        for build_row_indices in numeric_table
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
            let combined_row = if context.left_is_build {
                combine_rows(build_row, probe_row)
            } else {
                combine_rows(probe_row, build_row)
            };

            if self.evaluate_expression(
                context.condition,
                context.combined_columns,
                &combined_row,
            )? {
                joined_rows.push(combined_row);
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

struct HashJoinMatchContext<'a> {
    build: &'a ExecutionResult,
    left_is_build: bool,
    condition: &'a Expression,
    combined_columns: &'a [ColumnDefinition],
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

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum NonNumericJoinKey {
    Text(String),
    Boolean(bool),
    Date(String),
    Time(String),
    DateTime(String),
}

enum JoinKey {
    Numeric(NumericJoinKey),
    NonNumeric(NonNumericJoinKey),
}

fn join_key(value: &Value) -> Option<JoinKey> {
    match value {
        Value::Null => None,
        Value::Integer(value) => Some(JoinKey::Numeric(NumericJoinKey(*value as f64))),
        Value::Float(value) if value.is_finite() => Some(JoinKey::Numeric(NumericJoinKey(*value))),
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
