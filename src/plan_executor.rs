use crate::ast::*;
use crate::database::{DatabaseCatalog, RowId, ScopedDatabase};
use crate::error::RustqlError;
use crate::executor::aggregate::DEFAULT_PERCENTILE_FRACTION;
use crate::executor::expr::{
    apply_arithmetic, compare_values, compare_values_same_type, evaluate_expression,
    evaluate_value_expression_with_db,
};
use crate::executor::select::execute_select_internal;
use crate::planner::PlanNode;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, HashSet};

#[derive(Debug)]
pub struct ExecutionResult {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
}

pub struct PlanExecutor<'a> {
    db: &'a dyn DatabaseCatalog,
}

impl<'a> PlanExecutor<'a> {
    pub fn new(db: &'a dyn DatabaseCatalog) -> Self {
        PlanExecutor { db }
    }

    pub fn execute(
        &self,
        plan: &PlanNode,
        select_stmt: &SelectStatement,
    ) -> Result<ExecutionResult, RustqlError> {
        let result = self.execute_plan_node(plan)?;

        let projected = self.apply_projection(&result, select_stmt)?;
        let distincted = if select_stmt.distinct {
            self.apply_distinct(projected)?
        } else {
            projected
        };

        Ok(distincted)
    }

    fn execute_plan_node(&self, plan: &PlanNode) -> Result<ExecutionResult, RustqlError> {
        match plan {
            PlanNode::OneRow { .. } => Ok(ExecutionResult {
                columns: Vec::new(),
                rows: vec![Vec::new()],
            }),
            PlanNode::SeqScan {
                table,
                output_label,
                filter,
                ..
            } => self.execute_seq_scan(table, output_label.as_deref(), filter.as_ref()),
            PlanNode::IndexScan {
                table,
                index,
                output_label,
                filter,
                ..
            } => self.execute_index_scan(table, index, output_label.as_deref(), filter.as_ref()),
            PlanNode::FunctionScan {
                function,
                output_label,
                filter,
                ..
            } => self.execute_function_scan(function, output_label.as_deref(), filter.as_ref()),
            PlanNode::ValuesScan {
                values,
                columns,
                filter,
                ..
            } => self.execute_values_scan(values, columns, filter.as_ref()),
            PlanNode::Filter {
                input, condition, ..
            } => {
                let input_result = self.execute_plan_node(input)?;
                self.execute_filter(input_result, condition)
            }
            PlanNode::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
                ..
            } => {
                let left_result = self.execute_plan_node(left)?;
                let right_result = self.execute_plan_node(right)?;
                self.execute_nested_loop_join(left_result, right_result, join_type, condition)
            }
            PlanNode::HashJoin {
                left,
                right,
                condition,
                ..
            } => {
                let left_result = self.execute_plan_node(left)?;
                let right_result = self.execute_plan_node(right)?;
                self.execute_hash_join(left_result, right_result, condition)
            }
            PlanNode::LateralJoin {
                left,
                subquery,
                alias,
                right_columns,
                join_type,
                condition,
                ..
            } => {
                let left_result = self.execute_plan_node(left)?;
                self.execute_lateral_join(
                    left_result,
                    subquery,
                    alias,
                    right_columns,
                    join_type,
                    condition,
                )
            }
            PlanNode::Sort {
                input, order_by, ..
            } => {
                let input_result = self.execute_plan_node(input)?;
                self.execute_sort(input_result, order_by)
            }
            PlanNode::DistinctOn {
                input, distinct_on, ..
            } => {
                let input_result = self.execute_plan_node(input)?;
                self.execute_distinct_on(input_result, distinct_on)
            }
            PlanNode::Limit {
                input,
                limit,
                offset,
                with_ties,
                order_by,
                ..
            } => {
                let input_result = self.execute_plan_node(input)?;
                self.execute_limit(input_result, *limit, *offset, *with_ties, order_by)
            }
            PlanNode::Aggregate {
                input,
                group_by,
                grouping_sets,
                aggregates,
                having,
                ..
            } => {
                let input_result = self.execute_plan_node(input)?;
                self.execute_aggregate(
                    input_result,
                    group_by,
                    grouping_sets.as_deref(),
                    aggregates,
                    having.as_ref(),
                )
            }
        }
    }

    fn execute_values_scan(
        &self,
        values: &[Vec<Expression>],
        columns: &[String],
        filter: Option<&Expression>,
    ) -> Result<ExecutionResult, RustqlError> {
        let empty_columns: Vec<ColumnDefinition> = Vec::new();
        let empty_row: Vec<Value> = Vec::new();
        let output_columns: Vec<ColumnDefinition> = columns
            .iter()
            .map(|name| ColumnDefinition {
                name: name.clone(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            })
            .collect();

        let mut rows = Vec::with_capacity(values.len());
        for value_row in values {
            let row: Vec<Value> = value_row
                .iter()
                .map(|expr| self.evaluate_value_expression(expr, &empty_columns, &empty_row))
                .collect::<Result<_, _>>()?;
            let include = if let Some(filter_expr) = filter {
                self.evaluate_expression(filter_expr, &output_columns, &row)?
            } else {
                true
            };
            if include {
                rows.push(row);
            }
        }

        Ok(ExecutionResult {
            columns: columns.to_vec(),
            rows,
        })
    }

    fn execute_seq_scan(
        &self,
        table_name: &str,
        output_label: Option<&str>,
        filter: Option<&Expression>,
    ) -> Result<ExecutionResult, RustqlError> {
        let table = self
            .db
            .get_table(table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

        let mut rows = Vec::new();

        for row in &table.rows {
            let include = if let Some(filter_expr) = filter {
                self.evaluate_expression(filter_expr, &table.columns, row)?
            } else {
                true
            };

            if include {
                rows.push(row.clone());
            }
        }

        let columns = qualify_column_names(&table.columns, output_label);

        Ok(ExecutionResult { columns, rows })
    }

    fn execute_index_scan(
        &self,
        table_name: &str,
        index_name: &str,
        output_label: Option<&str>,
        filter: Option<&Expression>,
    ) -> Result<ExecutionResult, RustqlError> {
        let table = self
            .db
            .get_table(table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

        let row_ids = if let Some(filter_expr) = filter {
            if let Some(index_usage) =
                crate::executor::ddl::find_index_usage(self.db, table_name, filter_expr)
                    .filter(|usage| usage.index_name() == index_name)
            {
                crate::executor::ddl::get_indexed_rows(self.db, table, &index_usage)?
            } else {
                self.all_row_ids_for_index(index_name)?
            }
        } else {
            self.all_row_ids_for_index(index_name)?
        };

        let mut rows = Vec::new();
        for row_id in row_ids {
            if let Some(row) = table.row_by_id(row_id) {
                let include = if let Some(filter_expr) = filter {
                    self.evaluate_expression(filter_expr, &table.columns, row)?
                } else {
                    true
                };

                if include {
                    rows.push(row.clone());
                }
            }
        }

        let columns = qualify_column_names(&table.columns, output_label);

        Ok(ExecutionResult { columns, rows })
    }

    fn all_row_ids_for_index(&self, index_name: &str) -> Result<HashSet<RowId>, RustqlError> {
        if let Some(index) = self.db.get_index(index_name) {
            let mut row_ids = HashSet::new();
            for rows in index.entries.values() {
                row_ids.extend(rows.iter().copied());
            }
            return Ok(row_ids);
        }

        if let Some(index) = self.db.get_composite_index(index_name) {
            let mut row_ids = HashSet::new();
            for rows in index.entries.values() {
                row_ids.extend(rows.iter().copied());
            }
            return Ok(row_ids);
        }

        Err(RustqlError::Internal(format!(
            "Index '{}' does not exist",
            index_name
        )))
    }

    fn execute_function_scan(
        &self,
        function: &TableFunction,
        output_label: Option<&str>,
        filter: Option<&Expression>,
    ) -> Result<ExecutionResult, RustqlError> {
        match function.name.as_str() {
            "generate_series" => {
                let empty_columns: Vec<ColumnDefinition> = Vec::new();
                let empty_row: Vec<Value> = Vec::new();

                let start = match self.evaluate_value_expression(
                    &function.args[0],
                    &empty_columns,
                    &empty_row,
                )? {
                    Value::Integer(value) => value,
                    _ => {
                        return Err(RustqlError::TypeMismatch(
                            "GENERATE_SERIES arguments must be integers".to_string(),
                        ));
                    }
                };
                let stop = match self.evaluate_value_expression(
                    &function.args[1],
                    &empty_columns,
                    &empty_row,
                )? {
                    Value::Integer(value) => value,
                    _ => {
                        return Err(RustqlError::TypeMismatch(
                            "GENERATE_SERIES arguments must be integers".to_string(),
                        ));
                    }
                };
                let step = if function.args.len() > 2 {
                    match self.evaluate_value_expression(
                        &function.args[2],
                        &empty_columns,
                        &empty_row,
                    )? {
                        Value::Integer(value) => value,
                        _ => {
                            return Err(RustqlError::TypeMismatch(
                                "GENERATE_SERIES step must be an integer".to_string(),
                            ));
                        }
                    }
                } else if start <= stop {
                    1
                } else {
                    -1
                };

                if step == 0 {
                    return Err(RustqlError::Internal(
                        "GENERATE_SERIES step cannot be zero".to_string(),
                    ));
                }

                let column_name = qualified_column_name(
                    output_label,
                    function.alias.as_deref().unwrap_or("generate_series"),
                );
                let columns = vec![ColumnDefinition {
                    name: column_name.clone(),
                    data_type: DataType::Integer,
                    nullable: false,
                    primary_key: false,
                    unique: false,
                    default_value: None,
                    foreign_key: None,
                    check: None,
                    auto_increment: false,
                    generated: None,
                }];

                let mut rows = Vec::new();
                let mut current = start;
                if step > 0 {
                    while current <= stop {
                        let row = vec![Value::Integer(current)];
                        let include = if let Some(filter_expr) = filter {
                            self.evaluate_expression(filter_expr, &columns, &row)?
                        } else {
                            true
                        };
                        if include {
                            rows.push(row);
                        }
                        current += step;
                    }
                } else {
                    while current >= stop {
                        let row = vec![Value::Integer(current)];
                        let include = if let Some(filter_expr) = filter {
                            self.evaluate_expression(filter_expr, &columns, &row)?
                        } else {
                            true
                        };
                        if include {
                            rows.push(row);
                        }
                        current += step;
                    }
                }

                Ok(ExecutionResult {
                    columns: vec![column_name],
                    rows,
                })
            }
            other => Err(RustqlError::Internal(format!(
                "Unsupported table function in plan executor: {}",
                other
            ))),
        }
    }

    fn execute_filter(
        &self,
        input: ExecutionResult,
        condition: &Expression,
    ) -> Result<ExecutionResult, RustqlError> {
        let mut filtered_rows = Vec::new();
        let columns = column_definitions_from_names(&input.columns);

        for row in &input.rows {
            let include = self.evaluate_expression(condition, &columns, row)?;
            if include {
                filtered_rows.push(row.clone());
            }
        }

        Ok(ExecutionResult {
            columns: input.columns,
            rows: filtered_rows,
        })
    }

    fn execute_nested_loop_join(
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

            if matches!(
                join_type,
                JoinType::Left | JoinType::Full | JoinType::Natural
            ) && !has_match
            {
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

    fn execute_lateral_join(
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

            let subquery_result =
                match execute_select_internal(None, rewritten_subquery.clone(), &scoped_db) {
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

            if subquery_result.headers.len() != right_columns.len() {
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

    fn execute_hash_join(
        &self,
        left: ExecutionResult,
        right: ExecutionResult,
        condition: &Expression,
    ) -> Result<ExecutionResult, RustqlError> {
        let (build, probe, build_cols, probe_cols) = if left.rows.len() <= right.rows.len() {
            (&left, &right, &left.columns, &right.columns)
        } else {
            (&right, &left, &right.columns, &left.columns)
        };

        let (build_key_idx, probe_key_idx) =
            self.extract_join_keys(condition, build_cols, probe_cols)?;

        let mut hash_table: BTreeMap<Value, Vec<usize>> = BTreeMap::new();
        for (row_idx, row) in build.rows.iter().enumerate() {
            if build_key_idx < row.len() {
                let key = row[build_key_idx].clone();
                hash_table.entry(key).or_default().push(row_idx);
            }
        }

        let mut joined_rows = Vec::new();
        let mut joined_columns = left.columns.clone();
        joined_columns.extend(right.columns.clone());

        for probe_row in &probe.rows {
            if probe_key_idx < probe_row.len() {
                let key = probe_row[probe_key_idx].clone();
                if let Some(build_row_indices) = hash_table.get(&key) {
                    for &build_row_idx in build_row_indices {
                        let build_row = &build.rows[build_row_idx];
                        let combined_row = if left.rows.len() <= right.rows.len() {
                            combine_rows(build_row, probe_row)
                        } else {
                            combine_rows(probe_row, build_row)
                        };
                        joined_rows.push(combined_row);
                    }
                }
            }
        }

        Ok(ExecutionResult {
            columns: joined_columns,
            rows: joined_rows,
        })
    }

    fn execute_sort(
        &self,
        input: ExecutionResult,
        order_by: &[OrderByExpr],
    ) -> Result<ExecutionResult, RustqlError> {
        let column_defs = column_definitions_from_names(&input.columns);
        let mut keyed_rows: Vec<(Vec<Value>, Vec<Value>)> = input
            .rows
            .into_iter()
            .map(|row| {
                let keys = order_by
                    .iter()
                    .map(|order_expr| {
                        self.get_sort_value(&order_expr.expr, &input.columns, &column_defs, &row)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok((keys, row))
            })
            .collect::<Result<Vec<_>, RustqlError>>()?;

        keyed_rows.sort_by(|(a_keys, _), (b_keys, _)| {
            for (idx, order_expr) in order_by.iter().enumerate() {
                let a_val = a_keys.get(idx).unwrap_or(&Value::Null);
                let b_val = b_keys.get(idx).unwrap_or(&Value::Null);
                let cmp = compare_values_same_type(a_val, b_val);
                if cmp != Ordering::Equal {
                    return if order_expr.asc { cmp } else { cmp.reverse() };
                }
            }
            Ordering::Equal
        });

        let rows = keyed_rows.into_iter().map(|(_, row)| row).collect();

        Ok(ExecutionResult {
            columns: input.columns,
            rows,
        })
    }

    fn execute_limit(
        &self,
        input: ExecutionResult,
        limit: usize,
        offset: usize,
        with_ties: bool,
        order_by: &[OrderByExpr],
    ) -> Result<ExecutionResult, RustqlError> {
        let mut rows = input.rows;

        if offset < rows.len() {
            rows = rows.split_off(offset);
        } else {
            rows.clear();
        }

        if rows.len() > limit {
            let limit_with_ties = if with_ties && !order_by.is_empty() && limit > 0 {
                let column_defs = column_definitions_from_names(&input.columns);
                let boundary_values = self.extract_order_values(
                    order_by,
                    &input.columns,
                    &column_defs,
                    &rows[limit - 1],
                )?;
                let mut extended_limit = limit;
                while extended_limit < rows.len() {
                    let values = self.extract_order_values(
                        order_by,
                        &input.columns,
                        &column_defs,
                        &rows[extended_limit],
                    )?;
                    if values != boundary_values {
                        break;
                    }
                    extended_limit += 1;
                }
                extended_limit
            } else {
                limit
            };

            rows.truncate(limit_with_ties);
        }

        Ok(ExecutionResult {
            columns: input.columns,
            rows,
        })
    }

    fn execute_distinct_on(
        &self,
        input: ExecutionResult,
        distinct_on: &[Expression],
    ) -> Result<ExecutionResult, RustqlError> {
        let column_defs: Vec<ColumnDefinition> = input
            .columns
            .iter()
            .map(|name| ColumnDefinition {
                name: name.clone(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            })
            .collect();

        let mut seen = BTreeSet::new();
        let mut rows = Vec::new();

        for row in input.rows {
            let key: Vec<Value> = distinct_on
                .iter()
                .map(|expr| {
                    self.evaluate_distinct_on_value(expr, &input.columns, &column_defs, &row)
                })
                .collect::<Result<_, _>>()?;

            if seen.insert(key) {
                rows.push(row);
            }
        }

        Ok(ExecutionResult {
            columns: input.columns,
            rows,
        })
    }

    fn execute_aggregate(
        &self,
        input: ExecutionResult,
        group_by: &[Expression],
        grouping_sets: Option<&[Vec<Expression>]>,
        aggregates: &[AggregateFunction],
        having: Option<&Expression>,
    ) -> Result<ExecutionResult, RustqlError> {
        let column_defs: Vec<ColumnDefinition> = input
            .columns
            .iter()
            .map(|name| ColumnDefinition {
                name: name.clone(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            })
            .collect();

        let group_by_names: Vec<String> = group_by
            .iter()
            .map(|expr| match expr {
                Expression::Column(name) => name.clone(),
                _ => format!("{:?}", expr),
            })
            .collect();
        let aggregate_names: Vec<String> = aggregates
            .iter()
            .map(|agg| format!("{:?}", agg.function))
            .collect();

        let result_column_defs: Vec<ColumnDefinition> = group_by_names
            .iter()
            .chain(aggregate_names.iter())
            .map(|name| ColumnDefinition {
                name: name.clone(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            })
            .collect();

        let mut result_rows = Vec::new();
        let build_groups = |exprs: &[Expression]| -> BTreeMap<Vec<Value>, Vec<&[Value]>> {
            let mut groups: BTreeMap<Vec<Value>, Vec<&[Value]>> = BTreeMap::new();
            for row in &input.rows {
                let key: Vec<Value> = exprs
                    .iter()
                    .map(|expr| {
                        self.evaluate_value_expression(expr, &column_defs, row)
                            .unwrap_or(Value::Null)
                    })
                    .collect();
                groups.entry(key).or_default().push(row.as_slice());
            }

            if groups.is_empty() && exprs.is_empty() {
                groups.insert(Vec::new(), Vec::new());
            }

            groups
        };

        if let Some(grouping_sets) = grouping_sets {
            for set in grouping_sets {
                let groups = build_groups(set);

                for group_rows in groups.into_values() {
                    let mut result_row = Vec::with_capacity(group_by.len() + aggregates.len());

                    for group_expr in group_by {
                        if set.iter().any(|active_expr| active_expr == group_expr) {
                            let value = group_rows
                                .first()
                                .map(|row| {
                                    self.evaluate_value_expression(group_expr, &column_defs, row)
                                })
                                .transpose()?
                                .unwrap_or(Value::Null);
                            result_row.push(value);
                        } else {
                            result_row.push(Value::Null);
                        }
                    }

                    let aggregate_values =
                        self.compute_group_aggregate_values(aggregates, &group_rows, &column_defs)?;
                    result_row.extend(aggregate_values.iter().cloned());

                    if let Some(having_expr) = having {
                        let having_context = HavingContext {
                            result_columns: &result_column_defs,
                            result_row: &result_row,
                            input_columns: &column_defs,
                            selected_aggregates: aggregates,
                            aggregate_values: &aggregate_values,
                            group_rows: &group_rows,
                        };
                        let include = self.evaluate_having(having_expr, &having_context)?;

                        if !include {
                            continue;
                        }
                    }

                    result_rows.push(result_row);
                }
            }
        } else {
            let groups = build_groups(group_by);

            for (group_key, group_rows) in groups {
                let mut result_row = group_key.clone();
                let aggregate_values =
                    self.compute_group_aggregate_values(aggregates, &group_rows, &column_defs)?;
                result_row.extend(aggregate_values.iter().cloned());

                if let Some(having_expr) = having {
                    let having_context = HavingContext {
                        result_columns: &result_column_defs,
                        result_row: &result_row,
                        input_columns: &column_defs,
                        selected_aggregates: aggregates,
                        aggregate_values: &aggregate_values,
                        group_rows: &group_rows,
                    };
                    let include = self.evaluate_having(having_expr, &having_context)?;

                    if !include {
                        continue;
                    }
                }

                result_rows.push(result_row);
            }
        }

        Ok(ExecutionResult {
            columns: group_by_names.into_iter().chain(aggregate_names).collect(),
            rows: result_rows,
        })
    }

    fn evaluate_expression(
        &self,
        expr: &Expression,
        columns: &[ColumnDefinition],
        row: &[Value],
    ) -> Result<bool, RustqlError> {
        evaluate_expression(Some(self.db), expr, columns, row)
    }

    fn evaluate_value_expression(
        &self,
        expr: &Expression,
        columns: &[ColumnDefinition],
        row: &[Value],
    ) -> Result<Value, RustqlError> {
        evaluate_value_expression_with_db(expr, columns, row, Some(self.db))
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
            let build_idx = build_cols
                .iter()
                .position(|c| column_names_match(c, left_col));
            let probe_idx = probe_cols
                .iter()
                .position(|c| column_names_match(c, right_col));

            if let (Some(bi), Some(pi)) = (build_idx, probe_idx) {
                return Ok((bi, pi));
            }

            let swapped_build_idx = build_cols
                .iter()
                .position(|c| column_names_match(c, right_col));
            let swapped_probe_idx = probe_cols
                .iter()
                .position(|c| column_names_match(c, left_col));

            if let (Some(bi), Some(pi)) = (swapped_build_idx, swapped_probe_idx) {
                return Ok((bi, pi));
            }
        }
        Err(RustqlError::Internal(
            "Could not extract join keys from condition".to_string(),
        ))
    }

    fn get_sort_value(
        &self,
        expr: &Expression,
        columns: &[String],
        column_defs: &[ColumnDefinition],
        row: &[Value],
    ) -> Result<Value, RustqlError> {
        if let Expression::Column(col) = expr
            && let Some(idx) = find_result_column_index(columns, col)
        {
            return Ok(row.get(idx).cloned().unwrap_or(Value::Null));
        }

        self.evaluate_value_expression(expr, column_defs, row)
    }

    fn evaluate_distinct_on_value(
        &self,
        expr: &Expression,
        columns: &[String],
        column_defs: &[ColumnDefinition],
        row: &[Value],
    ) -> Result<Value, RustqlError> {
        if let Expression::Column(column_name) = expr
            && let Some(idx) = find_result_column_index(columns, column_name)
        {
            return Ok(row.get(idx).cloned().unwrap_or(Value::Null));
        }

        self.evaluate_value_expression(expr, column_defs, row)
    }

    fn extract_order_values(
        &self,
        order_by: &[OrderByExpr],
        columns: &[String],
        column_defs: &[ColumnDefinition],
        row: &[Value],
    ) -> Result<Vec<Value>, RustqlError> {
        order_by
            .iter()
            .map(|order_expr| self.get_sort_value(&order_expr.expr, columns, column_defs, row))
            .collect()
    }

    fn compute_group_aggregate_values(
        &self,
        aggregates: &[AggregateFunction],
        rows: &[&[Value]],
        columns: &[ColumnDefinition],
    ) -> Result<Vec<Value>, RustqlError> {
        let mut shared_inputs = vec![0usize; aggregates.len()];
        for (idx, aggregate) in aggregates.iter().enumerate() {
            shared_inputs[idx] = aggregates[..idx]
                .iter()
                .position(|candidate| aggregate_input_signature_matches(candidate, aggregate))
                .unwrap_or(idx);
        }

        let mut prepared_inputs: Vec<Option<PreparedAggregateInput>> = Vec::new();
        prepared_inputs.resize_with(aggregates.len(), || None);

        let mut values = Vec::with_capacity(aggregates.len());
        for (idx, aggregate) in aggregates.iter().enumerate() {
            let input_idx = shared_inputs[idx];
            if prepared_inputs[input_idx].is_none() {
                prepared_inputs[input_idx] =
                    Some(self.prepare_aggregate_input(&aggregates[input_idx], rows, columns)?);
            }
            let prepared_input = prepared_inputs[input_idx].as_ref().ok_or_else(|| {
                RustqlError::Internal("Aggregate input was not prepared".to_string())
            })?;
            values.push(self.compute_aggregate_from_input(aggregate, prepared_input)?);
        }

        Ok(values)
    }

    fn prepare_aggregate_input(
        &self,
        agg: &AggregateFunction,
        rows: &[&[Value]],
        columns: &[ColumnDefinition],
    ) -> Result<PreparedAggregateInput, RustqlError> {
        use std::collections::BTreeSet;

        let count_star = matches!(agg.expr.as_ref(), Expression::Column(name) if name == "*")
            && matches!(agg.function, AggregateFunctionType::Count);

        if count_star && agg.distinct {
            return Err(RustqlError::AggregateError(
                "COUNT(DISTINCT *) is not supported".to_string(),
            ));
        }

        let mut filtered_row_count = 0usize;
        let mut values = Vec::new();
        let mut seen = agg.distinct.then(BTreeSet::new);

        for row in rows {
            if let Some(filter_expr) = agg.filter.as_deref()
                && !self.evaluate_expression(filter_expr, columns, row)?
            {
                continue;
            }

            filtered_row_count += 1;
            if count_star {
                continue;
            }

            let value = self.evaluate_value_expression(&agg.expr, columns, row)?;
            if matches!(value, Value::Null) {
                continue;
            }

            if let Some(seen) = seen.as_mut()
                && !seen.insert(value.clone())
            {
                continue;
            }

            values.push(value);
        }

        Ok(PreparedAggregateInput {
            count_star,
            filtered_row_count,
            values,
        })
    }

    fn compute_aggregate_from_input(
        &self,
        agg: &AggregateFunction,
        input: &PreparedAggregateInput,
    ) -> Result<Value, RustqlError> {
        match agg.function {
            AggregateFunctionType::Count => {
                if input.count_star {
                    Ok(Value::Integer(input.filtered_row_count as i64))
                } else {
                    Ok(Value::Integer(input.values.len() as i64))
                }
            }
            AggregateFunctionType::Sum => {
                let mut sum = 0.0f64;
                let mut has_value = false;
                for value in &input.values {
                    match value {
                        Value::Integer(i) => {
                            sum += *i as f64;
                            has_value = true;
                        }
                        Value::Float(f) => {
                            sum += *f;
                            has_value = true;
                        }
                        _ => {
                            return Err(RustqlError::AggregateError(
                                "SUM requires numeric values".to_string(),
                            ));
                        }
                    }
                }
                if has_value {
                    Ok(Value::Float(sum))
                } else {
                    Ok(Value::Null)
                }
            }
            AggregateFunctionType::Avg => {
                let mut sum = 0.0f64;
                let mut count = 0i64;
                for value in &input.values {
                    match value {
                        Value::Integer(i) => {
                            sum += *i as f64;
                            count += 1;
                        }
                        Value::Float(f) => {
                            sum += *f;
                            count += 1;
                        }
                        _ => {
                            return Err(RustqlError::AggregateError(
                                "AVG requires numeric values".to_string(),
                            ));
                        }
                    }
                }
                if count > 0 {
                    Ok(Value::Float(sum / count as f64))
                } else {
                    Ok(Value::Null)
                }
            }
            AggregateFunctionType::Min => {
                let mut min_val: Option<Value> = None;
                for value in &input.values {
                    min_val = Some(match min_val {
                        None => value.clone(),
                        Some(current) => {
                            if compare_values_same_type(value, &current) == Ordering::Less {
                                value.clone()
                            } else {
                                current
                            }
                        }
                    });
                }
                Ok(min_val.unwrap_or(Value::Null))
            }
            AggregateFunctionType::Max => {
                let mut max_val: Option<Value> = None;
                for value in &input.values {
                    max_val = Some(match max_val {
                        None => value.clone(),
                        Some(current) => {
                            if compare_values_same_type(value, &current) == Ordering::Greater {
                                value.clone()
                            } else {
                                current
                            }
                        }
                    });
                }
                Ok(max_val.unwrap_or(Value::Null))
            }
            AggregateFunctionType::Variance => {
                let values = numeric_values(&input.values, "VARIANCE requires numeric values")?;
                if values.is_empty() {
                    return Ok(Value::Null);
                }
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance =
                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
                Ok(Value::Float(variance))
            }
            AggregateFunctionType::Stddev => {
                let values = numeric_values(&input.values, "STDDEV requires numeric values")?;
                if values.is_empty() {
                    return Ok(Value::Null);
                }
                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance =
                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
                Ok(Value::Float(variance.sqrt()))
            }
            AggregateFunctionType::GroupConcat => {
                let sep = agg.separator.as_deref().unwrap_or(",");
                let parts: Vec<String> = input.values.iter().map(format_value_string).collect();
                if parts.is_empty() {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Text(parts.join(sep)))
                }
            }
            AggregateFunctionType::BoolAnd => {
                let mut result = true;
                let mut has_value = false;
                for value in &input.values {
                    has_value = true;
                    match value {
                        Value::Boolean(b) => {
                            if !b {
                                result = false;
                            }
                        }
                        Value::Integer(n) => {
                            if *n == 0 {
                                result = false;
                            }
                        }
                        _ => {
                            return Err(RustqlError::AggregateError(
                                "BOOL_AND requires boolean or integer values".to_string(),
                            ));
                        }
                    }
                }
                if has_value {
                    Ok(Value::Boolean(result))
                } else {
                    Ok(Value::Null)
                }
            }
            AggregateFunctionType::BoolOr => {
                let mut result = false;
                let mut has_value = false;
                for value in &input.values {
                    has_value = true;
                    match value {
                        Value::Boolean(b) => {
                            if *b {
                                result = true;
                            }
                        }
                        Value::Integer(n) => {
                            if *n != 0 {
                                result = true;
                            }
                        }
                        _ => {
                            return Err(RustqlError::AggregateError(
                                "BOOL_OR requires boolean or integer values".to_string(),
                            ));
                        }
                    }
                }
                if has_value {
                    Ok(Value::Boolean(result))
                } else {
                    Ok(Value::Null)
                }
            }
            AggregateFunctionType::Median => {
                let mut values = numeric_values(&input.values, "MEDIAN requires numeric values")?;
                if values.is_empty() {
                    return Ok(Value::Null);
                }
                values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
                let mid = values.len() / 2;
                if values.len().is_multiple_of(2) {
                    Ok(Value::Float((values[mid - 1] + values[mid]) / 2.0))
                } else {
                    Ok(Value::Float(values[mid]))
                }
            }
            AggregateFunctionType::Mode => {
                let mut counts: BTreeMap<Value, (usize, usize)> = BTreeMap::new();
                for (position, value) in input.values.iter().cloned().enumerate() {
                    let entry = counts.entry(value).or_insert((0, position));
                    entry.0 += 1;
                }
                if counts.is_empty() {
                    return Ok(Value::Null);
                }
                let mode = counts
                    .into_iter()
                    .max_by(|a, b| a.1.0.cmp(&b.1.0).then_with(|| b.1.1.cmp(&a.1.1)))
                    .map(|(value, _)| value)
                    .unwrap_or(Value::Null);
                Ok(mode)
            }
            AggregateFunctionType::PercentileCont => {
                let frac = agg.percentile.unwrap_or(DEFAULT_PERCENTILE_FRACTION);
                let mut values =
                    numeric_values(&input.values, "PERCENTILE_CONT requires numeric values")?;
                if values.is_empty() {
                    return Ok(Value::Null);
                }
                values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
                let n = values.len();
                if n == 1 {
                    return Ok(Value::Float(values[0]));
                }
                let pos = frac * (n - 1) as f64;
                let lower = pos.floor() as usize;
                let upper = pos.ceil() as usize;
                if lower == upper {
                    Ok(Value::Float(values[lower]))
                } else {
                    let w = pos - lower as f64;
                    Ok(Value::Float(values[lower] * (1.0 - w) + values[upper] * w))
                }
            }
            AggregateFunctionType::PercentileDisc => {
                let frac = agg.percentile.unwrap_or(DEFAULT_PERCENTILE_FRACTION);
                let mut values = input.values.clone();
                if values.is_empty() {
                    return Ok(Value::Null);
                }
                values.sort();
                let idx = ((frac * values.len() as f64).ceil() as usize).saturating_sub(1);
                Ok(values[idx.min(values.len() - 1)].clone())
            }
        }
    }

    fn compute_aggregate(
        &self,
        agg: &AggregateFunction,
        rows: &[&[Value]],
        columns: &[ColumnDefinition],
    ) -> Result<Value, RustqlError> {
        let prepared = self.prepare_aggregate_input(agg, rows, columns)?;
        self.compute_aggregate_from_input(agg, &prepared)
    }

    fn evaluate_having(
        &self,
        expr: &Expression,
        context: &HavingContext<'_>,
    ) -> Result<bool, RustqlError> {
        match expr {
            Expression::BinaryOp { left, op, right } => {
                match op {
                    BinaryOperator::And => Ok(self.evaluate_having(left, context)?
                        && self.evaluate_having(right, context)?),
                    BinaryOperator::Or => Ok(self.evaluate_having(left, context)?
                        || self.evaluate_having(right, context)?),
                    _ => {
                        let left_val = self.evaluate_having_value(left, context)?;
                        let right_val = self.evaluate_having_value(right, context)?;
                        compare_values(&left_val, op, &right_val)
                    }
                }
            }
            Expression::IsNull { expr, not } => {
                let value = self.evaluate_having_value(expr, context)?;
                let is_null = matches!(value, Value::Null);
                Ok(if *not { !is_null } else { is_null })
            }
            Expression::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => Ok(!self.evaluate_having(expr, context)?),
                _ => Err(RustqlError::Internal(
                    "Unsupported unary operation in HAVING clause".to_string(),
                )),
            },
            _ => Err(RustqlError::Internal(
                "Invalid expression in HAVING clause".to_string(),
            )),
        }
    }

    fn evaluate_having_value(
        &self,
        expr: &Expression,
        context: &HavingContext<'_>,
    ) -> Result<Value, RustqlError> {
        match expr {
            Expression::Function(agg) => {
                if let Some(idx) = context
                    .selected_aggregates
                    .iter()
                    .position(|candidate| candidate == agg)
                {
                    Ok(context
                        .aggregate_values
                        .get(idx)
                        .cloned()
                        .unwrap_or(Value::Null))
                } else {
                    self.compute_aggregate(agg, context.group_rows, context.input_columns)
                }
            }
            Expression::Value(val) => Ok(val.clone()),
            Expression::Column(name) => {
                let col_name = if name.contains('.') {
                    name.split('.').next_back().unwrap_or(name)
                } else {
                    name.as_str()
                };
                let idx = context
                    .result_columns
                    .iter()
                    .position(|c| column_names_match(&c.name, col_name))
                    .ok_or_else(|| format!("Column '{}' not found in HAVING clause", name))?;
                Ok(context.result_row.get(idx).cloned().unwrap_or(Value::Null))
            }
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_having_value(left, context)?;
                let right_val = self.evaluate_having_value(right, context)?;
                match op {
                    BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide => apply_arithmetic(&left_val, &right_val, op),
                    _ => Err(RustqlError::Internal(
                        "Only arithmetic operators are supported in HAVING expressions".to_string(),
                    )),
                }
            }
            _ => Err(RustqlError::Internal(
                "Complex expressions not yet supported in HAVING".to_string(),
            )),
        }
    }

    fn apply_projection(
        &self,
        result: &ExecutionResult,
        select_stmt: &SelectStatement,
    ) -> Result<ExecutionResult, RustqlError> {
        let column_defs: Vec<ColumnDefinition> = result
            .columns
            .iter()
            .map(|name| ColumnDefinition {
                name: name.clone(),
                data_type: DataType::Text,
                nullable: true,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            })
            .collect();

        let column_specs: Vec<(String, Column)> =
            if matches!(select_stmt.columns.first(), Some(Column::All)) {
                result
                    .columns
                    .iter()
                    .map(|c| {
                        (
                            c.clone(),
                            Column::Named {
                                name: c.clone(),
                                alias: None,
                            },
                        )
                    })
                    .collect()
            } else {
                select_stmt
                    .columns
                    .iter()
                    .map(|col| match col {
                        Column::Named { name, alias } => {
                            Ok((alias.clone().unwrap_or_else(|| name.clone()), col.clone()))
                        }
                        Column::Expression { alias, .. } => Ok((
                            alias.clone().unwrap_or_else(|| "<expression>".to_string()),
                            col.clone(),
                        )),
                        Column::Function(agg) => Ok((
                            crate::executor::aggregate::format_aggregate_header(agg),
                            col.clone(),
                        )),
                        Column::Subquery(_) => Ok(("<subquery>".to_string(), col.clone())),
                        Column::All => Err(RustqlError::Internal(
                            "Wildcard projection must be expanded before plan projection"
                                .to_string(),
                        )),
                    })
                    .collect::<Result<Vec<_>, RustqlError>>()?
            };

        let aggregate_count = select_stmt
            .columns
            .iter()
            .filter(|col| matches!(col, Column::Function(_)))
            .count();
        let has_window_functions = select_stmt.columns.iter().any(|column| {
            matches!(
                column,
                Column::Expression {
                    expr: Expression::WindowFunction { .. },
                    ..
                }
            )
        });

        for (_, col) in &column_specs {
            if let Column::Named { name, .. } = col {
                find_result_column_index(&result.columns, name)
                    .ok_or_else(|| RustqlError::ColumnNotFound(name.to_string()))?;
            }
        }

        let window_rows = if has_window_functions {
            let mut rows = result.rows.clone();
            crate::executor::aggregate::evaluate_window_functions(
                &mut rows,
                &column_defs,
                &select_stmt.columns,
            )?;
            Some(rows)
        } else {
            None
        };

        let mut projected_rows = Vec::new();
        for (row_idx, row) in result.rows.iter().enumerate() {
            let mut projected_row = Vec::new();
            let mut aggregate_offset = result.columns.len().saturating_sub(aggregate_count);
            let mut window_offset = result.columns.len();
            for (_, col) in &column_specs {
                let val = match col {
                    Column::All => {
                        return Err(RustqlError::Internal(
                            "Wildcard projection must be expanded before plan projection"
                                .to_string(),
                        ));
                    }
                    Column::Named { name, .. } => {
                        let idx = find_result_column_index(&result.columns, name)
                            .ok_or_else(|| RustqlError::ColumnNotFound(name.to_string()))?;
                        row.get(idx).cloned().unwrap_or(Value::Null)
                    }
                    Column::Expression { expr, .. } => match expr {
                        Expression::WindowFunction { .. } => window_rows
                            .as_ref()
                            .and_then(|rows| rows.get(row_idx))
                            .and_then(|window_row| window_row.get(window_offset))
                            .cloned()
                            .inspect(|_| {
                                window_offset += 1;
                            })
                            .unwrap_or(Value::Null),
                        _ => self.evaluate_value_expression(expr, &column_defs, row)?,
                    },
                    Column::Function(_) => {
                        let value = row.get(aggregate_offset).cloned().unwrap_or(Value::Null);
                        aggregate_offset += 1;
                        value
                    }
                    Column::Subquery(_) => {
                        return Err(RustqlError::Internal(
                            "Subqueries in SELECT list not yet supported in plan executor"
                                .to_string(),
                        ));
                    }
                };
                projected_row.push(val);
            }
            projected_rows.push(projected_row);
        }

        let projected_columns: Vec<String> =
            column_specs.iter().map(|(name, _)| name.clone()).collect();

        Ok(ExecutionResult {
            columns: projected_columns,
            rows: projected_rows,
        })
    }

    fn apply_distinct(&self, input: ExecutionResult) -> Result<ExecutionResult, RustqlError> {
        use std::collections::BTreeSet;
        let mut seen = BTreeSet::new();
        let mut unique_rows = Vec::new();

        for row in input.rows {
            if seen.insert(row.clone()) {
                unique_rows.push(row);
            }
        }

        Ok(ExecutionResult {
            columns: input.columns,
            rows: unique_rows,
        })
    }
}

fn column_names_match(candidate: &str, reference: &str) -> bool {
    candidate == reference
        || unqualified_column_name(candidate) == unqualified_column_name(reference)
}

fn find_result_column_index(columns: &[String], reference: &str) -> Option<usize> {
    columns
        .iter()
        .position(|column| column == reference)
        .or_else(|| {
            let unqualified = unqualified_column_name(reference);
            columns
                .iter()
                .position(|column| column_names_match(column, unqualified))
        })
}

fn qualify_column_names(columns: &[ColumnDefinition], output_label: Option<&str>) -> Vec<String> {
    columns
        .iter()
        .map(|column| qualified_column_name(output_label, &column.name))
        .collect()
}

fn qualified_column_name(output_label: Option<&str>, column_name: &str) -> String {
    output_label
        .map(|label| format!("{}.{}", label, column_name))
        .unwrap_or_else(|| column_name.to_string())
}

fn unqualified_column_name(name: &str) -> &str {
    name.split('.').next_back().unwrap_or(name)
}

impl ExecutionResult {
    pub fn new(columns: Vec<String>, rows: Vec<Vec<Value>>) -> Self {
        ExecutionResult { columns, rows }
    }
}

impl Clone for ExecutionResult {
    fn clone(&self) -> Self {
        ExecutionResult {
            columns: self.columns.clone(),
            rows: self.rows.clone(),
        }
    }
}

struct PreparedAggregateInput {
    count_star: bool,
    filtered_row_count: usize,
    values: Vec<Value>,
}

struct HavingContext<'a> {
    result_columns: &'a [ColumnDefinition],
    result_row: &'a [Value],
    input_columns: &'a [ColumnDefinition],
    selected_aggregates: &'a [AggregateFunction],
    aggregate_values: &'a [Value],
    group_rows: &'a [&'a [Value]],
}

fn column_definitions_from_names(columns: &[String]) -> Vec<ColumnDefinition> {
    columns
        .iter()
        .map(|name| ColumnDefinition {
            name: name.clone(),
            data_type: DataType::Text,
            nullable: true,
            primary_key: false,
            unique: false,
            default_value: None,
            foreign_key: None,
            check: None,
            auto_increment: false,
            generated: None,
        })
        .collect()
}

fn combined_column_definitions(left: &[String], right: &[String]) -> Vec<ColumnDefinition> {
    let mut combined = column_definitions_from_names(left);
    combined.extend(column_definitions_from_names(right));
    combined
}

fn aggregate_input_signature_matches(left: &AggregateFunction, right: &AggregateFunction) -> bool {
    left.expr == right.expr && left.distinct == right.distinct && left.filter == right.filter
}

fn numeric_values(values: &[Value], error: &str) -> Result<Vec<f64>, RustqlError> {
    values
        .iter()
        .map(|value| match value {
            Value::Integer(i) => Ok(*i as f64),
            Value::Float(f) => Ok(*f),
            _ => Err(RustqlError::AggregateError(error.to_string())),
        })
        .collect()
}

fn format_value_string(value: &Value) -> String {
    match value {
        Value::Integer(n) => n.to_string(),
        Value::Float(f) => format!("{}", f),
        Value::Text(s) => s.clone(),
        Value::Boolean(b) => b.to_string(),
        Value::Date(d) => d.clone(),
        Value::Time(t) => t.clone(),
        Value::DateTime(dt) => dt.clone(),
        Value::Null => "NULL".to_string(),
    }
}

fn combine_rows(left: &[Value], right: &[Value]) -> Vec<Value> {
    let mut combined = Vec::with_capacity(left.len() + right.len());
    combined.extend_from_slice(left);
    combined.extend_from_slice(right);
    combined
}

fn combine_row_with_right_nulls(left: &[Value], right_len: usize) -> Vec<Value> {
    let mut combined = Vec::with_capacity(left.len() + right_len);
    combined.extend_from_slice(left);
    combined.resize(left.len() + right_len, Value::Null);
    combined
}

fn combine_row_with_left_nulls(left_len: usize, right: &[Value]) -> Vec<Value> {
    let mut combined = Vec::with_capacity(left_len + right.len());
    combined.resize(left_len, Value::Null);
    combined.extend_from_slice(right);
    combined
}

fn lateral_subquery_with_outer_scope(
    subquery: &SelectStatement,
    outer_table_name: &str,
) -> SelectStatement {
    let mut rewritten = subquery.clone();
    if rewritten.from.is_empty()
        && rewritten.from_subquery.is_none()
        && rewritten.from_function.is_none()
        && rewritten.from_values.is_none()
    {
        rewritten.from = outer_table_name.to_string();
    } else {
        rewritten.joins.push(Join {
            join_type: JoinType::Cross,
            table: outer_table_name.to_string(),
            table_alias: None,
            on: None,
            using_columns: None,
            lateral: true,
            subquery: None,
        });
    }
    rewritten
}
