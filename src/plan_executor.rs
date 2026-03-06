use crate::ast::*;
use crate::database::{Database, Table};
use crate::error::RustqlError;
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
    db: &'a Database,
}

impl<'a> PlanExecutor<'a> {
    pub fn new(db: &'a Database) -> Self {
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

    fn execute_seq_scan(
        &self,
        table_name: &str,
        output_label: Option<&str>,
        filter: Option<&Expression>,
    ) -> Result<ExecutionResult, RustqlError> {
        let table = self
            .db
            .tables
            .get(table_name)
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
            .tables
            .get(table_name)
            .ok_or_else(|| format!("Table '{}' does not exist", table_name))?;

        let index = self
            .db
            .indexes
            .get(index_name)
            .ok_or_else(|| format!("Index '{}' does not exist", index_name))?;

        let _col_idx = table
            .columns
            .iter()
            .position(|c| c.name == index.column)
            .ok_or_else(|| format!("Column '{}' not found in table", index.column))?;

        let filter_value = if let Some(filter_expr) = filter {
            self.extract_filter_value(filter_expr, &index.column)
        } else {
            None
        };

        let mut row_indices = HashSet::new();

        if let Some(value) = filter_value {
            if let Some(indices) = index.entries.get(&value) {
                for &idx in indices {
                    row_indices.insert(idx);
                }
            }
        } else {
            for indices in index.entries.values() {
                for &idx in indices {
                    row_indices.insert(idx);
                }
            }
        }

        let mut rows = Vec::new();
        for &row_idx in &row_indices {
            if let Some(row) = table.row_by_id(row_idx) {
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

        for row in &input.rows {
            let columns: Vec<ColumnDefinition> = input
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

        for left_row in &left.rows {
            let mut has_match = false;
            for (right_idx, right_row) in right.rows.iter().enumerate() {
                let combined_row: Vec<Value> =
                    left_row.iter().chain(right_row.iter()).cloned().collect();

                let include = if matches!(join_type, JoinType::Cross) {
                    true
                } else {
                    self.evaluate_join_condition(
                        condition,
                        &left.columns,
                        &right.columns,
                        left_row,
                        right_row,
                    )?
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
                let mut combined_row = left_row.clone();
                combined_row.extend(vec![Value::Null; right.columns.len()]);
                joined_rows.push(combined_row);
            }
        }

        if matches!(join_type, JoinType::Right | JoinType::Full) {
            for (right_idx, right_row) in right.rows.iter().enumerate() {
                if !matched_right[right_idx] {
                    let mut combined_row = vec![Value::Null; left.columns.len()];
                    combined_row.extend(right_row.clone());
                    joined_rows.push(combined_row);
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

        for left_row in &left.rows {
            let temp_table_name = format!("__lateral_outer_{}", alias);
            let rewritten_subquery = lateral_subquery_with_outer_scope(subquery, &temp_table_name);
            let mut scoped_db = self.db.clone();
            scoped_db.tables.insert(
                temp_table_name,
                Table::new(outer_scope_columns.clone(), vec![left_row.clone()], vec![]),
            );

            let subquery_result =
                match execute_select_internal(None, rewritten_subquery, &scoped_db) {
                    Ok(result) => result,
                    Err(err) => {
                        if matches!(join_type, JoinType::Left | JoinType::Full) {
                            let mut combined_row = left_row.clone();
                            combined_row.extend(vec![Value::Null; right_columns.len()]);
                            joined_rows.push(combined_row);
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
                let include = self.evaluate_join_condition(
                    condition,
                    &left.columns,
                    right_columns,
                    left_row,
                    right_row,
                )?;

                if include {
                    let mut combined_row = left_row.clone();
                    combined_row.extend(right_row.clone());
                    joined_rows.push(combined_row);
                    has_match = true;
                }
            }

            if matches!(join_type, JoinType::Left | JoinType::Full) && !has_match {
                let mut combined_row = left_row.clone();
                combined_row.extend(vec![Value::Null; right_columns.len()]);
                joined_rows.push(combined_row);
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

        let mut hash_table: BTreeMap<Value, Vec<Vec<Value>>> = BTreeMap::new();
        for row in &build.rows {
            if build_key_idx < row.len() {
                let key = row[build_key_idx].clone();
                hash_table.entry(key).or_default().push(row.clone());
            }
        }

        let mut joined_rows = Vec::new();
        let mut joined_columns = left.columns.clone();
        joined_columns.extend(right.columns.clone());

        for probe_row in &probe.rows {
            if probe_key_idx < probe_row.len() {
                let key = probe_row[probe_key_idx].clone();
                if let Some(build_rows) = hash_table.get(&key) {
                    for build_row in build_rows {
                        let combined_row: Vec<Value> = if left.rows.len() <= right.rows.len() {
                            build_row.iter().chain(probe_row.iter()).cloned().collect()
                        } else {
                            probe_row.iter().chain(build_row.iter()).cloned().collect()
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
        let mut rows = input.rows;

        rows.sort_by(|a, b| {
            for order_expr in order_by {
                let a_val = self
                    .get_sort_value(&order_expr.expr, &input.columns, a)
                    .unwrap_or(Value::Null);
                let b_val = self
                    .get_sort_value(&order_expr.expr, &input.columns, b)
                    .unwrap_or(Value::Null);
                let cmp = self.compare_values(&a_val, &b_val);
                if cmp != Ordering::Equal {
                    return if order_expr.asc { cmp } else { cmp.reverse() };
                }
            }
            Ordering::Equal
        });

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
                let boundary_values =
                    self.extract_order_values(order_by, &input.columns, &rows[limit - 1]);
                let mut extended_limit = limit;
                while extended_limit < rows.len()
                    && self.extract_order_values(order_by, &input.columns, &rows[extended_limit])
                        == boundary_values
                {
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
        if let Some(grouping_sets) = grouping_sets {
            for set in grouping_sets {
                let mut groups: BTreeMap<Vec<Value>, Vec<Vec<Value>>> = BTreeMap::new();
                for row in &input.rows {
                    let key: Vec<Value> = set
                        .iter()
                        .map(|expr| {
                            self.evaluate_value_expression(expr, &column_defs, row)
                                .unwrap_or(Value::Null)
                        })
                        .collect();
                    groups.entry(key).or_default().push(row.clone());
                }

                if groups.is_empty() && set.is_empty() {
                    groups.insert(Vec::new(), Vec::new());
                }

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

                    for agg in aggregates {
                        let agg_value = self.compute_aggregate(agg, &group_rows, &input.columns)?;
                        result_row.push(agg_value);
                    }

                    if let Some(having_expr) = having {
                        let include = self.evaluate_having(
                            having_expr,
                            &result_column_defs,
                            &result_row,
                            &column_defs,
                            &group_rows,
                        )?;

                        if !include {
                            continue;
                        }
                    }

                    result_rows.push(result_row);
                }
            }
        } else {
            let mut groups: BTreeMap<Vec<Value>, Vec<Vec<Value>>> = BTreeMap::new();
            for row in &input.rows {
                let key: Vec<Value> = group_by
                    .iter()
                    .map(|expr| {
                        self.evaluate_value_expression(expr, &column_defs, row)
                            .unwrap_or(Value::Null)
                    })
                    .collect();
                groups.entry(key).or_default().push(row.clone());
            }

            if groups.is_empty() && group_by.is_empty() {
                groups.insert(Vec::new(), Vec::new());
            }

            for (group_key, group_rows) in groups {
                let mut result_row = group_key.clone();

                for agg in aggregates {
                    let agg_value = self.compute_aggregate(agg, &group_rows, &input.columns)?;
                    result_row.push(agg_value);
                }

                if let Some(having_expr) = having {
                    let include = self.evaluate_having(
                        having_expr,
                        &result_column_defs,
                        &result_row,
                        &column_defs,
                        &group_rows,
                    )?;

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
        match expr {
            Expression::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => Ok(self.evaluate_expression(left, columns, row)?
                    && self.evaluate_expression(right, columns, row)?),
                BinaryOperator::Or => Ok(self.evaluate_expression(left, columns, row)?
                    || self.evaluate_expression(right, columns, row)?),
                BinaryOperator::Like => {
                    let left_val = self.evaluate_value_expression(left, columns, row)?;
                    let right_val = self.evaluate_value_expression(right, columns, row)?;
                    match (left_val, right_val) {
                        (Value::Text(text), Value::Text(pattern)) => {
                            Ok(self.match_like(&text, &pattern))
                        }
                        _ => Err(RustqlError::TypeMismatch(
                            "LIKE operator requires text values".to_string(),
                        )),
                    }
                }
                BinaryOperator::ILike => {
                    let left_val = self.evaluate_value_expression(left, columns, row)?;
                    let right_val = self.evaluate_value_expression(right, columns, row)?;
                    match (left_val, right_val) {
                        (Value::Text(text), Value::Text(pattern)) => {
                            Ok(self.match_like(&text.to_lowercase(), &pattern.to_lowercase()))
                        }
                        _ => Err(RustqlError::TypeMismatch(
                            "ILIKE operator requires text values".to_string(),
                        )),
                    }
                }
                BinaryOperator::Between => {
                    let left_val = self.evaluate_value_expression(left, columns, row)?;
                    match &**right {
                        Expression::BinaryOp {
                            left: lb,
                            op: lb_op,
                            right: rb,
                        } if *lb_op == BinaryOperator::And => {
                            let lower = self.evaluate_value_expression(lb, columns, row)?;
                            let upper = self.evaluate_value_expression(rb, columns, row)?;
                            Ok(self.is_between(&left_val, &lower, &upper))
                        }
                        _ => Err(RustqlError::TypeMismatch(
                            "BETWEEN requires two values".to_string(),
                        )),
                    }
                }
                BinaryOperator::In => {
                    let left_val = self.evaluate_value_expression(left, columns, row)?;
                    match &**right {
                        Expression::In { values, .. } => Ok(values.contains(&left_val)),
                        _ => {
                            let right_val = self.evaluate_value_expression(right, columns, row)?;
                            self.compare_values_for_where(&left_val, op, &right_val)
                        }
                    }
                }
                _ => {
                    let left_val = self.evaluate_value_expression(left, columns, row)?;
                    let right_val = self.evaluate_value_expression(right, columns, row)?;
                    self.compare_values_for_where(&left_val, op, &right_val)
                }
            },
            Expression::In { left, values } => {
                let left_val = self.evaluate_value_expression(left, columns, row)?;
                Ok(values.contains(&left_val))
            }
            Expression::IsNull { expr, not } => {
                let value = self.evaluate_value_expression(expr, columns, row)?;
                let is_null = matches!(value, Value::Null);
                Ok(if *not { !is_null } else { is_null })
            }
            Expression::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => Ok(!self.evaluate_expression(expr, columns, row)?),
                _ => Err(RustqlError::Internal(
                    "Unsupported unary operation in WHERE clause".to_string(),
                )),
            },
            Expression::IsDistinctFrom { left, right, not } => {
                let left_val = self.evaluate_value_expression(left, columns, row)?;
                let right_val = self.evaluate_value_expression(right, columns, row)?;
                let is_distinct = match (&left_val, &right_val) {
                    (Value::Null, Value::Null) => false,
                    (Value::Null, _) | (_, Value::Null) => true,
                    _ => left_val != right_val,
                };
                Ok(if *not { !is_distinct } else { is_distinct })
            }
            _ => Err(RustqlError::Internal(
                "Invalid expression in WHERE clause".to_string(),
            )),
        }
    }

    fn evaluate_value_expression(
        &self,
        expr: &Expression,
        columns: &[ColumnDefinition],
        row: &[Value],
    ) -> Result<Value, RustqlError> {
        match expr {
            Expression::Column(name) => {
                if name == "*" {
                    return Ok(Value::Integer(1));
                }
                let idx = find_defined_column_index(columns, name)
                    .ok_or_else(|| RustqlError::ColumnNotFound(name.to_string()))?;
                Ok(row.get(idx).cloned().unwrap_or(Value::Null))
            }
            Expression::Value(val) => Ok(val.clone()),
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_value_expression(left, columns, row)?;
                let right_val = self.evaluate_value_expression(right, columns, row)?;
                match op {
                    BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide => self.apply_arithmetic(&left_val, &right_val, op),
                    _ => Err(RustqlError::Internal(
                        "Only arithmetic operators are supported in SELECT expressions".to_string(),
                    )),
                }
            }
            Expression::UnaryOp { op, expr } => {
                let val = self.evaluate_value_expression(expr, columns, row)?;
                match op {
                    UnaryOperator::Minus => match val {
                        Value::Integer(n) => Ok(Value::Integer(-n)),
                        Value::Float(f) => Ok(Value::Float(-f)),
                        _ => Err(RustqlError::Internal(
                            "Unary minus only supported for numeric types".to_string(),
                        )),
                    },
                    _ => Err(RustqlError::Internal(
                        "Unsupported unary operator in SELECT expression".to_string(),
                    )),
                }
            }
            _ => Err(RustqlError::Internal(
                "Complex expressions not yet supported in SELECT".to_string(),
            )),
        }
    }

    fn apply_arithmetic(
        &self,
        left: &Value,
        right: &Value,
        op: &BinaryOperator,
    ) -> Result<Value, RustqlError> {
        let to_float = |value: &Value| -> Result<f64, RustqlError> {
            match value {
                Value::Integer(i) => Ok(*i as f64),
                Value::Float(f) => Ok(*f),
                Value::Null => Ok(0.0),
                _ => Err(RustqlError::TypeMismatch(
                    "Arithmetic requires numeric values".to_string(),
                )),
            }
        };

        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => match op {
                BinaryOperator::Plus => Ok(Value::Integer(l + r)),
                BinaryOperator::Minus => Ok(Value::Integer(l - r)),
                BinaryOperator::Multiply => Ok(Value::Integer(l * r)),
                BinaryOperator::Divide => {
                    if *r == 0 {
                        Err(RustqlError::DivisionByZero)
                    } else if l % r == 0 {
                        Ok(Value::Integer(l / r))
                    } else {
                        Ok(Value::Float(*l as f64 / *r as f64))
                    }
                }
                _ => unreachable!(),
            },
            _ => {
                let l = to_float(left)?;
                let r = to_float(right)?;
                match op {
                    BinaryOperator::Plus => Ok(Value::Float(l + r)),
                    BinaryOperator::Minus => Ok(Value::Float(l - r)),
                    BinaryOperator::Multiply => Ok(Value::Float(l * r)),
                    BinaryOperator::Divide => {
                        if r.abs() < f64::EPSILON {
                            Err(RustqlError::DivisionByZero)
                        } else {
                            Ok(Value::Float(l / r))
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    fn compare_values_for_where(
        &self,
        left: &Value,
        op: &BinaryOperator,
        right: &Value,
    ) -> Result<bool, RustqlError> {
        if matches!(left, Value::Null) || matches!(right, Value::Null) {
            return Ok(false);
        }

        let (left_num, right_num) = match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => (Some(*l as f64), Some(*r as f64)),
            (Value::Float(l), Value::Float(r)) => (Some(*l), Some(*r)),
            (Value::Integer(l), Value::Float(r)) => (Some(*l as f64), Some(*r)),
            (Value::Float(l), Value::Integer(r)) => (Some(*l), Some(*r as f64)),
            _ => (None, None),
        };
        if let (Some(l), Some(r)) = (left_num, right_num) {
            Ok(match op {
                BinaryOperator::Equal => (l - r).abs() < f64::EPSILON,
                BinaryOperator::NotEqual => (l - r).abs() >= f64::EPSILON,
                BinaryOperator::LessThan => l < r,
                BinaryOperator::LessThanOrEqual => l <= r,
                BinaryOperator::GreaterThan => l > r,
                BinaryOperator::GreaterThanOrEqual => l >= r,
                _ => {
                    return Err(RustqlError::TypeMismatch(
                        "Invalid operator for numeric comparison".to_string(),
                    ));
                }
            })
        } else {
            match (left, right, op) {
                (Value::Text(l), Value::Text(r), op) => Ok(match op {
                    BinaryOperator::Equal => l == r,
                    BinaryOperator::NotEqual => l != r,
                    BinaryOperator::LessThan => l < r,
                    BinaryOperator::LessThanOrEqual => l <= r,
                    BinaryOperator::GreaterThan => l > r,
                    BinaryOperator::GreaterThanOrEqual => l >= r,
                    _ => {
                        return Err(RustqlError::TypeMismatch(
                            "Invalid operator for strings".to_string(),
                        ));
                    }
                }),
                (Value::Boolean(l), Value::Boolean(r), op) => Ok(match op {
                    BinaryOperator::Equal => l == r,
                    BinaryOperator::NotEqual => l != r,
                    _ => {
                        return Err(RustqlError::TypeMismatch(
                            "Invalid operator for booleans".to_string(),
                        ));
                    }
                }),
                (Value::Date(l), Value::Date(r), op) => Ok(match op {
                    BinaryOperator::Equal => l == r,
                    BinaryOperator::NotEqual => l != r,
                    BinaryOperator::LessThan => l < r,
                    BinaryOperator::LessThanOrEqual => l <= r,
                    BinaryOperator::GreaterThan => l > r,
                    BinaryOperator::GreaterThanOrEqual => l >= r,
                    _ => {
                        return Err(RustqlError::TypeMismatch(
                            "Invalid operator for dates".to_string(),
                        ));
                    }
                }),
                (Value::Time(l), Value::Time(r), op) => Ok(match op {
                    BinaryOperator::Equal => l == r,
                    BinaryOperator::NotEqual => l != r,
                    BinaryOperator::LessThan => l < r,
                    BinaryOperator::LessThanOrEqual => l <= r,
                    BinaryOperator::GreaterThan => l > r,
                    BinaryOperator::GreaterThanOrEqual => l >= r,
                    _ => {
                        return Err(RustqlError::TypeMismatch(
                            "Invalid operator for times".to_string(),
                        ));
                    }
                }),
                (Value::DateTime(l), Value::DateTime(r), op) => Ok(match op {
                    BinaryOperator::Equal => l == r,
                    BinaryOperator::NotEqual => l != r,
                    BinaryOperator::LessThan => l < r,
                    BinaryOperator::LessThanOrEqual => l <= r,
                    BinaryOperator::GreaterThan => l > r,
                    BinaryOperator::GreaterThanOrEqual => l >= r,
                    _ => {
                        return Err(RustqlError::TypeMismatch(
                            "Invalid operator for datetimes".to_string(),
                        ));
                    }
                }),
                _ => Err(RustqlError::TypeMismatch(
                    "Cannot compare incompatible types".to_string(),
                )),
            }
        }
    }

    fn match_like(&self, text: &str, pattern: &str) -> bool {
        let text_chars: Vec<char> = text.chars().collect();
        let pattern_chars: Vec<char> = pattern.chars().collect();

        fn match_pattern(
            text: &[char],
            pattern: &[char],
            text_idx: usize,
            pattern_idx: usize,
        ) -> bool {
            let text_len = text.len();
            let pattern_len = pattern.len();

            if pattern_idx == pattern_len {
                return text_idx == text_len;
            }

            if pattern[pattern_idx] == '%' {
                if pattern_idx + 1 == pattern_len {
                    return true;
                }
                for i in text_idx..=text_len {
                    if match_pattern(text, pattern, i, pattern_idx + 1) {
                        return true;
                    }
                }
                return false;
            }

            if pattern[pattern_idx] == '_' {
                if text_idx < text_len {
                    return match_pattern(text, pattern, text_idx + 1, pattern_idx + 1);
                }
                return false;
            }

            if text_idx < text_len && text[text_idx] == pattern[pattern_idx] {
                return match_pattern(text, pattern, text_idx + 1, pattern_idx + 1);
            }

            false
        }

        match_pattern(&text_chars, &pattern_chars, 0, 0)
    }

    fn is_between(&self, val: &Value, lower: &Value, upper: &Value) -> bool {
        match (val, lower, upper) {
            (Value::Integer(v), Value::Integer(l), Value::Integer(u)) => *v >= *l && *v <= *u,
            (Value::Float(v), Value::Float(l), Value::Float(u)) => *v >= *l && *v <= *u,
            (Value::Integer(v), Value::Integer(l), Value::Float(u)) => {
                *v as f64 >= *l as f64 && *v as f64 <= *u
            }
            (Value::Integer(v), Value::Float(l), Value::Integer(u)) => {
                *v as f64 >= *l && *v as f64 <= *u as f64
            }
            (Value::Float(v), Value::Integer(l), Value::Integer(u)) => {
                *v >= *l as f64 && *v <= *u as f64
            }
            (Value::Float(v), Value::Integer(l), Value::Float(u)) => *v >= *l as f64 && *v <= *u,
            (Value::Float(v), Value::Float(l), Value::Integer(u)) => *v >= *l && *v <= *u as f64,
            (Value::Integer(v), Value::Float(l), Value::Float(u)) => {
                *v as f64 >= *l && *v as f64 <= *u
            }
            (Value::Text(v), Value::Text(l), Value::Text(u)) => v >= l && v <= u,
            _ => false,
        }
    }

    fn extract_filter_value(&self, expr: &Expression, column: &str) -> Option<Value> {
        if let Expression::BinaryOp {
            left,
            op: BinaryOperator::Equal,
            right,
        } = expr
            && let Expression::Column(col) = left.as_ref()
            && column_names_match(col, column)
            && let Expression::Value(v) = right.as_ref()
        {
            return Some(v.clone());
        }
        None
    }

    fn evaluate_join_condition(
        &self,
        condition: &Expression,
        left_columns: &[String],
        right_columns: &[String],
        left_row: &[Value],
        right_row: &[Value],
    ) -> Result<bool, RustqlError> {
        if let Expression::Value(Value::Boolean(value)) = condition {
            return Ok(*value);
        }

        let mut combined_columns = column_definitions_from_names(left_columns);
        combined_columns.extend(column_definitions_from_names(right_columns));

        let combined_row: Vec<Value> = left_row.iter().chain(right_row.iter()).cloned().collect();

        self.evaluate_expression(condition, &combined_columns, &combined_row)
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
        row: &[Value],
    ) -> Option<Value> {
        if let Expression::Column(col) = expr
            && let Some(idx) = find_result_column_index(columns, col)
        {
            return row.get(idx).cloned();
        }
        None
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
        row: &[Value],
    ) -> Vec<Value> {
        order_by
            .iter()
            .map(|order_expr| {
                self.get_sort_value(&order_expr.expr, columns, row)
                    .unwrap_or(Value::Null)
            })
            .collect()
    }

    fn compare_values(&self, a: &Value, b: &Value) -> Ordering {
        match (a, b) {
            (Value::Integer(i1), Value::Integer(i2)) => i1.cmp(i2),
            (Value::Float(f1), Value::Float(f2)) => f1.partial_cmp(f2).unwrap_or(Ordering::Equal),
            (Value::Text(s1), Value::Text(s2)) => s1.cmp(s2),
            (Value::Boolean(b1), Value::Boolean(b2)) => b1.cmp(b2),
            _ => Ordering::Equal,
        }
    }

    fn compute_aggregate(
        &self,
        agg: &AggregateFunction,
        rows: &[Vec<Value>],
        columns: &[String],
    ) -> Result<Value, RustqlError> {
        use std::collections::BTreeSet;

        let column_defs: Vec<ColumnDefinition> = columns
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

        let eval_expr_for_row = |row: &Vec<Value>| -> Result<Value, RustqlError> {
            self.evaluate_value_expression(&agg.expr, &column_defs, row)
        };

        let filtered_rows: Vec<&Vec<Value>> = rows
            .iter()
            .filter(|row| {
                agg.filter.as_deref().is_none_or(|filter_expr| {
                    self.evaluate_expression(filter_expr, &column_defs, row)
                        .unwrap_or(false)
                })
            })
            .collect();

        let mut seen: BTreeSet<Value> = BTreeSet::new();

        match agg.function {
            AggregateFunctionType::Count => {
                if let Expression::Column(name) = agg.expr.as_ref()
                    && name == "*"
                {
                    if agg.distinct {
                        return Err(RustqlError::AggregateError(
                            "COUNT(DISTINCT *) is not supported".to_string(),
                        ));
                    }
                    return Ok(Value::Integer(filtered_rows.len() as i64));
                }

                let mut count = 0i64;
                for row in &filtered_rows {
                    let val = eval_expr_for_row(row)?;
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    if agg.distinct && !seen.insert(val) {
                        continue;
                    }
                    count += 1;
                }
                Ok(Value::Integer(count))
            }
            AggregateFunctionType::Sum => {
                let mut sum = 0.0f64;
                let mut has_value = false;

                for row in &filtered_rows {
                    let val = eval_expr_for_row(row)?;
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    if agg.distinct && !seen.insert(val.clone()) {
                        continue;
                    }
                    match val {
                        Value::Integer(i) => {
                            sum += i as f64;
                            has_value = true;
                        }
                        Value::Float(f) => {
                            sum += f;
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

                for row in &filtered_rows {
                    let val = eval_expr_for_row(row)?;
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    if agg.distinct && !seen.insert(val.clone()) {
                        continue;
                    }
                    match val {
                        Value::Integer(i) => {
                            sum += i as f64;
                            count += 1;
                        }
                        Value::Float(f) => {
                            sum += f;
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

                for row in &filtered_rows {
                    let val = eval_expr_for_row(row)?;
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    if agg.distinct && !seen.insert(val.clone()) {
                        continue;
                    }
                    min_val = Some(match min_val {
                        None => val,
                        Some(current) => {
                            if self.compare_values(&val, &current) == Ordering::Less {
                                val
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

                for row in &filtered_rows {
                    let val = eval_expr_for_row(row)?;
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    if agg.distinct && !seen.insert(val.clone()) {
                        continue;
                    }
                    max_val = Some(match max_val {
                        None => val,
                        Some(current) => {
                            if self.compare_values(&val, &current) == Ordering::Greater {
                                val
                            } else {
                                current
                            }
                        }
                    });
                }

                Ok(max_val.unwrap_or(Value::Null))
            }
            AggregateFunctionType::Variance => {
                let mut values = Vec::new();

                for row in &filtered_rows {
                    let val = eval_expr_for_row(row)?;
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    if agg.distinct && !seen.insert(val.clone()) {
                        continue;
                    }
                    match val {
                        Value::Integer(i) => values.push(i as f64),
                        Value::Float(f) => values.push(f),
                        _ => {
                            return Err(RustqlError::AggregateError(
                                "VARIANCE requires numeric values".to_string(),
                            ));
                        }
                    }
                }

                if values.is_empty() {
                    return Ok(Value::Null);
                }

                let mean = values.iter().sum::<f64>() / values.len() as f64;
                let variance =
                    values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / values.len() as f64;
                Ok(Value::Float(variance))
            }
            AggregateFunctionType::Stddev => {
                let mut values = Vec::new();

                for row in &filtered_rows {
                    let val = eval_expr_for_row(row)?;
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    if agg.distinct && !seen.insert(val.clone()) {
                        continue;
                    }
                    match val {
                        Value::Integer(i) => values.push(i as f64),
                        Value::Float(f) => values.push(f),
                        _ => {
                            return Err(RustqlError::AggregateError(
                                "STDDEV requires numeric values".to_string(),
                            ));
                        }
                    }
                }

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
                let mut parts: Vec<String> = Vec::new();
                for row in &filtered_rows {
                    let val = eval_expr_for_row(row)?;
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    if agg.distinct && !seen.insert(val.clone()) {
                        continue;
                    }
                    parts.push(match &val {
                        Value::Integer(n) => n.to_string(),
                        Value::Float(f) => format!("{}", f),
                        Value::Text(s) => s.clone(),
                        Value::Boolean(b) => b.to_string(),
                        Value::Date(d) => d.clone(),
                        Value::Time(t) => t.clone(),
                        Value::DateTime(dt) => dt.clone(),
                        Value::Null => "NULL".to_string(),
                    });
                }
                if parts.is_empty() {
                    Ok(Value::Null)
                } else {
                    Ok(Value::Text(parts.join(sep)))
                }
            }
            AggregateFunctionType::BoolAnd => {
                let mut result = true;
                let mut has_value = false;
                for row in &filtered_rows {
                    let val = eval_expr_for_row(row)?;
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    has_value = true;
                    match val {
                        Value::Boolean(b) => {
                            if !b {
                                result = false;
                            }
                        }
                        Value::Integer(n) => {
                            if n == 0 {
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
                for row in &filtered_rows {
                    let val = eval_expr_for_row(row)?;
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    has_value = true;
                    match val {
                        Value::Boolean(b) => {
                            if b {
                                result = true;
                            }
                        }
                        Value::Integer(n) => {
                            if n != 0 {
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
                let mut values: Vec<f64> = Vec::new();
                for row in &filtered_rows {
                    let val = eval_expr_for_row(row)?;
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    if agg.distinct && !seen.insert(val.clone()) {
                        continue;
                    }
                    match val {
                        Value::Integer(i) => values.push(i as f64),
                        Value::Float(f) => values.push(f),
                        _ => {
                            return Err(RustqlError::AggregateError(
                                "MEDIAN requires numeric values".to_string(),
                            ));
                        }
                    }
                }
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
                let mut counts: Vec<(Value, usize)> = Vec::new();
                for row in &filtered_rows {
                    let val = eval_expr_for_row(row)?;
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    if agg.distinct && !seen.insert(val.clone()) {
                        continue;
                    }
                    if let Some(entry) = counts.iter_mut().find(|(v, _)| *v == val) {
                        entry.1 += 1;
                    } else {
                        counts.push((val, 1));
                    }
                }
                if counts.is_empty() {
                    return Ok(Value::Null);
                }
                counts.sort_by(|a, b| b.1.cmp(&a.1));
                Ok(counts[0].0.clone())
            }
            AggregateFunctionType::PercentileCont => {
                let frac = agg.percentile.unwrap_or(0.5);
                let mut values: Vec<f64> = Vec::new();
                for row in &filtered_rows {
                    let val = eval_expr_for_row(row)?;
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    if agg.distinct && !seen.insert(val.clone()) {
                        continue;
                    }
                    match val {
                        Value::Integer(i) => values.push(i as f64),
                        Value::Float(f) => values.push(f),
                        _ => {
                            return Err(RustqlError::AggregateError(
                                "PERCENTILE_CONT requires numeric values".to_string(),
                            ));
                        }
                    }
                }
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
                let frac = agg.percentile.unwrap_or(0.5);
                let mut values: Vec<Value> = Vec::new();
                for row in &filtered_rows {
                    let val = eval_expr_for_row(row)?;
                    if matches!(val, Value::Null) {
                        continue;
                    }
                    if agg.distinct && !seen.insert(val.clone()) {
                        continue;
                    }
                    values.push(val);
                }
                if values.is_empty() {
                    return Ok(Value::Null);
                }
                values.sort();
                let idx = ((frac * values.len() as f64).ceil() as usize).saturating_sub(1);
                let idx = idx.min(values.len() - 1);
                Ok(values[idx].clone())
            }
        }
    }

    fn evaluate_having(
        &self,
        expr: &Expression,
        result_columns: &[ColumnDefinition],
        result_row: &[Value],
        input_columns: &[ColumnDefinition],
        group_rows: &[Vec<Value>],
    ) -> Result<bool, RustqlError> {
        match expr {
            Expression::BinaryOp { left, op, right } => match op {
                BinaryOperator::And => Ok(self.evaluate_having(
                    left,
                    result_columns,
                    result_row,
                    input_columns,
                    group_rows,
                )? && self.evaluate_having(
                    right,
                    result_columns,
                    result_row,
                    input_columns,
                    group_rows,
                )?),
                BinaryOperator::Or => Ok(self.evaluate_having(
                    left,
                    result_columns,
                    result_row,
                    input_columns,
                    group_rows,
                )? || self.evaluate_having(
                    right,
                    result_columns,
                    result_row,
                    input_columns,
                    group_rows,
                )?),
                _ => {
                    let left_val = self.evaluate_having_value(
                        left,
                        result_columns,
                        result_row,
                        input_columns,
                        group_rows,
                    )?;
                    let right_val = self.evaluate_having_value(
                        right,
                        result_columns,
                        result_row,
                        input_columns,
                        group_rows,
                    )?;
                    self.compare_values_for_where(&left_val, op, &right_val)
                }
            },
            Expression::IsNull { expr, not } => {
                let value = self.evaluate_having_value(
                    expr,
                    result_columns,
                    result_row,
                    input_columns,
                    group_rows,
                )?;
                let is_null = matches!(value, Value::Null);
                Ok(if *not { !is_null } else { is_null })
            }
            Expression::UnaryOp { op, expr } => match op {
                UnaryOperator::Not => Ok(!self.evaluate_having(
                    expr,
                    result_columns,
                    result_row,
                    input_columns,
                    group_rows,
                )?),
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
        result_columns: &[ColumnDefinition],
        result_row: &[Value],
        input_columns: &[ColumnDefinition],
        group_rows: &[Vec<Value>],
    ) -> Result<Value, RustqlError> {
        match expr {
            Expression::Function(agg) => {
                let group_rows_vec: Vec<Vec<Value>> = group_rows.to_vec();

                let col_names: Vec<String> = input_columns.iter().map(|c| c.name.clone()).collect();
                self.compute_aggregate(agg, &group_rows_vec, &col_names)
            }
            Expression::Value(val) => Ok(val.clone()),
            Expression::Column(name) => {
                let col_name = if name.contains('.') {
                    name.split('.').next_back().unwrap_or(name)
                } else {
                    name.as_str()
                };
                let idx = result_columns
                    .iter()
                    .position(|c| column_names_match(&c.name, col_name))
                    .ok_or_else(|| format!("Column '{}' not found in HAVING clause", name))?;
                Ok(result_row.get(idx).cloned().unwrap_or(Value::Null))
            }
            Expression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_having_value(
                    left,
                    result_columns,
                    result_row,
                    input_columns,
                    group_rows,
                )?;
                let right_val = self.evaluate_having_value(
                    right,
                    result_columns,
                    result_row,
                    input_columns,
                    group_rows,
                )?;
                match op {
                    BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide => self.apply_arithmetic(&left_val, &right_val, op),
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
                            (alias.clone().unwrap_or_else(|| name.clone()), col.clone())
                        }
                        Column::Expression { alias, .. } => (
                            alias.clone().unwrap_or_else(|| "<expression>".to_string()),
                            col.clone(),
                        ),
                        Column::Function(agg) => (
                            crate::executor::aggregate::format_aggregate_header(agg),
                            col.clone(),
                        ),
                        Column::Subquery(_) => ("<subquery>".to_string(), col.clone()),
                        Column::All => unreachable!(),
                    })
                    .collect()
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
                        unreachable!("Column::All should not appear in column_specs")
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

fn find_defined_column_index(columns: &[ColumnDefinition], reference: &str) -> Option<usize> {
    columns
        .iter()
        .position(|column| column.name == reference)
        .or_else(|| {
            let unqualified = unqualified_column_name(reference);
            columns
                .iter()
                .position(|column| column_names_match(&column.name, unqualified))
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
