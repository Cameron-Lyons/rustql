use crate::ast::*;
use crate::database::Database;
use crate::planner::PlanNode;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashSet};

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
    ) -> Result<ExecutionResult, String> {
        let result = self.execute_plan_node(plan)?;

        let projected = self.apply_projection(&result, select_stmt)?;
        let distincted = if select_stmt.distinct {
            self.apply_distinct(projected)?
        } else {
            projected
        };

        Ok(distincted)
    }

    fn execute_plan_node(&self, plan: &PlanNode) -> Result<ExecutionResult, String> {
        match plan {
            PlanNode::SeqScan { table, filter, .. } => {
                self.execute_seq_scan(table, filter.as_ref())
            }
            PlanNode::IndexScan {
                table,
                index,
                filter,
                ..
            } => self.execute_index_scan(table, index, filter.as_ref()),
            PlanNode::Filter {
                input, condition, ..
            } => {
                let input_result = self.execute_plan_node(input)?;
                self.execute_filter(input_result, condition)
            }
            PlanNode::NestedLoopJoin {
                left,
                right,
                condition,
                ..
            } => {
                let left_result = self.execute_plan_node(left)?;
                let right_result = self.execute_plan_node(right)?;
                self.execute_nested_loop_join(left_result, right_result, condition)
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
            PlanNode::Sort {
                input, order_by, ..
            } => {
                let input_result = self.execute_plan_node(input)?;
                self.execute_sort(input_result, order_by)
            }
            PlanNode::Limit {
                input,
                limit,
                offset,
                ..
            } => {
                let input_result = self.execute_plan_node(input)?;
                self.execute_limit(input_result, *limit, *offset)
            }
            PlanNode::Aggregate {
                input,
                group_by,
                aggregates,
                having,
                ..
            } => {
                let input_result = self.execute_plan_node(input)?;
                self.execute_aggregate(input_result, group_by, aggregates, having.as_ref())
            }
        }
    }

    fn execute_seq_scan(
        &self,
        table_name: &str,
        filter: Option<&Expression>,
    ) -> Result<ExecutionResult, String> {
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

        let columns: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();

        Ok(ExecutionResult { columns, rows })
    }

    fn execute_index_scan(
        &self,
        table_name: &str,
        index_name: &str,
        filter: Option<&Expression>,
    ) -> Result<ExecutionResult, String> {
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
            if row_idx < table.rows.len() {
                let row = &table.rows[row_idx];

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

        let columns: Vec<String> = table.columns.iter().map(|c| c.name.clone()).collect();

        Ok(ExecutionResult { columns, rows })
    }

    fn execute_filter(
        &self,
        input: ExecutionResult,
        condition: &Expression,
    ) -> Result<ExecutionResult, String> {
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
                    default_value: None,
                    foreign_key: None,
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
        condition: &Expression,
    ) -> Result<ExecutionResult, String> {
        let mut joined_rows = Vec::new();
        let mut joined_columns = left.columns.clone();
        joined_columns.extend(right.columns.iter().map(|c| format!("{}.{}", "right", c)));

        for left_row in &left.rows {
            for right_row in &right.rows {
                let combined_row: Vec<Value> =
                    left_row.iter().chain(right_row.iter()).cloned().collect();

                let include =
                    self.evaluate_join_condition(condition, &left, &right, left_row, right_row)?;

                if include {
                    joined_rows.push(combined_row);
                }
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
    ) -> Result<ExecutionResult, String> {
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
        joined_columns.extend(right.columns.iter().map(|c| format!("{}.{}", "right", c)));

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
    ) -> Result<ExecutionResult, String> {
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
    ) -> Result<ExecutionResult, String> {
        let mut rows = input.rows;

        if offset < rows.len() {
            rows = rows.split_off(offset);
        } else {
            rows.clear();
        }

        if rows.len() > limit {
            rows.truncate(limit);
        }

        Ok(ExecutionResult {
            columns: input.columns,
            rows,
        })
    }

    fn execute_aggregate(
        &self,
        input: ExecutionResult,
        group_by: &[String],
        aggregates: &[AggregateFunction],
        having: Option<&Expression>,
    ) -> Result<ExecutionResult, String> {
        let group_by_indices: Vec<usize> = group_by
            .iter()
            .filter_map(|col| input.columns.iter().position(|c| c == col))
            .collect();

        let mut groups: BTreeMap<Vec<Value>, Vec<Vec<Value>>> = BTreeMap::new();
        for row in &input.rows {
            let key: Vec<Value> = group_by_indices
                .iter()
                .map(|&idx| row[idx].clone())
                .collect();
            groups.entry(key).or_default().push(row.clone());
        }

        let mut result_rows = Vec::new();
        for (group_key, group_rows) in groups {
            let mut result_row = group_key.clone();

            for agg in aggregates {
                let agg_value = self.compute_aggregate(agg, &group_rows, &input.columns)?;
                result_row.push(agg_value);
            }

            if let Some(_having_expr) = having {}

            result_rows.push(result_row);
        }

        let mut result_columns = group_by.to_vec();
        for agg in aggregates {
            result_columns.push(format!("{:?}", agg.function));
        }

        Ok(ExecutionResult {
            columns: result_columns,
            rows: result_rows,
        })
    }

    fn evaluate_expression(
        &self,
        _expr: &Expression,
        _columns: &[ColumnDefinition],
        _row: &[Value],
    ) -> Result<bool, String> {
        Ok(true)
    }

    fn extract_filter_value(&self, expr: &Expression, column: &str) -> Option<Value> {
        if let Expression::BinaryOp {
            left,
            op: BinaryOperator::Equal,
            right,
        } = expr
        {
            if let Expression::Column(col) = left.as_ref() {
                if col.ends_with(column) || col == column {
                    if let Expression::Value(v) = right.as_ref() {
                        return Some(v.clone());
                    }
                }
            }
        }
        None
    }

    fn evaluate_join_condition(
        &self,
        _condition: &Expression,
        _left: &ExecutionResult,
        _right: &ExecutionResult,
        _left_row: &[Value],
        _right_row: &[Value],
    ) -> Result<bool, String> {
        Ok(true)
    }

    fn extract_join_keys(
        &self,
        condition: &Expression,
        build_cols: &[String],
        probe_cols: &[String],
    ) -> Result<(usize, usize), String> {
        if let Expression::BinaryOp {
            left,
            op: BinaryOperator::Equal,
            right,
        } = condition
        {
            if let (Expression::Column(left_col), Expression::Column(right_col)) =
                (left.as_ref(), right.as_ref())
            {
                let build_idx = build_cols
                    .iter()
                    .position(|c| c.ends_with(left_col.split('.').last().unwrap_or(left_col)));
                let probe_idx = probe_cols
                    .iter()
                    .position(|c| c.ends_with(right_col.split('.').last().unwrap_or(right_col)));

                if let (Some(bi), Some(pi)) = (build_idx, probe_idx) {
                    return Ok((bi, pi));
                }
            }
        }
        Err("Could not extract join keys from condition".to_string())
    }

    fn get_sort_value(
        &self,
        expr: &Expression,
        columns: &[String],
        row: &[Value],
    ) -> Option<Value> {
        if let Expression::Column(col) = expr {
            let col_name = col.split('.').last().unwrap_or(col);
            if let Some(idx) = columns
                .iter()
                .position(|c| c == col_name || c.ends_with(col_name))
            {
                return row.get(idx).cloned();
            }
        }
        None
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
    ) -> Result<Value, String> {
        let col_idx = if let Expression::Column(col) = agg.expr.as_ref() {
            columns.iter().position(|c| c == col).unwrap_or(0)
        } else {
            0
        };

        let values: Vec<Value> = rows
            .iter()
            .filter_map(|row| row.get(col_idx).cloned())
            .collect();

        match agg.function {
            AggregateFunctionType::Count => Ok(Value::Integer(values.len() as i64)),
            AggregateFunctionType::Sum => {
                let sum: f64 = values
                    .iter()
                    .filter_map(|v| match v {
                        Value::Integer(i) => Some(*i as f64),
                        Value::Float(f) => Some(*f),
                        _ => None,
                    })
                    .sum();
                Ok(Value::Float(sum))
            }
            AggregateFunctionType::Avg => {
                let sum: f64 = values
                    .iter()
                    .filter_map(|v| match v {
                        Value::Integer(i) => Some(*i as f64),
                        Value::Float(f) => Some(*f),
                        _ => None,
                    })
                    .sum();
                Ok(Value::Float(if values.is_empty() {
                    0.0
                } else {
                    sum / values.len() as f64
                }))
            }
            AggregateFunctionType::Min => Ok(values
                .into_iter()
                .reduce(|a, b| {
                    if self.compare_values(&a, &b) == Ordering::Less {
                        a
                    } else {
                        b
                    }
                })
                .unwrap_or(Value::Null)),
            AggregateFunctionType::Max => Ok(values
                .into_iter()
                .reduce(|a, b| {
                    if self.compare_values(&a, &b) == Ordering::Greater {
                        a
                    } else {
                        b
                    }
                })
                .unwrap_or(Value::Null)),
        }
    }

    fn apply_projection(
        &self,
        result: &ExecutionResult,
        _select_stmt: &SelectStatement,
    ) -> Result<ExecutionResult, String> {
        Ok(result.clone())
    }

    fn apply_distinct(&self, input: ExecutionResult) -> Result<ExecutionResult, String> {
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
