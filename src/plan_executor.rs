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

            // Apply HAVING clause filter
            if let Some(having_expr) = having {
                // Create column definitions for the result row (group by columns + aggregates)
                let mut result_columns: Vec<String> = group_by.to_vec();
                for agg in aggregates {
                    result_columns.push(format!("{:?}", agg.function));
                }
                let result_column_defs: Vec<ColumnDefinition> = result_columns
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

                // Create column definitions for the input (for evaluating aggregates in HAVING)
                let input_column_defs: Vec<ColumnDefinition> = input
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

                // Evaluate HAVING clause - it can reference group by columns and aggregates
                let include = self.evaluate_having(
                    having_expr,
                    &result_column_defs,
                    &result_row,
                    &input_column_defs,
                    &group_rows,
                )?;

                if !include {
                    continue;
                }
            }

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
        expr: &Expression,
        columns: &[ColumnDefinition],
        row: &[Value],
    ) -> Result<bool, String> {
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
                        _ => Err("LIKE operator requires text values".to_string()),
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
                        _ => Err("BETWEEN requires two values".to_string()),
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
                _ => Err("Unsupported unary operation in WHERE clause".to_string()),
            },
            _ => Err("Invalid expression in WHERE clause".to_string()),
        }
    }

    fn evaluate_value_expression(
        &self,
        expr: &Expression,
        columns: &[ColumnDefinition],
        row: &[Value],
    ) -> Result<Value, String> {
        match expr {
            Expression::Column(name) => {
                if name == "*" {
                    return Ok(Value::Integer(1));
                }
                let col_name = if name.contains('.') {
                    name.split('.').next_back().unwrap_or(name)
                } else {
                    name
                };
                let idx = columns
                    .iter()
                    .position(|c| c.name == col_name)
                    .ok_or_else(|| format!("Column '{}' not found", name))?;
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
                    _ => Err(
                        "Only arithmetic operators are supported in SELECT expressions".to_string(),
                    ),
                }
            }
            Expression::UnaryOp { op, expr } => {
                let val = self.evaluate_value_expression(expr, columns, row)?;
                match op {
                    UnaryOperator::Minus => match val {
                        Value::Integer(n) => Ok(Value::Integer(-n)),
                        Value::Float(f) => Ok(Value::Float(-f)),
                        _ => Err("Unary minus only supported for numeric types".to_string()),
                    },
                    _ => Err("Unsupported unary operator in SELECT expression".to_string()),
                }
            }
            _ => Err("Complex expressions not yet supported in SELECT".to_string()),
        }
    }

    fn apply_arithmetic(
        &self,
        left: &Value,
        right: &Value,
        op: &BinaryOperator,
    ) -> Result<Value, String> {
        let to_float = |value: &Value| -> Result<f64, String> {
            match value {
                Value::Integer(i) => Ok(*i as f64),
                Value::Float(f) => Ok(*f),
                Value::Null => Ok(0.0),
                _ => Err("Arithmetic requires numeric values".to_string()),
            }
        };

        match (left, right) {
            (Value::Integer(l), Value::Integer(r)) => match op {
                BinaryOperator::Plus => Ok(Value::Integer(l + r)),
                BinaryOperator::Minus => Ok(Value::Integer(l - r)),
                BinaryOperator::Multiply => Ok(Value::Integer(l * r)),
                BinaryOperator::Divide => {
                    if *r == 0 {
                        Err("Division by zero".to_string())
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
                            Err("Division by zero".to_string())
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
    ) -> Result<bool, String> {
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
                _ => return Err("Invalid operator for numeric comparison".to_string()),
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
                    _ => return Err("Invalid operator for strings".to_string()),
                }),
                (Value::Boolean(l), Value::Boolean(r), op) => Ok(match op {
                    BinaryOperator::Equal => l == r,
                    BinaryOperator::NotEqual => l != r,
                    _ => return Err("Invalid operator for booleans".to_string()),
                }),
                (Value::Date(l), Value::Date(r), op) => Ok(match op {
                    BinaryOperator::Equal => l == r,
                    BinaryOperator::NotEqual => l != r,
                    BinaryOperator::LessThan => l < r,
                    BinaryOperator::LessThanOrEqual => l <= r,
                    BinaryOperator::GreaterThan => l > r,
                    BinaryOperator::GreaterThanOrEqual => l >= r,
                    _ => return Err("Invalid operator for dates".to_string()),
                }),
                (Value::Time(l), Value::Time(r), op) => Ok(match op {
                    BinaryOperator::Equal => l == r,
                    BinaryOperator::NotEqual => l != r,
                    BinaryOperator::LessThan => l < r,
                    BinaryOperator::LessThanOrEqual => l <= r,
                    BinaryOperator::GreaterThan => l > r,
                    BinaryOperator::GreaterThanOrEqual => l >= r,
                    _ => return Err("Invalid operator for times".to_string()),
                }),
                (Value::DateTime(l), Value::DateTime(r), op) => Ok(match op {
                    BinaryOperator::Equal => l == r,
                    BinaryOperator::NotEqual => l != r,
                    BinaryOperator::LessThan => l < r,
                    BinaryOperator::LessThanOrEqual => l <= r,
                    BinaryOperator::GreaterThan => l > r,
                    BinaryOperator::GreaterThanOrEqual => l >= r,
                    _ => return Err("Invalid operator for datetimes".to_string()),
                }),
                _ => Err("Cannot compare incompatible types".to_string()),
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
        condition: &Expression,
        left: &ExecutionResult,
        right: &ExecutionResult,
        left_row: &[Value],
        right_row: &[Value],
    ) -> Result<bool, String> {
        // Combine columns and rows for evaluation
        let mut combined_columns: Vec<ColumnDefinition> = left
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
        combined_columns.extend(right.columns.iter().map(|name| ColumnDefinition {
            name: format!("right.{}", name),
            data_type: DataType::Text,
            nullable: true,
            primary_key: false,
            default_value: None,
            foreign_key: None,
        }));

        let combined_row: Vec<Value> = left_row.iter().chain(right_row.iter()).cloned().collect();

        self.evaluate_expression(condition, &combined_columns, &combined_row)
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

    fn evaluate_having(
        &self,
        expr: &Expression,
        result_columns: &[ColumnDefinition],
        result_row: &[Value],
        input_columns: &[ColumnDefinition],
        group_rows: &[Vec<Value>],
    ) -> Result<bool, String> {
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
                _ => Err("Unsupported unary operation in HAVING clause".to_string()),
            },
            _ => Err("Invalid expression in HAVING clause".to_string()),
        }
    }

    fn evaluate_having_value(
        &self,
        expr: &Expression,
        result_columns: &[ColumnDefinition],
        result_row: &[Value],
        input_columns: &[ColumnDefinition],
        group_rows: &[Vec<Value>],
    ) -> Result<Value, String> {
        match expr {
            Expression::Function(agg) => {
                // Evaluate aggregate function over the group
                let group_rows_vec: Vec<Vec<Value>> = group_rows.iter().cloned().collect();
                // Use input_columns for aggregate computation (they represent the original table columns)
                let col_names: Vec<String> = input_columns.iter().map(|c| c.name.clone()).collect();
                self.compute_aggregate(agg, &group_rows_vec, &col_names)
            }
            Expression::Value(val) => Ok(val.clone()),
            Expression::Column(name) => {
                // Look up in result columns (group by or aggregate result)
                let col_name = if name.contains('.') {
                    name.split('.').next_back().unwrap_or(name)
                } else {
                    name.as_str()
                };
                let idx = result_columns
                    .iter()
                    .position(|c| {
                        let c_name = if c.name.contains('.') {
                            c.name.split('.').next_back().unwrap_or(&c.name)
                        } else {
                            &c.name
                        };
                        c_name == col_name || c.name.ends_with(col_name)
                    })
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
                    _ => Err(
                        "Only arithmetic operators are supported in HAVING expressions".to_string(),
                    ),
                }
            }
            _ => Err("Complex expressions not yet supported in HAVING".to_string()),
        }
    }

    fn apply_projection(
        &self,
        result: &ExecutionResult,
        select_stmt: &SelectStatement,
    ) -> Result<ExecutionResult, String> {
        // Convert column names to ColumnDefinition for evaluation
        let column_defs: Vec<ColumnDefinition> = result
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

        // Determine which columns to project
        let column_specs: Vec<(String, Column)> =
            if matches!(select_stmt.columns.get(0), Some(Column::All)) {
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
                        Column::Function(agg) => {
                            let alias = agg
                                .alias
                                .clone()
                                .unwrap_or_else(|| format!("{:?}", agg.function));
                            (alias, col.clone())
                        }
                        Column::Subquery(_) => ("<subquery>".to_string(), col.clone()),
                        Column::All => unreachable!(),
                    })
                    .collect()
            };

        let mut projected_rows = Vec::new();
        for row in &result.rows {
            let mut projected_row = Vec::new();
            for (_, col) in &column_specs {
                let val = match col {
                    Column::All => {
                        unreachable!("Column::All should not appear in column_specs")
                    }
                    Column::Named { name, .. } => {
                        let column_name = if name.contains('.') {
                            name.split('.').next_back().unwrap_or(name)
                        } else {
                            name.as_str()
                        };
                        let idx = result
                            .columns
                            .iter()
                            .position(|c| {
                                let c_name = if c.contains('.') {
                                    c.split('.').next_back().unwrap_or(c)
                                } else {
                                    c.as_str()
                                };
                                c_name == column_name || c.ends_with(column_name)
                            })
                            .ok_or_else(|| format!("Column '{}' not found", name))?;
                        row.get(idx).cloned().unwrap_or(Value::Null)
                    }
                    Column::Expression { expr, .. } => {
                        self.evaluate_value_expression(expr, &column_defs, row)?
                    }
                    Column::Function(_) => {
                        return Err(
                            "Aggregate functions should be handled before projection".to_string()
                        );
                    }
                    Column::Subquery(_) => {
                        return Err(
                            "Subqueries in SELECT list not yet supported in plan executor"
                                .to_string(),
                        );
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
