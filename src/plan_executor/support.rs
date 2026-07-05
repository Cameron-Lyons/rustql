use super::*;

pub(super) fn column_names_match(candidate: &str, reference: &str) -> bool {
    candidate == reference
        || unqualified_column_name(candidate) == unqualified_column_name(reference)
}

pub(super) fn find_result_column_index(columns: &[String], reference: &str) -> Option<usize> {
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

pub(super) fn find_aggregate_result_column_index(
    columns: &[String],
    aggregate: &AggregateFunction,
) -> Option<usize> {
    let internal_name = format!("{:?}", aggregate.function);
    find_result_column_index(columns, &internal_name)
        .or_else(|| find_result_column_index(columns, &format_aggregate_header(aggregate)))
        .or_else(|| {
            aggregate
                .alias
                .as_deref()
                .and_then(|alias| find_result_column_index(columns, alias))
        })
}

pub(super) fn qualify_column_names(
    columns: &[ColumnDefinition],
    output_label: Option<&str>,
) -> Vec<String> {
    columns
        .iter()
        .map(|column| qualified_column_name(output_label, &column.name))
        .collect()
}

pub(super) fn qualified_column_name(output_label: Option<&str>, column_name: &str) -> String {
    output_label
        .map(|label| format!("{}.{}", label, column_name))
        .unwrap_or_else(|| column_name.to_string())
}

pub(super) fn unqualified_column_name(name: &str) -> &str {
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

pub(super) fn column_definitions_from_names(columns: &[String]) -> Vec<ColumnDefinition> {
    columns
        .iter()
        .map(|name| synthetic_column_definition(name))
        .collect()
}

pub(super) fn column_definitions_from_result(result: &ExecutionResult) -> Vec<ColumnDefinition> {
    result
        .columns
        .iter()
        .enumerate()
        .map(|(idx, name)| {
            let (data_type, nullable) = infer_result_column(&result.rows, idx);
            ColumnDefinition {
                name: name.clone(),
                data_type,
                nullable,
                primary_key: false,
                unique: false,
                default_value: None,
                foreign_key: None,
                check: None,
                auto_increment: false,
                generated: None,
            }
        })
        .collect()
}

fn infer_result_column(rows: &[Vec<Value>], idx: usize) -> (DataType, bool) {
    let mut data_type = None;
    let mut nullable = false;

    for row in rows {
        match row.get(idx) {
            Some(Value::Null) | None => nullable = true,
            Some(value) if data_type.is_none() => {
                data_type = value_data_type(value);
            }
            Some(_) => {}
        }

        if nullable && data_type.is_some() {
            break;
        }
    }

    (data_type.unwrap_or(DataType::Text), nullable)
}

fn value_data_type(value: &Value) -> Option<DataType> {
    match value {
        Value::Null => None,
        Value::Integer(_) => Some(DataType::Integer),
        Value::Float(_) => Some(DataType::Float),
        Value::Text(_) => Some(DataType::Text),
        Value::Boolean(_) => Some(DataType::Boolean),
        Value::Date(_) => Some(DataType::Date),
        Value::Time(_) => Some(DataType::Time),
        Value::DateTime(_) => Some(DataType::DateTime),
    }
}

pub(super) fn scalar_outer_scope_columns(
    columns: &[String],
    select_stmt: &SelectStatement,
) -> Vec<ColumnDefinition> {
    let mut definitions = column_definitions_from_names(columns);
    let source_label = if select_stmt.joins.is_empty() {
        if !select_stmt.from.is_empty() {
            select_stmt
                .from_alias
                .clone()
                .or_else(|| Some(select_stmt.from.clone()))
        } else if let Some((_, alias, _)) = select_stmt.from_values.as_ref() {
            Some(alias.clone())
        } else {
            select_stmt
                .from_function
                .as_ref()
                .and_then(|function| function.alias.clone())
        }
    } else {
        None
    };

    if let Some(label) = source_label {
        for column in &mut definitions {
            if !column.name.contains('.') {
                column.name = format!("{}.{}", label, column.name);
            }
        }
    }

    definitions
}

pub(super) fn combined_column_definitions(
    left: &[String],
    right: &[String],
) -> Vec<ColumnDefinition> {
    let mut combined = Vec::with_capacity(left.len() + right.len());
    combined.extend(left.iter().map(|name| synthetic_column_definition(name)));
    combined.extend(right.iter().map(|name| synthetic_column_definition(name)));
    combined
}

fn synthetic_column_definition(name: &str) -> ColumnDefinition {
    ColumnDefinition {
        name: name.to_string(),
        data_type: DataType::Text,
        nullable: true,
        primary_key: false,
        unique: false,
        default_value: None,
        foreign_key: None,
        check: None,
        auto_increment: false,
        generated: None,
    }
}

pub(super) fn combine_rows(left: &[Value], right: &[Value]) -> Vec<Value> {
    let mut combined = Vec::with_capacity(left.len() + right.len());
    combined.extend_from_slice(left);
    combined.extend_from_slice(right);
    combined
}

pub(super) fn combine_row_with_right_nulls(left: &[Value], right_len: usize) -> Vec<Value> {
    let mut combined = Vec::with_capacity(left.len() + right_len);
    combined.extend_from_slice(left);
    combined.resize(left.len() + right_len, Value::Null);
    combined
}

pub(super) fn combine_row_with_left_nulls(left_len: usize, right: &[Value]) -> Vec<Value> {
    let mut combined = Vec::with_capacity(left_len + right.len());
    combined.resize(left_len, Value::Null);
    combined.extend_from_slice(right);
    combined
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_column_inference_uses_first_non_null_type() {
        let result = ExecutionResult {
            columns: vec!["price".to_string()],
            rows: vec![vec![Value::Null], vec![Value::Float(9.99)]],
        };

        let columns = column_definitions_from_result(&result);

        assert_eq!(columns[0].name, "price");
        assert_eq!(columns[0].data_type, DataType::Float);
        assert!(columns[0].nullable);
    }

    #[test]
    fn result_column_inference_marks_missing_cells_nullable() {
        let result = ExecutionResult {
            columns: vec!["id".to_string()],
            rows: vec![Vec::new(), vec![Value::Integer(1)]],
        };

        let columns = column_definitions_from_result(&result);

        assert_eq!(columns[0].data_type, DataType::Integer);
        assert!(columns[0].nullable);
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) struct NumericJoinKey(pub(super) f64);

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
pub(super) enum NonNumericJoinKey {
    Text(String),
    Boolean(bool),
    Date(String),
    Time(String),
    DateTime(String),
}

pub(super) enum JoinKey {
    /// Integer key, carried as its `as f64` cast because the expression
    /// evaluator compares all numerics through f64.
    Integer(f64),
    Float(f64),
    NonNumeric(NonNumericJoinKey),
}

/// Hash key for numeric build values: exact f64 bit pattern, with -0.0
/// collapsed onto 0.0 so the two zero encodings share one table entry.
pub(super) fn numeric_key_bits(value: f64) -> u64 {
    (value + 0.0).to_bits()
}

pub(super) fn join_key(value: &Value) -> Option<JoinKey> {
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

/// Mirrors the expression evaluator's column resolution (exact name first,
/// then unqualified-suffix matching) so the fast path picks the same cells
/// the evaluator would.
pub(super) fn resolve_combined_column(columns: &[ColumnDefinition], name: &str) -> Option<usize> {
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
