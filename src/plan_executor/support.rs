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
    let mut combined = column_definitions_from_names(left);
    combined.extend(column_definitions_from_names(right));
    combined
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
