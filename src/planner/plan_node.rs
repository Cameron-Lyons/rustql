use crate::ast::*;
use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum PlanNode {
    OneRow {
        cost: f64,
        rows: usize,
    },

    SeqScan {
        table: String,
        output_label: Option<String>,
        filter: Option<Expression>,
        cost: f64,
        rows: usize,
    },

    IndexScan {
        table: String,
        index: String,
        output_label: Option<String>,
        filter: Option<Expression>,
        cost: f64,
        rows: usize,
    },

    FunctionScan {
        function: TableFunction,
        output_label: Option<String>,
        filter: Option<Expression>,
        cost: f64,
        rows: usize,
    },

    ValuesScan {
        values: Vec<Vec<Expression>>,
        columns: Vec<String>,
        filter: Option<Expression>,
        cost: f64,
        rows: usize,
    },

    NestedLoopJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        join_type: JoinType,
        condition: Expression,
        cost: f64,
        rows: usize,
    },

    HashJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        condition: Expression,
        cost: f64,
        rows: usize,
    },

    LateralJoin {
        left: Box<PlanNode>,
        subquery: Box<SelectStatement>,
        alias: String,
        right_columns: Vec<String>,
        join_type: JoinType,
        condition: Expression,
        cost: f64,
        rows: usize,
    },

    Filter {
        input: Box<PlanNode>,
        condition: Expression,
        cost: f64,
        rows: usize,
    },

    Sort {
        input: Box<PlanNode>,
        order_by: Vec<OrderByExpr>,
        cost: f64,
        rows: usize,
    },

    DistinctOn {
        input: Box<PlanNode>,
        distinct_on: Vec<Expression>,
        cost: f64,
        rows: usize,
    },

    Limit {
        input: Box<PlanNode>,
        limit: usize,
        offset: usize,
        with_ties: bool,
        order_by: Vec<OrderByExpr>,
        cost: f64,
        rows: usize,
    },

    Aggregate {
        input: Box<PlanNode>,
        group_by: Vec<Expression>,
        grouping_sets: Option<Vec<Vec<Expression>>>,
        aggregates: Vec<AggregateFunction>,
        having: Option<Expression>,
        cost: f64,
        rows: usize,
    },

    SetOperation {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        left_select: Box<SelectStatement>,
        right_select: Box<SelectStatement>,
        op: SetOperation,
        cost: f64,
        rows: usize,
    },
}

impl fmt::Display for PlanNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.fmt_with_indent(f, 0)
    }
}

impl PlanNode {
    fn fmt_with_indent(&self, f: &mut fmt::Formatter<'_>, indent: usize) -> fmt::Result {
        let indent_str = "  ".repeat(indent);
        match self {
            PlanNode::OneRow { cost, rows } => {
                writeln!(f, "{}Result", indent_str)?;
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)
            }
            PlanNode::SeqScan {
                table,
                filter,
                cost,
                rows,
                ..
            } => {
                writeln!(f, "{}Seq Scan on {}", indent_str, table)?;
                if filter.is_some() {
                    writeln!(f, "{}  Filter: [WHERE clause]", indent_str)?;
                }
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)
            }
            PlanNode::FunctionScan {
                function,
                filter,
                cost,
                rows,
                ..
            } => {
                writeln!(f, "{}Function Scan on {}", indent_str, function.name)?;
                if filter.is_some() {
                    writeln!(f, "{}  Filter: [WHERE clause]", indent_str)?;
                }
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)
            }
            PlanNode::ValuesScan {
                columns,
                filter,
                cost,
                rows,
                ..
            } => {
                writeln!(f, "{}Values Scan", indent_str)?;
                if !columns.is_empty() {
                    writeln!(f, "{}  Output: {}", indent_str, columns.join(", "))?;
                }
                if filter.is_some() {
                    writeln!(f, "{}  Filter: [WHERE clause]", indent_str)?;
                }
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)
            }
            PlanNode::IndexScan {
                table,
                index,
                filter,
                cost,
                rows,
                ..
            } => {
                writeln!(f, "{}Index Scan using {} on {}", indent_str, index, table)?;
                if filter.is_some() {
                    writeln!(f, "{}  Filter: [WHERE clause]", indent_str)?;
                }
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)
            }
            PlanNode::NestedLoopJoin {
                left,
                right,
                join_type,
                condition: _,
                cost,
                rows,
            } => {
                let join_label = match join_type {
                    JoinType::Left => "Nested Loop Left Join",
                    JoinType::Right => "Nested Loop Right Join",
                    JoinType::Full => "Nested Loop Full Join",
                    JoinType::Natural => "Nested Loop Natural Join",
                    JoinType::Cross => "Nested Loop Cross Join",
                    _ => "Nested Loop Join",
                };
                writeln!(f, "{}{}", indent_str, join_label)?;
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
                left.fmt_with_indent(f, indent + 1)?;
                right.fmt_with_indent(f, indent + 1)
            }
            PlanNode::HashJoin {
                left,
                right,
                condition: _,
                cost,
                rows,
            } => {
                writeln!(f, "{}Hash Join", indent_str)?;
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
                left.fmt_with_indent(f, indent + 1)?;
                right.fmt_with_indent(f, indent + 1)
            }
            PlanNode::LateralJoin {
                left,
                alias,
                right_columns,
                join_type,
                condition,
                cost,
                rows,
                ..
            } => {
                let join_label = match join_type {
                    JoinType::Left => "Lateral Left Join",
                    JoinType::Right => "Lateral Right Join",
                    JoinType::Full => "Lateral Full Join",
                    JoinType::Cross => "Lateral Cross Join",
                    JoinType::Natural => "Lateral Natural Join",
                    JoinType::Inner => "Lateral Join",
                };
                writeln!(f, "{}{}", indent_str, join_label)?;
                writeln!(f, "{}  Alias: {}", indent_str, alias)?;
                if !matches!(condition, Expression::Value(Value::Boolean(true))) {
                    writeln!(f, "{}  Condition: {:?}", indent_str, condition)?;
                }
                if !right_columns.is_empty() {
                    writeln!(f, "{}  Output: {}", indent_str, right_columns.join(", "))?;
                }
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
                left.fmt_with_indent(f, indent + 1)
            }
            PlanNode::Filter {
                input,
                condition: _,
                cost,
                rows,
            } => {
                writeln!(f, "{}Filter", indent_str)?;
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
                input.fmt_with_indent(f, indent + 1)
            }
            PlanNode::Sort {
                input,
                order_by: _,
                cost,
                rows,
            } => {
                writeln!(f, "{}Sort", indent_str)?;
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
                input.fmt_with_indent(f, indent + 1)
            }
            PlanNode::DistinctOn {
                input,
                distinct_on,
                cost,
                rows,
            } => {
                let distinct_strs: Vec<String> = distinct_on
                    .iter()
                    .map(|expr| format!("{:?}", expr))
                    .collect();
                writeln!(
                    f,
                    "{}Distinct On ({})",
                    indent_str,
                    distinct_strs.join(", ")
                )?;
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
                input.fmt_with_indent(f, indent + 1)
            }
            PlanNode::Limit {
                input,
                limit,
                offset,
                with_ties,
                order_by,
                cost,
                rows,
            } => {
                let with_ties_suffix = if *with_ties && !order_by.is_empty() {
                    " With Ties"
                } else {
                    ""
                };
                writeln!(
                    f,
                    "{}Limit: {} Offset: {}{}",
                    indent_str, limit, offset, with_ties_suffix
                )?;
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
                input.fmt_with_indent(f, indent + 1)
            }
            PlanNode::Aggregate {
                input,
                group_by,
                grouping_sets,
                aggregates: _,
                having: _,
                cost,
                rows,
            } => {
                if let Some(sets) = grouping_sets {
                    let grouping_set_strs: Vec<String> = sets
                        .iter()
                        .map(|set| {
                            if set.is_empty() {
                                "()".to_string()
                            } else {
                                format!(
                                    "({})",
                                    set.iter()
                                        .map(|expr| format!("{:?}", expr))
                                        .collect::<Vec<_>>()
                                        .join(", ")
                                )
                            }
                        })
                        .collect();
                    writeln!(
                        f,
                        "{}Aggregate (Grouping Sets: {})",
                        indent_str,
                        grouping_set_strs.join(", ")
                    )?;
                } else {
                    let group_by_strs: Vec<String> =
                        group_by.iter().map(|e| format!("{:?}", e)).collect();
                    writeln!(
                        f,
                        "{}Aggregate (Group By: {})",
                        indent_str,
                        group_by_strs.join(", ")
                    )?;
                }
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
                input.fmt_with_indent(f, indent + 1)
            }
            PlanNode::SetOperation {
                left,
                right,
                op,
                cost,
                rows,
                ..
            } => {
                let label = match op {
                    SetOperation::Union => "Union",
                    SetOperation::UnionAll => "Union All",
                    SetOperation::Intersect => "Intersect",
                    SetOperation::IntersectAll => "Intersect All",
                    SetOperation::Except => "Except",
                    SetOperation::ExceptAll => "Except All",
                };
                writeln!(f, "{}Set Operation ({})", indent_str, label)?;
                writeln!(f, "{}  Cost: {:.2}, Rows: {}", indent_str, cost, rows)?;
                left.fmt_with_indent(f, indent + 1)?;
                right.fmt_with_indent(f, indent + 1)
            }
        }
    }
}
