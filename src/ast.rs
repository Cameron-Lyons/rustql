use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq)]
pub enum Statement {
    Select(SelectStatement),
    Insert(InsertStatement),
    Update(UpdateStatement),
    Delete(DeleteStatement),
    CreateTable(CreateTableStatement),
    DropTable(DropTableStatement),
    AlterTable(AlterTableStatement),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub distinct: bool,
    pub columns: Vec<Column>,
    pub from: String,
    pub joins: Vec<Join>,
    pub where_clause: Option<Expression>,
    pub group_by: Option<Vec<String>>,
    pub having: Option<Expression>,
    pub order_by: Option<Vec<OrderByExpr>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub join_type: JoinType,
    pub table: String,
    pub on: Expression,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Column {
    All,
    Named { name: String, alias: Option<String> },
    Function(AggregateFunction),
    Subquery(Box<SelectStatement>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateFunction {
    pub function: AggregateFunctionType,
    pub expr: Box<Expression>,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunctionType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Value>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTableStatement {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    Float,
    Text,
    Boolean,
    Date,
    Time,
    DateTime,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropTableStatement {
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AlterTableStatement {
    pub table: String,
    pub operation: AlterOperation,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterOperation {
    AddColumn(ColumnDefinition),
    DropColumn(String),
    RenameColumn { old: String, new: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum Expression {
    BinaryOp {
        left: Box<Expression>,
        op: BinaryOperator,
        right: Box<Expression>,
    },
    UnaryOp {
        op: UnaryOperator,
        expr: Box<Expression>,
    },
    In {
        left: Box<Expression>,
        values: Vec<Value>,
    },
    IsNull {
        expr: Box<Expression>,
        not: bool,
    },
    Subquery(Box<SelectStatement>),
    Exists(Box<SelectStatement>),
    Column(String),
    Value(Value),
    Function(AggregateFunction),
}

#[derive(Debug, Clone, PartialEq)]
pub enum BinaryOperator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    And,
    Or,
    Plus,
    Minus,
    Multiply,
    Divide,
    Like,
    Between,
    In,
}

#[derive(Debug, Clone, PartialEq)]
pub enum UnaryOperator {
    Not,
    Minus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Value {
    #[allow(dead_code)]
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    #[allow(dead_code)]
    Boolean(bool),
    Date(String),     // ISO format: YYYY-MM-DD
    Time(String),     // ISO format: HH:MM:SS
    DateTime(String), // ISO format: YYYY-MM-DD HH:MM:SS
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: Expression,
    pub asc: bool,
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Null, _) => Ordering::Less,
            (_, Value::Null) => Ordering::Greater,
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => {
                if a < b {
                    Ordering::Less
                } else if a > b {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            }
            (Value::Integer(a), Value::Float(b)) => {
                let a = *a as f64;
                if a < *b {
                    Ordering::Less
                } else if a > *b {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            }
            (Value::Float(a), Value::Integer(b)) => {
                let b = *b as f64;
                if a < &b {
                    Ordering::Less
                } else if a > &b {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                }
            }
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::Time(a), Value::Time(b)) => a.cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.cmp(b),
            _ => Ordering::Equal,
        }
    }
}
