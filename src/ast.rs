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
    CreateIndex(CreateIndexStatement),
    DropIndex(DropIndexStatement),
    TruncateTable { table_name: String },
    CreateView { name: String, query_sql: String },
    DropView { name: String, if_exists: bool },
    BeginTransaction,
    CommitTransaction,
    RollbackTransaction,
    Savepoint(String),
    ReleaseSavepoint(String),
    RollbackToSavepoint(String),
    Explain(SelectStatement),
    Describe(String),
    ShowTables,
    Analyze(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct SelectStatement {
    pub ctes: Vec<Cte>,
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
    pub union: Option<Box<SelectStatement>>,
    pub union_all: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Cte {
    pub name: String,
    pub query: SelectStatement,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Join {
    pub join_type: JoinType,
    pub table: String,
    pub on: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Natural,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Column {
    All,
    Named {
        name: String,
        alias: Option<String>,
    },
    Function(AggregateFunction),
    Subquery(Box<SelectStatement>),
    Expression {
        expr: Expression,
        alias: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateFunction {
    pub function: AggregateFunctionType,
    pub expr: Box<Expression>,
    pub distinct: bool,
    pub alias: Option<String>,
    pub separator: Option<String>,
    pub percentile: Option<f64>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggregateFunctionType {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Stddev,
    Variance,
    GroupConcat,
    BoolAnd,
    BoolOr,
    Median,
    Mode,
    PercentileCont,
    PercentileDisc,
}

#[derive(Debug, Clone, PartialEq)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Value>>,
    pub source_query: Option<Box<SelectStatement>>,
    pub on_conflict: Option<OnConflictClause>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OnConflictClause {
    pub columns: Vec<String>,
    pub action: OnConflictAction,
}

#[derive(Debug, Clone, PartialEq)]
pub enum OnConflictAction {
    DoNothing,
    DoUpdate { assignments: Vec<Assignment> },
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expression>,
    pub from: Option<UpdateFrom>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateFrom {
    pub table: String,
    pub joins: Vec<Join>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
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
    pub as_query: Option<Box<SelectStatement>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDefinition {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub unique: bool,
    pub default_value: Option<Value>,
    pub foreign_key: Option<ForeignKeyConstraint>,
    #[serde(default)]
    pub check: Option<String>,
    #[serde(default)]
    pub auto_increment: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForeignKeyConstraint {
    pub referenced_table: String,
    pub referenced_column: String,
    pub on_delete: ForeignKeyAction,
    pub on_update: ForeignKeyAction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ForeignKeyAction {
    Restrict,
    Cascade,
    SetNull,
    NoAction,
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
pub struct CreateIndexStatement {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DropIndexStatement {
    pub name: String,
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
    Case {
        operand: Option<Box<Expression>>,
        when_clauses: Vec<(Expression, Expression)>,
        else_clause: Option<Box<Expression>>,
    },
    ScalarFunction {
        name: ScalarFunctionType,
        args: Vec<Expression>,
    },
    WindowFunction {
        function: WindowFunctionType,
        args: Vec<Expression>,
        partition_by: Vec<Expression>,
        order_by: Vec<OrderByExpr>,
        frame: Option<WindowFrame>,
    },
    Cast {
        expr: Box<Expression>,
        data_type: DataType,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarFunctionType {
    Upper,
    Lower,
    Length,
    Substring,
    Abs,
    Round,
    Coalesce,
    Trim,
    Replace,
    ConcatFn,
    Position,
    Instr,
    Ceil,
    Floor,
    Sqrt,
    Power,
    Mod,
    Now,
    Year,
    Month,
    Day,
    DateAdd,
    Datediff,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFunctionType {
    RowNumber,
    Rank,
    DenseRank,
    Aggregate(AggregateFunctionType),
    Lag,
    Lead,
    Ntile,
    FirstValue,
    LastValue,
    NthValue,
    PercentRank,
    CumeDist,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WindowFrame {
    pub mode: WindowFrameMode,
    pub start: WindowFrameBound,
    pub end: WindowFrameBound,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameMode {
    Rows,
    Range,
}

#[derive(Debug, Clone, PartialEq)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(usize),
    CurrentRow,
    Following(usize),
    UnboundedFollowing,
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
    Concat,
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
    Date(String),
    Time(String),
    DateTime(String),
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
