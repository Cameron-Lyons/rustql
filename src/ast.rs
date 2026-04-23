use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    ExplainAnalyze(SelectStatement),
    Describe(String),
    ShowTables,
    Analyze(String),
    Merge(MergeStatement),
    Do { statements: Vec<Statement> },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SetOperation {
    Union,
    UnionAll,
    Intersect,
    IntersectAll,
    Except,
    ExceptAll,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SelectStatement {
    pub ctes: Vec<Cte>,
    pub distinct: bool,
    pub distinct_on: Option<Vec<Expression>>,
    pub columns: Vec<Column>,
    pub from: String,
    pub from_alias: Option<String>,
    pub from_subquery: Option<(Box<SelectStatement>, String)>,
    pub from_function: Option<TableFunction>,
    pub joins: Vec<Join>,
    pub where_clause: Option<Expression>,
    pub group_by: Option<GroupByClause>,
    pub having: Option<Expression>,
    pub order_by: Option<Vec<OrderByExpr>>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub fetch: Option<FetchClause>,
    pub set_op: Option<(SetOperation, Box<SelectStatement>)>,
    pub window_definitions: Vec<WindowDefinition>,
    pub from_values: Option<(Vec<Vec<Expression>>, String, Vec<String>)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FetchClause {
    pub count: usize,
    pub with_ties: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GroupByClause {
    Simple(Vec<Expression>),
    GroupingSets(Vec<Vec<Expression>>),
    Rollup(Vec<Expression>),
    Cube(Vec<Expression>),
}

impl GroupByClause {
    pub fn exprs(&self) -> &[Expression] {
        match self {
            GroupByClause::Simple(exprs) => exprs,
            GroupByClause::Rollup(exprs) => exprs,
            GroupByClause::Cube(exprs) => exprs,
            GroupByClause::GroupingSets(sets) => {
                if let Some(first) = sets.first() {
                    first
                } else {
                    &[]
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableFunction {
    pub name: String,
    pub args: Vec<Expression>,
    pub alias: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cte {
    pub name: String,
    pub query: SelectStatement,
    pub recursive: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Join {
    pub join_type: JoinType,
    pub table: String,
    pub table_alias: Option<String>,
    pub on: Option<Expression>,
    pub using_columns: Option<Vec<String>>,
    pub lateral: bool,
    pub subquery: Option<(Box<SelectStatement>, String)>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Natural,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateFunction {
    pub function: AggregateFunctionType,
    pub expr: Box<Expression>,
    pub distinct: bool,
    pub alias: Option<String>,
    pub separator: Option<String>,
    pub percentile: Option<f64>,
    pub filter: Option<Box<Expression>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InsertStatement {
    pub table: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Value>>,
    pub source_query: Option<Box<SelectStatement>>,
    pub on_conflict: Option<OnConflictClause>,
    pub returning: Option<Vec<Column>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OnConflictClause {
    pub columns: Vec<String>,
    pub action: OnConflictAction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OnConflictAction {
    DoNothing,
    DoUpdate { assignments: Vec<Assignment> },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateStatement {
    pub table: String,
    pub assignments: Vec<Assignment>,
    pub where_clause: Option<Expression>,
    pub from: Option<UpdateFrom>,
    pub returning: Option<Vec<Column>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UpdateFrom {
    pub table: String,
    pub joins: Vec<Join>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub column: String,
    pub value: Expression,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteStatement {
    pub table: String,
    pub where_clause: Option<Expression>,
    pub using: Option<DeleteUsing>,
    pub returning: Option<Vec<Column>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DeleteUsing {
    pub table: String,
    pub alias: Option<String>,
    pub joins: Vec<Join>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TableConstraint {
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateTableStatement {
    pub name: String,
    pub columns: Vec<ColumnDefinition>,
    pub constraints: Vec<TableConstraint>,
    pub as_query: Option<Box<SelectStatement>>,
    pub if_not_exists: bool,
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
    #[serde(default)]
    pub generated: Option<GeneratedColumn>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GeneratedColumn {
    pub expr_sql: String,
    pub always: bool,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    Integer,
    Float,
    Text,
    Boolean,
    Date,
    Time,
    DateTime,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropTableStatement {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlterTableStatement {
    pub table: String,
    pub operation: AlterOperation,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AlterOperation {
    AddColumn(ColumnDefinition),
    DropColumn(String),
    RenameColumn { old: String, new: String },
    RenameTable(String),
    AddConstraint(TableConstraint),
    DropConstraint(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateIndexStatement {
    pub name: String,
    pub table: String,
    pub columns: Vec<String>,
    pub if_not_exists: bool,
    pub where_clause: Option<Expression>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DropIndexStatement {
    pub name: String,
    pub if_exists: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowDefinition {
    pub name: String,
    pub partition_by: Vec<Expression>,
    pub order_by: Vec<OrderByExpr>,
    pub frame: Option<WindowFrame>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MergeStatement {
    pub target_table: String,
    pub source: MergeSource,
    pub on_condition: Expression,
    pub when_clauses: Vec<MergeWhenClause>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MergeSource {
    Table {
        name: String,
        alias: Option<String>,
    },
    Subquery {
        query: Box<SelectStatement>,
        alias: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MergeWhenClause {
    Matched {
        condition: Option<Expression>,
        action: MergeMatchedAction,
    },
    NotMatched {
        condition: Option<Expression>,
        action: MergeNotMatchedAction,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MergeMatchedAction {
    Update { assignments: Vec<Assignment> },
    Delete,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum MergeNotMatchedAction {
    Insert {
        columns: Option<Vec<String>>,
        values: Vec<Expression>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    Any {
        left: Box<Expression>,
        op: BinaryOperator,
        subquery: Box<SelectStatement>,
    },
    All {
        left: Box<Expression>,
        op: BinaryOperator,
        subquery: Box<SelectStatement>,
    },
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
    IsDistinctFrom {
        left: Box<Expression>,
        right: Box<Expression>,
        not: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    Nullif,
    Greatest,
    Least,
    Lpad,
    Rpad,
    LeftFn,
    RightFn,
    Reverse,
    Repeat,
    Log,
    Exp,
    Sign,
    DateTrunc,
    Extract,
    Ltrim,
    Rtrim,
    Ascii,
    Chr,
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Atan2,
    Random,
    Degrees,
    Radians,
    Quarter,
    Week,
    DayOfWeek,
    Pi,
    Trunc,
    Log10,
    Log2,
    Cbrt,
    Gcd,
    Lcm,
    Initcap,
    SplitPart,
    Translate,
    RegexpMatch,
    RegexpReplace,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WindowFrame {
    pub mode: WindowFrameMode,
    pub start: WindowFrameBound,
    pub end: WindowFrameBound,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WindowFrameMode {
    Rows,
    Range,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum WindowFrameBound {
    UnboundedPreceding,
    Preceding(usize),
    CurrentRow,
    Following(usize),
    UnboundedFollowing,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    ILike,
    Between,
    In,
    Concat,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum UnaryOperator {
    Not,
    Minus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Boolean(bool),
    Date(String),
    Time(String),
    DateTime(String),
}

impl Value {
    fn sort_rank(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Integer(_) => 1,
            Value::Float(_) => 2,
            Value::Text(_) => 3,
            Value::Boolean(_) => 4,
            Value::Date(_) => 5,
            Value::Time(_) => 6,
            Value::DateTime(_) => 7,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => f.write_str("NULL"),
            Value::Integer(value) => write!(f, "{}", value),
            Value::Float(value) => write!(f, "{}", value),
            Value::Text(value) => f.write_str(value),
            Value::Boolean(value) => write!(f, "{}", value),
            Value::Date(value) => f.write_str(value),
            Value::Time(value) => f.write_str(value),
            Value::DateTime(value) => f.write_str(value),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderByExpr {
    pub expr: Expression,
    pub asc: bool,
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => compare_floats(*a, *b),
            (Value::Text(a), Value::Text(b)) => a.cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Date(a), Value::Date(b)) => a.cmp(b),
            (Value::Time(a), Value::Time(b)) => a.cmp(b),
            (Value::DateTime(a), Value::DateTime(b)) => a.cmp(b),
            _ => self.sort_rank().cmp(&other.sort_rank()),
        }
    }
}

fn compare_floats(left: f64, right: f64) -> Ordering {
    if left == right || (left.is_nan() && right.is_nan()) {
        return Ordering::Equal;
    }

    match (left.is_nan(), right.is_nan()) {
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        _ if left < right => Ordering::Less,
        _ => Ordering::Greater,
    }
}
