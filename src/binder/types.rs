use super::*;

impl BoundType {
    fn known(self) -> Option<DataType> {
        match self {
            BoundType::Known(data_type) => Some(data_type),
            BoundType::Unknown => None,
        }
    }
}

pub(super) fn column_ref_type(reference: &BoundColumnRef) -> BoundType {
    if reference
        .relation
        .as_deref()
        .is_some_and(|relation| relation.starts_with("__lateral_outer_"))
        || reference.name.starts_with("__outer_col_")
    {
        BoundType::Unknown
    } else {
        BoundType::Known(reference.data_type.clone())
    }
}

pub(super) fn column_definition(
    name: String,
    data_type: DataType,
    nullable: bool,
) -> ColumnDefinition {
    ColumnDefinition {
        name,
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
}

pub(super) fn value_type(value: &Value) -> BoundType {
    match value {
        Value::Null => BoundType::Unknown,
        Value::Integer(_) => BoundType::Known(DataType::Integer),
        Value::Float(_) => BoundType::Known(DataType::Float),
        Value::Text(_) => BoundType::Known(DataType::Text),
        Value::Boolean(_) => BoundType::Known(DataType::Boolean),
        Value::Date(_) => BoundType::Known(DataType::Date),
        Value::Time(_) => BoundType::Known(DataType::Time),
        Value::DateTime(_) => BoundType::Known(DataType::DateTime),
    }
}

pub(super) fn constant_expression_type(expr: &Expression) -> Option<DataType> {
    if let Expression::Value(value) = expr
        && let BoundType::Known(data_type) = value_type(value)
    {
        return Some(data_type);
    }
    None
}

pub(super) fn syntactic_column_type(column: &Column) -> DataType {
    match column {
        Column::Expression { expr, .. } => syntactic_expr_type(expr).unwrap_or(DataType::Text),
        Column::Function(aggregate) => match aggregate.function {
            AggregateFunctionType::Count => DataType::Integer,
            AggregateFunctionType::Avg
            | AggregateFunctionType::Stddev
            | AggregateFunctionType::Variance
            | AggregateFunctionType::Median
            | AggregateFunctionType::PercentileCont
            | AggregateFunctionType::PercentileDisc => DataType::Float,
            AggregateFunctionType::BoolAnd | AggregateFunctionType::BoolOr => DataType::Boolean,
            _ => DataType::Text,
        },
        _ => DataType::Text,
    }
}

fn syntactic_expr_type(expr: &Expression) -> Option<DataType> {
    match expr {
        Expression::Value(value) => match value_type(value) {
            BoundType::Known(data_type) => Some(data_type),
            BoundType::Unknown => None,
        },
        Expression::Cast { data_type, .. } => Some(data_type.clone()),
        Expression::BinaryOp { left, op, right } => match op {
            BinaryOperator::Plus
            | BinaryOperator::Minus
            | BinaryOperator::Multiply
            | BinaryOperator::Divide => {
                let left = syntactic_expr_type(left)?;
                let right = syntactic_expr_type(right)?;
                if matches!(left, DataType::Float) || matches!(right, DataType::Float) {
                    Some(DataType::Float)
                } else if matches!(left, DataType::Integer) && matches!(right, DataType::Integer) {
                    Some(DataType::Integer)
                } else {
                    None
                }
            }
            BinaryOperator::Concat => Some(DataType::Text),
            BinaryOperator::And
            | BinaryOperator::Or
            | BinaryOperator::Equal
            | BinaryOperator::NotEqual
            | BinaryOperator::LessThan
            | BinaryOperator::LessThanOrEqual
            | BinaryOperator::GreaterThan
            | BinaryOperator::GreaterThanOrEqual
            | BinaryOperator::Like
            | BinaryOperator::ILike
            | BinaryOperator::Between
            | BinaryOperator::In => Some(DataType::Boolean),
        },
        Expression::UnaryOp { op, expr } => match op {
            UnaryOperator::Minus => syntactic_expr_type(expr),
            UnaryOperator::Not => Some(DataType::Boolean),
        },
        Expression::IsNull { .. }
        | Expression::Exists(_)
        | Expression::Any { .. }
        | Expression::All { .. }
        | Expression::IsDistinctFrom { .. }
        | Expression::In { .. } => Some(DataType::Boolean),
        Expression::ScalarFunction { name, .. } => scalar_function_type(name, &[])
            .known()
            .or(Some(DataType::Text)),
        _ => None,
    }
}

pub(super) fn bound_type_or_text(data_type: &BoundType) -> DataType {
    match data_type {
        BoundType::Known(data_type) => data_type.clone(),
        BoundType::Unknown => DataType::Text,
    }
}

pub(super) fn aggregate_type(function: &AggregateFunctionType, expr: &BoundExpr) -> BoundType {
    match function {
        AggregateFunctionType::Count => BoundType::Known(DataType::Integer),
        AggregateFunctionType::Sum => match expr.data_type {
            BoundType::Known(DataType::Integer) => BoundType::Known(DataType::Integer),
            BoundType::Known(DataType::Float) => BoundType::Known(DataType::Float),
            _ => BoundType::Unknown,
        },
        AggregateFunctionType::Avg
        | AggregateFunctionType::Stddev
        | AggregateFunctionType::Variance
        | AggregateFunctionType::Median
        | AggregateFunctionType::PercentileCont
        | AggregateFunctionType::PercentileDisc => BoundType::Known(DataType::Float),
        AggregateFunctionType::Min | AggregateFunctionType::Max | AggregateFunctionType::Mode => {
            expr.data_type.clone()
        }
        AggregateFunctionType::GroupConcat => BoundType::Known(DataType::Text),
        AggregateFunctionType::BoolAnd | AggregateFunctionType::BoolOr => {
            BoundType::Known(DataType::Boolean)
        }
    }
}

pub(super) fn scalar_function_type(name: &ScalarFunctionType, args: &[BoundExpr]) -> BoundType {
    match name {
        ScalarFunctionType::Upper
        | ScalarFunctionType::Lower
        | ScalarFunctionType::Substring
        | ScalarFunctionType::Trim
        | ScalarFunctionType::Replace
        | ScalarFunctionType::ConcatFn
        | ScalarFunctionType::Lpad
        | ScalarFunctionType::Rpad
        | ScalarFunctionType::LeftFn
        | ScalarFunctionType::RightFn
        | ScalarFunctionType::Reverse
        | ScalarFunctionType::Repeat
        | ScalarFunctionType::DateTrunc
        | ScalarFunctionType::Ltrim
        | ScalarFunctionType::Rtrim
        | ScalarFunctionType::Chr
        | ScalarFunctionType::Initcap
        | ScalarFunctionType::SplitPart
        | ScalarFunctionType::Translate
        | ScalarFunctionType::RegexpReplace => BoundType::Known(DataType::Text),
        ScalarFunctionType::Length
        | ScalarFunctionType::Position
        | ScalarFunctionType::Instr
        | ScalarFunctionType::Year
        | ScalarFunctionType::Month
        | ScalarFunctionType::Day
        | ScalarFunctionType::Datediff
        | ScalarFunctionType::Ascii
        | ScalarFunctionType::Week
        | ScalarFunctionType::DayOfWeek
        | ScalarFunctionType::Quarter
        | ScalarFunctionType::Gcd
        | ScalarFunctionType::Lcm => BoundType::Known(DataType::Integer),
        ScalarFunctionType::Now => BoundType::Known(DataType::DateTime),
        ScalarFunctionType::Random
        | ScalarFunctionType::Pi
        | ScalarFunctionType::Log
        | ScalarFunctionType::Exp
        | ScalarFunctionType::Sin
        | ScalarFunctionType::Cos
        | ScalarFunctionType::Tan
        | ScalarFunctionType::Asin
        | ScalarFunctionType::Acos
        | ScalarFunctionType::Atan
        | ScalarFunctionType::Atan2
        | ScalarFunctionType::Degrees
        | ScalarFunctionType::Radians
        | ScalarFunctionType::Log10
        | ScalarFunctionType::Log2
        | ScalarFunctionType::Cbrt
        | ScalarFunctionType::Sqrt
        | ScalarFunctionType::Power => BoundType::Known(DataType::Float),
        ScalarFunctionType::Coalesce
        | ScalarFunctionType::Nullif
        | ScalarFunctionType::Greatest
        | ScalarFunctionType::Least => args
            .iter()
            .map(|arg| arg.data_type.clone())
            .find(|data_type| !matches!(data_type, BoundType::Unknown))
            .unwrap_or(BoundType::Unknown),
        ScalarFunctionType::Abs
        | ScalarFunctionType::Round
        | ScalarFunctionType::Ceil
        | ScalarFunctionType::Floor
        | ScalarFunctionType::Mod
        | ScalarFunctionType::Sign
        | ScalarFunctionType::Trunc => args
            .first()
            .map(|arg| arg.data_type.clone())
            .unwrap_or(BoundType::Unknown),
        ScalarFunctionType::DateAdd => BoundType::Known(DataType::Date),
        ScalarFunctionType::Extract => BoundType::Known(DataType::Integer),
        ScalarFunctionType::RegexpMatch => BoundType::Known(DataType::Boolean),
    }
}

pub(super) fn window_function_type(
    function: &WindowFunctionType,
    args: &[BoundExpr],
) -> BoundType {
    match function {
        WindowFunctionType::RowNumber
        | WindowFunctionType::Rank
        | WindowFunctionType::DenseRank
        | WindowFunctionType::Ntile => BoundType::Known(DataType::Integer),
        WindowFunctionType::PercentRank | WindowFunctionType::CumeDist => {
            BoundType::Known(DataType::Float)
        }
        WindowFunctionType::Aggregate(function) => args
            .first()
            .map(|arg| aggregate_type(function, arg))
            .unwrap_or(BoundType::Unknown),
        WindowFunctionType::Lag
        | WindowFunctionType::Lead
        | WindowFunctionType::FirstValue
        | WindowFunctionType::LastValue
        | WindowFunctionType::NthValue => args
            .first()
            .map(|arg| arg.data_type.clone())
            .unwrap_or(BoundType::Unknown),
    }
}

pub(super) fn ensure_boolean(expr: &BoundExpr, context: &str) -> Result<(), RustqlError> {
    if matches!(
        expr.data_type,
        BoundType::Known(DataType::Boolean) | BoundType::Unknown
    ) {
        Ok(())
    } else {
        Err(RustqlError::TypeMismatch(format!(
            "{} must evaluate to BOOLEAN",
            context
        )))
    }
}

pub(super) fn ensure_numeric(expr: &BoundExpr, context: &str) -> Result<(), RustqlError> {
    match expr.data_type {
        BoundType::Known(DataType::Integer | DataType::Float) | BoundType::Unknown => Ok(()),
        _ => Err(RustqlError::TypeMismatch(format!(
            "{} requires numeric values",
            context
        ))),
    }
}

pub(super) fn ensure_text(expr: &BoundExpr, context: &str) -> Result<(), RustqlError> {
    match expr.data_type {
        BoundType::Known(DataType::Text) | BoundType::Unknown => Ok(()),
        _ => Err(RustqlError::TypeMismatch(format!(
            "{} requires text values",
            context
        ))),
    }
}

pub(super) fn ensure_comparable(
    left: &BoundType,
    right: &BoundType,
    context: &str,
) -> Result<(), RustqlError> {
    if types_comparable(left, right) {
        Ok(())
    } else {
        Err(RustqlError::TypeMismatch(format!(
            "{} operands have incompatible types: {} and {}",
            context,
            display_bound_type(left),
            display_bound_type(right)
        )))
    }
}

fn types_comparable(left: &BoundType, right: &BoundType) -> bool {
    match (left, right) {
        (BoundType::Unknown, _) | (_, BoundType::Unknown) => true,
        (BoundType::Known(left), BoundType::Known(right)) if left == right => true,
        (BoundType::Known(left), BoundType::Known(right)) => {
            is_numeric_type(left) && is_numeric_type(right)
        }
    }
}

pub(super) fn common_numeric_type(left: &BoundType, right: &BoundType) -> BoundType {
    match (left, right) {
        (BoundType::Known(DataType::Float), _) | (_, BoundType::Known(DataType::Float)) => {
            BoundType::Known(DataType::Float)
        }
        (BoundType::Known(DataType::Integer), BoundType::Known(DataType::Integer)) => {
            BoundType::Known(DataType::Integer)
        }
        _ => BoundType::Unknown,
    }
}

pub(super) fn common_type(left: &BoundType, right: &BoundType) -> BoundType {
    match (left, right) {
        (BoundType::Unknown, data_type) | (data_type, BoundType::Unknown) => data_type.clone(),
        (BoundType::Known(left), BoundType::Known(right)) if left == right => {
            BoundType::Known(left.clone())
        }
        _ if types_comparable(left, right) => common_numeric_type(left, right),
        _ => BoundType::Unknown,
    }
}

fn is_numeric_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Integer | DataType::Float)
}

fn display_bound_type(data_type: &BoundType) -> &'static str {
    match data_type {
        BoundType::Unknown => "UNKNOWN",
        BoundType::Known(DataType::Integer) => "INTEGER",
        BoundType::Known(DataType::Float) => "FLOAT",
        BoundType::Known(DataType::Text) => "TEXT",
        BoundType::Known(DataType::Boolean) => "BOOLEAN",
        BoundType::Known(DataType::Date) => "DATE",
        BoundType::Known(DataType::Time) => "TIME",
        BoundType::Known(DataType::DateTime) => "DATETIME",
    }
}
