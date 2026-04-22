use super::*;

impl Parser {
    pub(super) fn parse_arithmetic_expression(&mut self) -> Result<Expression, RustqlError> {
        self.parse_arithmetic_term()
    }

    pub(super) fn parse_arithmetic_term(&mut self) -> Result<Expression, RustqlError> {
        let mut expr = self.parse_arithmetic_factor()?;

        loop {
            let op = match self.current_token() {
                Token::Plus => BinaryOperator::Plus,
                Token::Minus => BinaryOperator::Minus,
                Token::Concat => BinaryOperator::Concat,
                _ => break,
            };

            self.advance();
            let right = self.parse_arithmetic_factor()?;
            expr = Expression::BinaryOp {
                left: Box::new(expr),
                op,
                right: Box::new(right),
            };
        }

        Ok(expr)
    }

    pub(super) fn parse_arithmetic_factor(&mut self) -> Result<Expression, RustqlError> {
        let mut expr = self.parse_arithmetic_unary()?;

        loop {
            let op = match self.current_token() {
                Token::Star => {
                    self.advance();
                    BinaryOperator::Multiply
                }
                Token::Divide => {
                    self.advance();
                    BinaryOperator::Divide
                }
                _ => break,
            };

            let right = self.parse_arithmetic_unary()?;
            expr = Expression::BinaryOp {
                left: Box::new(expr),
                op,
                right: Box::new(right),
            };
        }

        Ok(expr)
    }

    pub(super) fn parse_arithmetic_unary(&mut self) -> Result<Expression, RustqlError> {
        match self.current_token() {
            Token::Minus => {
                self.advance();
                Ok(Expression::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(self.parse_arithmetic_unary()?),
                })
            }
            Token::Plus => {
                self.advance();
                self.parse_arithmetic_unary()
            }
            _ => self.parse_arithmetic_primary(),
        }
    }

    pub(super) fn parse_arithmetic_primary(&mut self) -> Result<Expression, RustqlError> {
        let expr = match self.current_token() {
            Token::LeftParen => {
                self.advance();
                let expr = self.parse_arithmetic_expression()?;
                self.consume(Token::RightParen)?;
                expr
            }
            Token::Case => self.parse_case_expression()?,
            Token::Cast => self.parse_cast_expression()?,
            Token::Upper
            | Token::Lower
            | Token::Length
            | Token::Substring
            | Token::Abs
            | Token::Round
            | Token::Coalesce
            | Token::Trim
            | Token::Replace
            | Token::Position
            | Token::Instr
            | Token::Ceil
            | Token::Ceiling
            | Token::Floor
            | Token::Sqrt
            | Token::Power
            | Token::Mod
            | Token::Now
            | Token::Year
            | Token::Month
            | Token::Day
            | Token::DateAdd
            | Token::Datediff
            | Token::ConcatFn
            | Token::Ltrim
            | Token::Rtrim
            | Token::Ascii
            | Token::Chr
            | Token::Sin
            | Token::Cos
            | Token::Tan
            | Token::Asin
            | Token::Acos
            | Token::Atan
            | Token::Atan2
            | Token::Random
            | Token::Degrees
            | Token::Radians
            | Token::Quarter
            | Token::Week
            | Token::DayOfWeek
            | Token::Pi
            | Token::Trunc
            | Token::Log10
            | Token::Log2
            | Token::Cbrt
            | Token::Gcd
            | Token::Lcm
            | Token::Initcap
            | Token::SplitPart
            | Token::Translate
            | Token::RegexpMatch
            | Token::RegexpReplace => self.parse_scalar_function()?,
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();
                Expression::Column(name)
            }
            Token::Number(n) => {
                let n = *n;
                self.advance();
                Expression::Value(Value::Integer(n))
            }
            Token::Float(n) => {
                let n = *n;
                self.advance();
                Expression::Value(Value::Float(n))
            }
            Token::StringLiteral(s) => {
                let s = s.clone();
                self.advance();
                Expression::Value(Value::Text(s))
            }
            Token::Null => {
                self.advance();
                Expression::Value(Value::Null)
            }
            Token::GenerateSeries
            | Token::Filter
            | Token::Lateral
            | Token::Grouping
            | Token::Sets
            | Token::Cube
            | Token::Rollup
            | Token::Fetch
            | Token::First
            | Token::Next
            | Token::Only
            | Token::Ties
            | Token::Row
            | Token::Window
            | Token::Merge
            | Token::Matched
            | Token::Generated
            | Token::Always
            | Token::Stored => {
                let name = token_to_string(self.current_token()).to_lowercase();
                self.advance();
                Expression::Column(name)
            }
            _ => {
                return Err(RustqlError::ParseError(format!(
                    "Unexpected token in expression: {:?}",
                    self.current_token()
                )));
            }
        };
        if *self.current_token() == Token::DoubleColon {
            self.advance();
            let data_type = self.parse_data_type()?;
            return Ok(Expression::Cast {
                expr: Box::new(expr),
                data_type,
            });
        }
        Ok(expr)
    }

    pub(super) fn parse_case_expression(&mut self) -> Result<Expression, RustqlError> {
        self.consume(Token::Case)?;
        let operand = if *self.current_token() != Token::When {
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };
        let mut when_clauses = Vec::new();
        while *self.current_token() == Token::When {
            self.advance();
            let condition = self.parse_expression()?;
            self.consume(Token::Then)?;
            let result = self.parse_expression()?;
            when_clauses.push((condition, result));
        }
        let else_clause = if *self.current_token() == Token::Else {
            self.advance();
            Some(Box::new(self.parse_expression()?))
        } else {
            None
        };
        self.consume(Token::End)?;
        Ok(Expression::Case {
            operand,
            when_clauses,
            else_clause,
        })
    }

    pub(super) fn parse_cast_expression(&mut self) -> Result<Expression, RustqlError> {
        self.consume(Token::Cast)?;
        self.consume(Token::LeftParen)?;
        let expr = self.parse_expression()?;
        self.consume(Token::As)?;
        let data_type = self.parse_data_type()?;
        self.consume(Token::RightParen)?;
        Ok(Expression::Cast {
            expr: Box::new(expr),
            data_type,
        })
    }

    pub(super) fn parse_scalar_function(&mut self) -> Result<Expression, RustqlError> {
        let func_type = match self.advance() {
            Token::Upper => ScalarFunctionType::Upper,
            Token::Lower => ScalarFunctionType::Lower,
            Token::Length => ScalarFunctionType::Length,
            Token::Substring => ScalarFunctionType::Substring,
            Token::Abs => ScalarFunctionType::Abs,
            Token::Round => ScalarFunctionType::Round,
            Token::Coalesce => ScalarFunctionType::Coalesce,
            Token::Trim => ScalarFunctionType::Trim,
            Token::Replace => ScalarFunctionType::Replace,
            Token::Position => ScalarFunctionType::Position,
            Token::Instr => ScalarFunctionType::Instr,
            Token::Ceil | Token::Ceiling => ScalarFunctionType::Ceil,
            Token::Floor => ScalarFunctionType::Floor,
            Token::Sqrt => ScalarFunctionType::Sqrt,
            Token::Power => ScalarFunctionType::Power,
            Token::Mod => ScalarFunctionType::Mod,
            Token::Now => ScalarFunctionType::Now,
            Token::Year => ScalarFunctionType::Year,
            Token::Month => ScalarFunctionType::Month,
            Token::Day => ScalarFunctionType::Day,
            Token::DateAdd => ScalarFunctionType::DateAdd,
            Token::Datediff => ScalarFunctionType::Datediff,
            Token::ConcatFn => ScalarFunctionType::ConcatFn,
            Token::Nullif => ScalarFunctionType::Nullif,
            Token::Greatest => ScalarFunctionType::Greatest,
            Token::Least => ScalarFunctionType::Least,
            Token::Lpad => ScalarFunctionType::Lpad,
            Token::Rpad => ScalarFunctionType::Rpad,
            Token::Left => ScalarFunctionType::LeftFn,
            Token::Right => ScalarFunctionType::RightFn,
            Token::Reverse => ScalarFunctionType::Reverse,
            Token::Repeat => ScalarFunctionType::Repeat,
            Token::Log => ScalarFunctionType::Log,
            Token::Exp => ScalarFunctionType::Exp,
            Token::Sign => ScalarFunctionType::Sign,
            Token::DateTrunc => ScalarFunctionType::DateTrunc,
            Token::Extract => ScalarFunctionType::Extract,
            Token::Ltrim => ScalarFunctionType::Ltrim,
            Token::Rtrim => ScalarFunctionType::Rtrim,
            Token::Ascii => ScalarFunctionType::Ascii,
            Token::Chr => ScalarFunctionType::Chr,
            Token::Sin => ScalarFunctionType::Sin,
            Token::Cos => ScalarFunctionType::Cos,
            Token::Tan => ScalarFunctionType::Tan,
            Token::Asin => ScalarFunctionType::Asin,
            Token::Acos => ScalarFunctionType::Acos,
            Token::Atan => ScalarFunctionType::Atan,
            Token::Atan2 => ScalarFunctionType::Atan2,
            Token::Random => ScalarFunctionType::Random,
            Token::Degrees => ScalarFunctionType::Degrees,
            Token::Radians => ScalarFunctionType::Radians,
            Token::Quarter => ScalarFunctionType::Quarter,
            Token::Week => ScalarFunctionType::Week,
            Token::DayOfWeek => ScalarFunctionType::DayOfWeek,
            Token::Pi => ScalarFunctionType::Pi,
            Token::Trunc => ScalarFunctionType::Trunc,
            Token::Log10 => ScalarFunctionType::Log10,
            Token::Log2 => ScalarFunctionType::Log2,
            Token::Cbrt => ScalarFunctionType::Cbrt,
            Token::Gcd => ScalarFunctionType::Gcd,
            Token::Lcm => ScalarFunctionType::Lcm,
            Token::Initcap => ScalarFunctionType::Initcap,
            Token::SplitPart => ScalarFunctionType::SplitPart,
            Token::Translate => ScalarFunctionType::Translate,
            Token::RegexpMatch => ScalarFunctionType::RegexpMatch,
            Token::RegexpReplace => ScalarFunctionType::RegexpReplace,
            _ => {
                return Err(RustqlError::ParseError(
                    "Expected scalar function name".to_string(),
                ));
            }
        };
        if func_type == ScalarFunctionType::Extract {
            self.consume(Token::LeftParen)?;
            let part_name = match self.current_token() {
                Token::Identifier(s) => {
                    let s = s.to_uppercase();
                    self.advance();
                    s
                }
                Token::Year => {
                    self.advance();
                    "YEAR".to_string()
                }
                Token::Month => {
                    self.advance();
                    "MONTH".to_string()
                }
                Token::Day => {
                    self.advance();
                    "DAY".to_string()
                }
                Token::Quarter => {
                    self.advance();
                    "QUARTER".to_string()
                }
                Token::Week => {
                    self.advance();
                    "WEEK".to_string()
                }
                Token::DayOfWeek => {
                    self.advance();
                    "DOW".to_string()
                }
                _ => {
                    return Err(RustqlError::ParseError(
                        "Expected date part (YEAR, MONTH, DAY, HOUR, MINUTE, SECOND, QUARTER, WEEK, DOW) after EXTRACT("
                            .to_string(),
                    ));
                }
            };
            self.consume(Token::From)?;
            let inner_expr = self.parse_expression()?;
            self.consume(Token::RightParen)?;
            return Ok(Expression::ScalarFunction {
                name: ScalarFunctionType::Extract,
                args: vec![Expression::Value(Value::Text(part_name)), inner_expr],
            });
        }
        self.consume(Token::LeftParen)?;
        let mut args = Vec::new();
        if *self.current_token() != Token::RightParen {
            loop {
                args.push(self.parse_expression()?);
                if *self.current_token() == Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
        }
        self.consume(Token::RightParen)?;
        Ok(Expression::ScalarFunction {
            name: func_type,
            args,
        })
    }

    pub(super) fn parse_aggregate_function(&mut self) -> Result<Column, RustqlError> {
        let func_type = match self.advance() {
            Token::Count => AggregateFunctionType::Count,
            Token::Sum => AggregateFunctionType::Sum,
            Token::Avg => AggregateFunctionType::Avg,
            Token::Min => AggregateFunctionType::Min,
            Token::Max => AggregateFunctionType::Max,
            Token::Stddev => AggregateFunctionType::Stddev,
            Token::Variance => AggregateFunctionType::Variance,
            Token::GroupConcat | Token::StringAgg => AggregateFunctionType::GroupConcat,
            Token::BoolAnd | Token::Every => AggregateFunctionType::BoolAnd,
            Token::BoolOr => AggregateFunctionType::BoolOr,
            Token::Median => AggregateFunctionType::Median,
            Token::Mode => AggregateFunctionType::Mode,
            Token::PercentileCont => AggregateFunctionType::PercentileCont,
            Token::PercentileDisc => AggregateFunctionType::PercentileDisc,
            _ => {
                return Err(RustqlError::ParseError(
                    "Expected aggregate function".to_string(),
                ));
            }
        };

        self.consume(Token::LeftParen)?;

        let distinct = if *self.current_token() == Token::Distinct {
            self.advance();
            true
        } else {
            false
        };

        let mut separator = None;
        let mut percentile = None;

        let expr = match func_type {
            AggregateFunctionType::PercentileCont | AggregateFunctionType::PercentileDisc => {
                let frac_expr = self.parse_expression()?;
                let frac = match &frac_expr {
                    Expression::Value(Value::Float(f)) => *f,
                    Expression::Value(Value::Integer(n)) => *n as f64,
                    _ => {
                        return Err(RustqlError::ParseError(
                            "PERCENTILE_CONT/DISC requires a numeric fraction".to_string(),
                        ));
                    }
                };
                percentile = Some(frac);
                if *self.current_token() == Token::Comma {
                    self.advance();
                    Box::new(self.parse_expression()?)
                } else {
                    self.consume(Token::RightParen)?;
                    if *self.current_token() == Token::Within {
                        self.advance();
                        if let Token::Identifier(ref s) = *self.current_token() {
                            if s.to_uppercase() == "GROUP" {
                                self.advance();
                            }
                        } else if *self.current_token() == Token::Group {
                            self.advance();
                        }
                        self.consume(Token::LeftParen)?;
                        self.consume(Token::Order)?;
                        self.consume(Token::By)?;
                        let col_expr = self.parse_expression()?;
                        if *self.current_token() == Token::Asc
                            || *self.current_token() == Token::Desc
                        {
                            self.advance();
                        }
                        self.consume(Token::RightParen)?;
                        let alias = if *self.current_token() == Token::As {
                            self.advance();
                            match self.advance() {
                                Token::Identifier(name) => Some(name),
                                _ => {
                                    return Err(RustqlError::ParseError(
                                        "Expected alias after AS".to_string(),
                                    ));
                                }
                            }
                        } else {
                            None
                        };
                        return Ok(Column::Function(AggregateFunction {
                            function: func_type,
                            expr: Box::new(col_expr),
                            distinct,
                            alias,
                            separator: None,
                            percentile,
                            filter: None,
                        }));
                    } else {
                        return Err(RustqlError::ParseError(
                            "PERCENTILE_CONT/DISC requires WITHIN GROUP (ORDER BY col) or (frac, col) syntax".to_string(),
                        ));
                    }
                }
            }
            _ => {
                if *self.current_token() == Token::Star {
                    self.advance();
                    Box::new(Expression::Column("*".to_string()))
                } else {
                    Box::new(self.parse_expression()?)
                }
            }
        };

        if distinct && matches!(&*expr, Expression::Column(name) if name == "*") {
            return Err(RustqlError::ParseError(
                "DISTINCT * is not supported".to_string(),
            ));
        }

        if matches!(func_type, AggregateFunctionType::GroupConcat) {
            if *self.current_token() == Token::Comma {
                self.advance();
                if let Token::StringLiteral(s) = self.current_token() {
                    separator = Some(s.clone());
                    self.advance();
                }
            }
            if *self.current_token() == Token::Separator {
                self.advance();
                match self.current_token() {
                    Token::StringLiteral(s) => {
                        separator = Some(s.clone());
                        self.advance();
                    }
                    _ => {
                        return Err(RustqlError::ParseError(
                            "Expected string literal after SEPARATOR".to_string(),
                        ));
                    }
                }
            }
        }

        self.consume(Token::RightParen)?;

        let filter = if *self.current_token() == Token::Filter {
            self.advance();
            self.consume(Token::LeftParen)?;
            self.consume(Token::Where)?;
            let filter_expr = self.parse_expression()?;
            self.consume(Token::RightParen)?;
            Some(Box::new(filter_expr))
        } else {
            None
        };

        if *self.current_token() == Token::Over {
            let agg_type = func_type;
            let (partition_by, order_by, frame) = self.parse_over_clause()?;
            let alias = if *self.current_token() == Token::As {
                self.advance();
                match self.advance() {
                    Token::Identifier(name) => Some(name),
                    _ => {
                        return Err(RustqlError::ParseError(
                            "Expected alias after AS".to_string(),
                        ));
                    }
                }
            } else {
                None
            };
            return Ok(Column::Expression {
                expr: Expression::WindowFunction {
                    function: WindowFunctionType::Aggregate(agg_type),
                    args: Vec::new(),
                    partition_by,
                    order_by,
                    frame,
                },
                alias,
            });
        }

        let alias = if *self.current_token() == Token::As {
            self.advance();
            match self.advance() {
                Token::Identifier(name) => Some(name),
                _ => {
                    return Err(RustqlError::ParseError(
                        "Expected alias after AS".to_string(),
                    ));
                }
            }
        } else {
            None
        };

        Ok(Column::Function(AggregateFunction {
            function: func_type,
            expr,
            distinct,
            alias,
            separator,
            percentile,
            filter,
        }))
    }

    pub(super) fn parse_expression(&mut self) -> Result<Expression, RustqlError> {
        self.parse_or()
    }

    pub(super) fn parse_or(&mut self) -> Result<Expression, RustqlError> {
        let mut expr = self.parse_and()?;

        while *self.current_token() == Token::Or {
            self.advance();
            let right = self.parse_and()?;
            expr = Expression::BinaryOp {
                left: Box::new(expr),
                op: BinaryOperator::Or,
                right: Box::new(right),
            };
        }

        Ok(expr)
    }

    pub(super) fn parse_and(&mut self) -> Result<Expression, RustqlError> {
        let mut expr = self.parse_comparison()?;

        while *self.current_token() == Token::And {
            self.advance();
            let right = self.parse_comparison()?;
            expr = Expression::BinaryOp {
                left: Box::new(expr),
                op: BinaryOperator::And,
                right: Box::new(right),
            };
        }

        Ok(expr)
    }

    pub(super) fn try_parse_any_all(
        &mut self,
        left: Expression,
        op: BinaryOperator,
    ) -> Result<Option<Expression>, RustqlError> {
        if *self.current_token() == Token::Any || *self.current_token() == Token::All {
            let is_any = *self.current_token() == Token::Any;
            self.advance();
            self.consume(Token::LeftParen)?;
            let subquery = self.parse_select_inner(Vec::new())?;
            self.consume(Token::RightParen)?;
            if is_any {
                Ok(Some(Expression::Any {
                    left: Box::new(left),
                    op,
                    subquery: Box::new(subquery),
                }))
            } else {
                Ok(Some(Expression::All {
                    left: Box::new(left),
                    op,
                    subquery: Box::new(subquery),
                }))
            }
        } else {
            Ok(None)
        }
    }

    pub(super) fn parse_comparison(&mut self) -> Result<Expression, RustqlError> {
        let mut expr = self.parse_term()?;

        loop {
            match self.current_token() {
                Token::Equal => {
                    self.advance();
                    if let Some(any_all) =
                        self.try_parse_any_all(expr.clone(), BinaryOperator::Equal)?
                    {
                        expr = any_all;
                    } else {
                        let right = self.parse_term()?;
                        expr = Expression::BinaryOp {
                            left: Box::new(expr),
                            op: BinaryOperator::Equal,
                            right: Box::new(right),
                        };
                    }
                }
                Token::NotEqual => {
                    self.advance();
                    if let Some(any_all) =
                        self.try_parse_any_all(expr.clone(), BinaryOperator::NotEqual)?
                    {
                        expr = any_all;
                    } else {
                        let right = self.parse_term()?;
                        expr = Expression::BinaryOp {
                            left: Box::new(expr),
                            op: BinaryOperator::NotEqual,
                            right: Box::new(right),
                        };
                    }
                }
                Token::LessThan => {
                    self.advance();
                    if let Some(any_all) =
                        self.try_parse_any_all(expr.clone(), BinaryOperator::LessThan)?
                    {
                        expr = any_all;
                    } else {
                        let right = self.parse_term()?;
                        expr = Expression::BinaryOp {
                            left: Box::new(expr),
                            op: BinaryOperator::LessThan,
                            right: Box::new(right),
                        };
                    }
                }
                Token::LessThanOrEqual => {
                    self.advance();
                    if let Some(any_all) =
                        self.try_parse_any_all(expr.clone(), BinaryOperator::LessThanOrEqual)?
                    {
                        expr = any_all;
                    } else {
                        let right = self.parse_term()?;
                        expr = Expression::BinaryOp {
                            left: Box::new(expr),
                            op: BinaryOperator::LessThanOrEqual,
                            right: Box::new(right),
                        };
                    }
                }
                Token::GreaterThan => {
                    self.advance();
                    if let Some(any_all) =
                        self.try_parse_any_all(expr.clone(), BinaryOperator::GreaterThan)?
                    {
                        expr = any_all;
                    } else {
                        let right = self.parse_term()?;
                        expr = Expression::BinaryOp {
                            left: Box::new(expr),
                            op: BinaryOperator::GreaterThan,
                            right: Box::new(right),
                        };
                    }
                }
                Token::GreaterThanOrEqual => {
                    self.advance();
                    if let Some(any_all) =
                        self.try_parse_any_all(expr.clone(), BinaryOperator::GreaterThanOrEqual)?
                    {
                        expr = any_all;
                    } else {
                        let right = self.parse_term()?;
                        expr = Expression::BinaryOp {
                            left: Box::new(expr),
                            op: BinaryOperator::GreaterThanOrEqual,
                            right: Box::new(right),
                        };
                    }
                }
                Token::Like => {
                    self.advance();
                    let right = self.parse_term()?;
                    expr = Expression::BinaryOp {
                        left: Box::new(expr),
                        op: BinaryOperator::Like,
                        right: Box::new(right),
                    };
                }
                Token::ILike => {
                    self.advance();
                    let right = self.parse_term()?;
                    expr = Expression::BinaryOp {
                        left: Box::new(expr),
                        op: BinaryOperator::ILike,
                        right: Box::new(right),
                    };
                }
                Token::In => {
                    self.advance();
                    if *self.current_token() != Token::LeftParen {
                        return Err(RustqlError::ParseError("Expected '(' after IN".to_string()));
                    }
                    self.advance();

                    if *self.current_token() == Token::Select {
                        let subquery_stmt = self.parse_select_inner(Vec::new())?;
                        self.consume(Token::RightParen)?;
                        expr = Expression::BinaryOp {
                            left: Box::new(expr),
                            op: BinaryOperator::In,
                            right: Box::new(Expression::Subquery(Box::new(subquery_stmt))),
                        };
                    } else {
                        let mut values = Vec::new();
                        while *self.current_token() != Token::RightParen {
                            values.push(self.parse_value()?);
                            if *self.current_token() == Token::Comma {
                                self.advance();
                            }
                        }
                        self.consume(Token::RightParen)?;

                        expr = Expression::In {
                            left: Box::new(expr),
                            values,
                        };
                    }
                }
                Token::Between => {
                    self.advance();
                    let left_bound = self.parse_term()?;
                    self.consume(Token::And)?;
                    let right_bound = self.parse_term()?;
                    expr = Expression::BinaryOp {
                        left: Box::new(expr),
                        op: BinaryOperator::Between,
                        right: Box::new(Expression::BinaryOp {
                            left: Box::new(left_bound),
                            op: BinaryOperator::And,
                            right: Box::new(right_bound),
                        }),
                    };
                }
                Token::Is => {
                    self.advance();
                    let not = if *self.current_token() == Token::Not {
                        self.advance();
                        true
                    } else {
                        false
                    };
                    if *self.current_token() == Token::Distinct {
                        self.advance();
                        self.consume(Token::From)?;
                        let right = self.parse_term()?;
                        expr = Expression::IsDistinctFrom {
                            left: Box::new(expr),
                            right: Box::new(right),
                            not,
                        };
                    } else {
                        self.consume(Token::Null)?;
                        expr = Expression::IsNull {
                            expr: Box::new(expr),
                            not,
                        };
                    }
                }
                _ => break,
            }
        }

        Ok(expr)
    }

    pub(super) fn parse_term(&mut self) -> Result<Expression, RustqlError> {
        let mut expr = self.parse_factor()?;

        loop {
            let op = match self.current_token() {
                Token::Plus => BinaryOperator::Plus,
                Token::Minus => BinaryOperator::Minus,
                Token::Concat => BinaryOperator::Concat,
                _ => break,
            };

            self.advance();
            let right = self.parse_factor()?;
            expr = Expression::BinaryOp {
                left: Box::new(expr),
                op,
                right: Box::new(right),
            };
        }

        Ok(expr)
    }

    pub(super) fn parse_factor(&mut self) -> Result<Expression, RustqlError> {
        let mut expr = self.parse_unary()?;

        loop {
            let op = match self.current_token() {
                Token::Star => BinaryOperator::Multiply,
                Token::Divide => BinaryOperator::Divide,
                _ => break,
            };

            self.advance();
            let right = self.parse_unary()?;
            expr = Expression::BinaryOp {
                left: Box::new(expr),
                op,
                right: Box::new(right),
            };
        }

        Ok(expr)
    }

    pub(super) fn parse_unary(&mut self) -> Result<Expression, RustqlError> {
        match self.current_token() {
            Token::Not => {
                self.advance();
                Ok(Expression::UnaryOp {
                    op: UnaryOperator::Not,
                    expr: Box::new(self.parse_unary()?),
                })
            }
            Token::Minus => {
                self.advance();
                Ok(Expression::UnaryOp {
                    op: UnaryOperator::Minus,
                    expr: Box::new(self.parse_unary()?),
                })
            }
            _ => self.parse_primary(),
        }
    }

    pub(super) fn parse_primary(&mut self) -> Result<Expression, RustqlError> {
        let expr = self.parse_primary_inner()?;
        if *self.current_token() == Token::DoubleColon {
            self.advance();
            let data_type = self.parse_data_type()?;
            return Ok(Expression::Cast {
                expr: Box::new(expr),
                data_type,
            });
        }
        Ok(expr)
    }

    pub(super) fn parse_primary_inner(&mut self) -> Result<Expression, RustqlError> {
        match self.current_token().clone() {
            Token::Exists => {
                self.advance();
                self.consume(Token::LeftParen)?;
                let sub = self.parse_select_inner(Vec::new())?;
                self.consume(Token::RightParen)?;
                Ok(Expression::Exists(Box::new(sub)))
            }
            Token::Case => self.parse_case_expression(),
            Token::Cast => self.parse_cast_expression(),
            Token::Upper
            | Token::Lower
            | Token::Length
            | Token::Substring
            | Token::Abs
            | Token::Round
            | Token::Coalesce
            | Token::Trim
            | Token::Replace
            | Token::Position
            | Token::Instr
            | Token::Ceil
            | Token::Ceiling
            | Token::Floor
            | Token::Sqrt
            | Token::Power
            | Token::Mod
            | Token::Now
            | Token::Year
            | Token::Month
            | Token::Day
            | Token::DateAdd
            | Token::Datediff
            | Token::ConcatFn
            | Token::Nullif
            | Token::Greatest
            | Token::Least
            | Token::Lpad
            | Token::Rpad
            | Token::Reverse
            | Token::Repeat
            | Token::Log
            | Token::Exp
            | Token::Sign
            | Token::DateTrunc
            | Token::Extract
            | Token::Ltrim
            | Token::Rtrim
            | Token::Ascii
            | Token::Chr
            | Token::Sin
            | Token::Cos
            | Token::Tan
            | Token::Asin
            | Token::Acos
            | Token::Atan
            | Token::Atan2
            | Token::Random
            | Token::Degrees
            | Token::Radians
            | Token::Quarter
            | Token::Week
            | Token::DayOfWeek
            | Token::Pi
            | Token::Trunc
            | Token::Log10
            | Token::Log2
            | Token::Cbrt
            | Token::Gcd
            | Token::Lcm
            | Token::Initcap
            | Token::SplitPart
            | Token::Translate
            | Token::RegexpMatch
            | Token::RegexpReplace => self.parse_scalar_function(),
            Token::Left | Token::Right => {
                if self.current + 1 < self.tokens.len()
                    && self.tokens[self.current + 1] == Token::LeftParen
                {
                    self.parse_scalar_function()
                } else {
                    let name = format!("{:?}", self.current_token());
                    self.advance();
                    Ok(Expression::Column(name))
                }
            }
            Token::Count
            | Token::Sum
            | Token::Avg
            | Token::Min
            | Token::Max
            | Token::Stddev
            | Token::Variance
            | Token::GroupConcat
            | Token::StringAgg
            | Token::BoolAnd
            | Token::BoolOr
            | Token::Every
            | Token::Median
            | Token::Mode
            | Token::PercentileCont
            | Token::PercentileDisc => {
                let agg = self.parse_aggregate_function()?;
                if let Column::Function(func) = agg {
                    Ok(Expression::Function(func))
                } else if let Column::Expression { expr, .. } = agg {
                    Ok(expr)
                } else {
                    Err(RustqlError::ParseError(
                        "Expected aggregate function".to_string(),
                    ))
                }
            }
            Token::Null => {
                self.advance();
                Ok(Expression::Value(Value::Null))
            }
            Token::Identifier(name) => {
                self.advance();
                Ok(Expression::Column(name))
            }
            Token::Number(n) => {
                self.advance();
                Ok(Expression::Value(Value::Integer(n)))
            }
            Token::Float(f) => {
                self.advance();
                Ok(Expression::Value(Value::Float(f)))
            }
            Token::StringLiteral(s) => {
                self.advance();
                Ok(Expression::Value(Value::Text(s)))
            }
            Token::LeftParen => {
                self.advance();
                if *self.current_token() == Token::Select {
                    let sub = self.parse_select_inner(Vec::new())?;
                    self.consume(Token::RightParen)?;
                    Ok(Expression::Subquery(Box::new(sub)))
                } else {
                    let expr = self.parse_expression()?;
                    self.consume(Token::RightParen)?;
                    Ok(expr)
                }
            }
            Token::GenerateSeries
            | Token::Filter
            | Token::Lateral
            | Token::Grouping
            | Token::Sets
            | Token::Cube
            | Token::Rollup
            | Token::Fetch
            | Token::First
            | Token::Next
            | Token::Only
            | Token::Ties
            | Token::Row
            | Token::Window
            | Token::Merge
            | Token::Matched
            | Token::Generated
            | Token::Always
            | Token::Stored => {
                let name = token_to_string(self.current_token()).to_lowercase();
                self.advance();
                Ok(Expression::Column(name))
            }
            _ => Err(RustqlError::ParseError(format!(
                "Unexpected token in expression: {:?}",
                self.current_token()
            ))),
        }
    }
}
