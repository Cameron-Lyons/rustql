use crate::ast::*;
use crate::error::RustqlError;
use crate::lexer::Token;

pub struct Parser {
    tokens: Vec<Token>,
    current: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        Parser { tokens, current: 0 }
    }

    fn current_token(&self) -> &Token {
        self.tokens.get(self.current).unwrap_or(&Token::Eof)
    }

    #[allow(dead_code)]
    fn peek_token(&self) -> &Token {
        self.tokens.get(self.current + 1).unwrap_or(&Token::Eof)
    }

    fn consume(&mut self, expected: Token) -> Result<(), RustqlError> {
        if *self.current_token() == expected {
            self.current += 1;
            Ok(())
        } else {
            Err(RustqlError::ParseError(format!(
                "Expected {:?}, found {:?}",
                expected,
                self.current_token()
            )))
        }
    }

    fn advance(&mut self) -> Token {
        let token = self.current_token().clone();
        self.current += 1;
        token
    }

    fn parse_statement(&mut self) -> Result<Statement, RustqlError> {
        match self.current_token() {
            Token::Explain => self.parse_explain(),
            Token::With => self.parse_with_select(),
            Token::Select => self.parse_select_statement(Vec::new()),
            Token::Insert => self.parse_insert(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            Token::Alter => self.parse_alter(),
            Token::Begin => self.parse_begin_transaction(),
            Token::Commit => self.parse_commit_transaction(),
            Token::Rollback => self.parse_rollback(),
            Token::Savepoint => self.parse_savepoint(),
            Token::Release => self.parse_release_savepoint(),
            Token::Describe => self.parse_describe(),
            Token::Show => self.parse_show(),
            Token::Analyze => self.parse_analyze(),
            _ => Err(RustqlError::ParseError(format!(
                "Unexpected token: {:?}",
                self.current_token()
            ))),
        }
    }

    fn parse_with_select(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::With)?;
        let mut ctes = Vec::new();
        loop {
            let name = match self.advance() {
                Token::Identifier(name) => name,
                _ => {
                    return Err(RustqlError::ParseError(
                        "Expected CTE name after WITH".to_string(),
                    ));
                }
            };
            self.consume(Token::As)?;
            self.consume(Token::LeftParen)?;
            let query_stmt = self.parse_select_statement(Vec::new())?;
            let query = if let Statement::Select(s) = query_stmt {
                s
            } else {
                return Err(RustqlError::ParseError(
                    "Expected SELECT in CTE definition".to_string(),
                ));
            };
            self.consume(Token::RightParen)?;
            ctes.push(Cte { name, query });
            if *self.current_token() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }
        self.parse_select_statement(ctes)
    }

    fn parse_select_statement(&mut self, ctes: Vec<Cte>) -> Result<Statement, RustqlError> {
        let stmt = self.parse_select_inner(ctes)?;
        Ok(Statement::Select(stmt))
    }

    fn parse_select_inner(&mut self, ctes: Vec<Cte>) -> Result<SelectStatement, RustqlError> {
        self.consume(Token::Select)?;

        let distinct = if *self.current_token() == Token::Distinct {
            self.advance();
            true
        } else {
            false
        };

        let columns = self.parse_columns()?;

        self.consume(Token::From)?;

        let table = match self.advance() {
            Token::Identifier(name) => name,
            _ => return Err(RustqlError::ParseError("Expected table name".to_string())),
        };

        let mut joins = Vec::new();
        loop {
            let join_type = if *self.current_token() == Token::Left {
                self.advance();
                Some(JoinType::Left)
            } else if *self.current_token() == Token::Right {
                self.advance();
                Some(JoinType::Right)
            } else if *self.current_token() == Token::Full {
                self.advance();
                Some(JoinType::Full)
            } else if *self.current_token() == Token::Inner {
                self.advance();
                Some(JoinType::Inner)
            } else if *self.current_token() == Token::Cross {
                self.advance();
                Some(JoinType::Cross)
            } else if *self.current_token() == Token::Natural {
                self.advance();
                Some(JoinType::Natural)
            } else if *self.current_token() == Token::Join {
                Some(JoinType::Inner)
            } else {
                None
            };

            if let Some(join_type) = join_type {
                self.consume(Token::Join)?;

                let join_table = match self.advance() {
                    Token::Identifier(name) => name,
                    _ => {
                        return Err(RustqlError::ParseError(
                            "Expected table name after JOIN".to_string(),
                        ));
                    }
                };

                let on_expr = if matches!(join_type, JoinType::Cross | JoinType::Natural) {
                    if *self.current_token() == Token::On {
                        self.advance();
                        Some(self.parse_expression()?)
                    } else {
                        None
                    }
                } else {
                    self.consume(Token::On)?;
                    Some(self.parse_expression()?)
                };

                joins.push(Join {
                    join_type,
                    table: join_table,
                    on: on_expr,
                });
            } else {
                break;
            }
        }

        let where_clause = if *self.current_token() == Token::Where {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        let group_by = if *self.current_token() == Token::Group {
            self.advance();
            self.consume(Token::By)?;
            Some(self.parse_group_by()?)
        } else {
            None
        };

        let having = if *self.current_token() == Token::Having {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        let order_by = if *self.current_token() == Token::Order {
            self.advance();
            self.consume(Token::By)?;
            Some(self.parse_order_by()?)
        } else {
            None
        };

        let limit = if *self.current_token() == Token::Limit {
            self.advance();
            match self.advance() {
                Token::Number(n) => Some(n as usize),
                _ => {
                    return Err(RustqlError::ParseError(
                        "Expected number after LIMIT".to_string(),
                    ));
                }
            }
        } else {
            None
        };

        let offset = if *self.current_token() == Token::Offset {
            self.advance();
            match self.advance() {
                Token::Number(n) => Some(n as usize),
                _ => {
                    return Err(RustqlError::ParseError(
                        "Expected number after OFFSET".to_string(),
                    ));
                }
            }
        } else {
            None
        };

        let (union, union_all) = if *self.current_token() == Token::Union {
            self.advance();
            let is_all = *self.current_token() == Token::All;
            if is_all {
                self.advance();
            }
            let union_stmt = self.parse_select_inner(Vec::new())?;
            (Some(Box::new(union_stmt)), is_all)
        } else {
            (None, false)
        };

        Ok(SelectStatement {
            ctes,
            distinct,
            columns,
            from: table,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            union,
            union_all,
        })
    }

    fn parse_columns(&mut self) -> Result<Vec<Column>, RustqlError> {
        let mut columns = Vec::new();

        if *self.current_token() == Token::Star {
            self.advance();
            columns.push(Column::All);
        } else {
            loop {
                let column = match self.current_token() {
                    Token::Count | Token::Sum | Token::Avg | Token::Min | Token::Max => {
                        if self.current + 1 < self.tokens.len()
                            && self.tokens[self.current + 1] == Token::LeftParen
                        {
                            if self.current + 2 < self.tokens.len()
                                && matches!(
                                    &self.tokens[self.current + 2],
                                    Token::Star
                                        | Token::Distinct
                                        | Token::Identifier(_)
                                        | Token::Number(_)
                                        | Token::Float(_)
                                        | Token::StringLiteral(_)
                                )
                            {
                                self.parse_aggregate_function()?
                            } else {
                                let saved_pos = self.current;
                                match self.parse_column_expression() {
                                    Ok((expr, alias)) => Column::Expression { expr, alias },
                                    Err(_) => {
                                        self.current = saved_pos;
                                        self.parse_aggregate_function()?
                                    }
                                }
                            }
                        } else {
                            let saved_pos = self.current;
                            match self.parse_column_expression() {
                                Ok((expr, alias)) => Column::Expression { expr, alias },
                                Err(_) => {
                                    self.current = saved_pos;
                                    self.parse_aggregate_function()?
                                }
                            }
                        }
                    }
                    Token::RowNumber | Token::Rank | Token::DenseRank => {
                        let saved_pos = self.current;
                        match self.parse_window_function_column() {
                            Ok(col) => col,
                            Err(_) => {
                                self.current = saved_pos;
                                let (expr, alias) = self.parse_column_expression()?;
                                Column::Expression { expr, alias }
                            }
                        }
                    }
                    Token::LeftParen => {
                        let check_idx = self.current + 1;
                        if check_idx < self.tokens.len()
                            && matches!(&self.tokens[check_idx], Token::Select)
                        {
                            self.advance();
                            let subquery_stmt = self.parse_select_inner(Vec::new())?;
                            self.consume(Token::RightParen)?;
                            Column::Subquery(Box::new(subquery_stmt))
                        } else {
                            let saved_pos = self.current;
                            match self.parse_column_expression() {
                                Ok((expr, alias)) => Column::Expression { expr, alias },
                                Err(_) => {
                                    self.current = saved_pos;
                                    return Err(RustqlError::ParseError(
                                        "Unexpected '(' in column list".to_string(),
                                    ));
                                }
                            }
                        }
                    }
                    Token::Case => {
                        let expr = self.parse_case_expression()?;
                        let alias = if *self.current_token() == Token::As {
                            self.advance();
                            match self.advance() {
                                Token::Identifier(alias) => Some(alias),
                                _ => {
                                    return Err(RustqlError::ParseError(
                                        "Expected alias after AS".to_string(),
                                    ));
                                }
                            }
                        } else {
                            None
                        };
                        Column::Expression { expr, alias }
                    }
                    Token::Upper
                    | Token::Lower
                    | Token::Length
                    | Token::Substring
                    | Token::Abs
                    | Token::Round
                    | Token::Coalesce => {
                        let expr = self.parse_scalar_function()?;
                        let alias = if *self.current_token() == Token::As {
                            self.advance();
                            match self.advance() {
                                Token::Identifier(alias) => Some(alias),
                                _ => {
                                    return Err(RustqlError::ParseError(
                                        "Expected alias after AS".to_string(),
                                    ));
                                }
                            }
                        } else {
                            None
                        };
                        Column::Expression { expr, alias }
                    }
                    _ => {
                        let saved_pos = self.current;
                        match self.parse_column_expression() {
                            Ok((expr, alias)) => {
                                if let Expression::Column(name) = &expr {
                                    Column::Named {
                                        name: name.clone(),
                                        alias,
                                    }
                                } else {
                                    Column::Expression { expr, alias }
                                }
                            }
                            Err(_) => {
                                self.current = saved_pos;
                                match self.current_token() {
                                    Token::Identifier(name) => {
                                        let name = name.clone();
                                        self.advance();

                                        let alias = if *self.current_token() == Token::As {
                                            self.advance();
                                            match self.advance() {
                                                Token::Identifier(alias) => Some(alias),
                                                _ => {
                                                    return Err(RustqlError::ParseError(
                                                        "Expected alias after AS".to_string(),
                                                    ));
                                                }
                                            }
                                        } else {
                                            None
                                        };

                                        Column::Named { name, alias }
                                    }
                                    _ => break,
                                }
                            }
                        }
                    }
                };

                columns.push(column);

                if *self.current_token() != Token::Comma {
                    break;
                }
                self.advance();
            }
        }

        if columns.is_empty() {
            return Err(RustqlError::ParseError(
                "Expected column names or *".to_string(),
            ));
        }

        Ok(columns)
    }

    fn parse_window_function_column(&mut self) -> Result<Column, RustqlError> {
        let func_type = match self.advance() {
            Token::RowNumber => WindowFunctionType::RowNumber,
            Token::Rank => WindowFunctionType::Rank,
            Token::DenseRank => WindowFunctionType::DenseRank,
            _ => {
                return Err(RustqlError::ParseError(
                    "Expected window function".to_string(),
                ));
            }
        };
        self.consume(Token::LeftParen)?;
        self.consume(Token::RightParen)?;

        let (partition_by, order_by) = self.parse_over_clause()?;

        let alias = if *self.current_token() == Token::As {
            self.advance();
            match self.advance() {
                Token::Identifier(alias) => Some(alias),
                _ => {
                    return Err(RustqlError::ParseError(
                        "Expected alias after AS".to_string(),
                    ));
                }
            }
        } else {
            None
        };

        Ok(Column::Expression {
            expr: Expression::WindowFunction {
                function: func_type,
                partition_by,
                order_by,
            },
            alias,
        })
    }

    fn parse_over_clause(&mut self) -> Result<(Vec<Expression>, Vec<OrderByExpr>), RustqlError> {
        self.consume(Token::Over)?;
        self.consume(Token::LeftParen)?;

        let partition_by = if *self.current_token() == Token::Partition {
            self.advance();
            self.consume(Token::By)?;
            let mut exprs = Vec::new();
            loop {
                exprs.push(self.parse_expression()?);
                if *self.current_token() == Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            exprs
        } else {
            Vec::new()
        };

        let order_by = if *self.current_token() == Token::Order {
            self.advance();
            self.consume(Token::By)?;
            self.parse_order_by()?
        } else {
            Vec::new()
        };

        self.consume(Token::RightParen)?;
        Ok((partition_by, order_by))
    }

    fn parse_column_expression(&mut self) -> Result<(Expression, Option<String>), RustqlError> {
        let expr = self.parse_arithmetic_expression()?;

        let alias = if *self.current_token() == Token::As {
            self.advance();
            match self.advance() {
                Token::Identifier(alias) => Some(alias),
                _ => {
                    return Err(RustqlError::ParseError(
                        "Expected alias after AS".to_string(),
                    ));
                }
            }
        } else {
            None
        };

        Ok((expr, alias))
    }

    fn parse_arithmetic_expression(&mut self) -> Result<Expression, RustqlError> {
        self.parse_arithmetic_term()
    }

    fn parse_arithmetic_term(&mut self) -> Result<Expression, RustqlError> {
        let mut expr = self.parse_arithmetic_factor()?;

        loop {
            let op = match self.current_token() {
                Token::Plus => BinaryOperator::Plus,
                Token::Minus => BinaryOperator::Minus,
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

    fn parse_arithmetic_factor(&mut self) -> Result<Expression, RustqlError> {
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

    fn parse_arithmetic_unary(&mut self) -> Result<Expression, RustqlError> {
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

    fn parse_arithmetic_primary(&mut self) -> Result<Expression, RustqlError> {
        match self.current_token() {
            Token::LeftParen => {
                self.advance();
                let expr = self.parse_arithmetic_expression()?;
                self.consume(Token::RightParen)?;
                Ok(expr)
            }
            Token::Case => self.parse_case_expression(),
            Token::Upper
            | Token::Lower
            | Token::Length
            | Token::Substring
            | Token::Abs
            | Token::Round
            | Token::Coalesce => self.parse_scalar_function(),
            Token::Identifier(name) => {
                let name = name.clone();
                self.advance();
                Ok(Expression::Column(name))
            }
            Token::Number(n) => {
                let n = *n;
                self.advance();
                Ok(Expression::Value(Value::Integer(n)))
            }
            Token::Float(n) => {
                let n = *n;
                self.advance();
                Ok(Expression::Value(Value::Float(n)))
            }
            Token::StringLiteral(s) => {
                let s = s.clone();
                self.advance();
                Ok(Expression::Value(Value::Text(s)))
            }
            _ => Err(RustqlError::ParseError(format!(
                "Unexpected token in expression: {:?}",
                self.current_token()
            ))),
        }
    }

    fn parse_case_expression(&mut self) -> Result<Expression, RustqlError> {
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

    fn parse_scalar_function(&mut self) -> Result<Expression, RustqlError> {
        let func_type = match self.advance() {
            Token::Upper => ScalarFunctionType::Upper,
            Token::Lower => ScalarFunctionType::Lower,
            Token::Length => ScalarFunctionType::Length,
            Token::Substring => ScalarFunctionType::Substring,
            Token::Abs => ScalarFunctionType::Abs,
            Token::Round => ScalarFunctionType::Round,
            Token::Coalesce => ScalarFunctionType::Coalesce,
            _ => {
                return Err(RustqlError::ParseError(
                    "Expected scalar function name".to_string(),
                ));
            }
        };
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

    fn parse_aggregate_function(&mut self) -> Result<Column, RustqlError> {
        let func_type = match self.advance() {
            Token::Count => AggregateFunctionType::Count,
            Token::Sum => AggregateFunctionType::Sum,
            Token::Avg => AggregateFunctionType::Avg,
            Token::Min => AggregateFunctionType::Min,
            Token::Max => AggregateFunctionType::Max,
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

        let expr = if *self.current_token() == Token::Star {
            self.advance();
            Box::new(Expression::Column("*".to_string()))
        } else {
            Box::new(self.parse_expression()?)
        };

        if distinct && matches!(&*expr, Expression::Column(name) if name == "*") {
            return Err(RustqlError::ParseError(
                "DISTINCT * is not supported".to_string(),
            ));
        }

        self.consume(Token::RightParen)?;

        if *self.current_token() == Token::Over {
            let agg_type = func_type;
            let (partition_by, order_by) = self.parse_over_clause()?;
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
                    partition_by,
                    order_by,
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
        }))
    }

    fn parse_group_by(&mut self) -> Result<Vec<String>, RustqlError> {
        let mut columns = Vec::new();

        loop {
            match self.advance() {
                Token::Identifier(name) => columns.push(name),
                _ => {
                    return Err(RustqlError::ParseError(
                        "Expected column name in GROUP BY".to_string(),
                    ));
                }
            }

            if *self.current_token() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(columns)
    }

    fn parse_insert(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Insert)?;
        self.consume(Token::Into)?;

        let table = match self.advance() {
            Token::Identifier(name) => name,
            _ => return Err(RustqlError::ParseError("Expected table name".to_string())),
        };

        let columns = if *self.current_token() == Token::LeftParen {
            self.advance();
            let mut cols = Vec::new();

            while let Token::Identifier(name) = self.current_token() {
                cols.push(name.clone());
                self.advance();

                if *self.current_token() == Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }

            self.consume(Token::RightParen)?;
            Some(cols)
        } else {
            None
        };

        if *self.current_token() == Token::Select || *self.current_token() == Token::With {
            let source_stmt = if *self.current_token() == Token::With {
                self.parse_with_select()?
            } else {
                self.parse_select_statement(Vec::new())?
            };
            let source_query = if let Statement::Select(s) = source_stmt {
                s
            } else {
                return Err(RustqlError::ParseError(
                    "Expected SELECT after INSERT INTO table".to_string(),
                ));
            };
            return Ok(Statement::Insert(InsertStatement {
                table,
                columns,
                values: Vec::new(),
                source_query: Some(Box::new(source_query)),
            }));
        }

        self.consume(Token::Values)?;

        let values = self.parse_values()?;

        Ok(Statement::Insert(InsertStatement {
            table,
            columns,
            values,
            source_query: None,
        }))
    }

    fn parse_values(&mut self) -> Result<Vec<Vec<Value>>, RustqlError> {
        let mut all_values = Vec::new();

        loop {
            self.consume(Token::LeftParen)?;
            let mut values = Vec::new();

            loop {
                values.push(self.parse_value()?);

                if *self.current_token() == Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }

            self.consume(Token::RightParen)?;
            all_values.push(values);

            if *self.current_token() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(all_values)
    }

    fn parse_value(&mut self) -> Result<Value, RustqlError> {
        match self.advance() {
            Token::Null => Ok(Value::Null),
            Token::Number(n) => Ok(Value::Integer(n)),
            Token::Float(f) => Ok(Value::Float(f)),
            Token::StringLiteral(s) => Ok(Value::Text(s)),
            _ => Err(RustqlError::ParseError("Expected value".to_string())),
        }
    }

    fn parse_update(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Update)?;

        let table = match self.advance() {
            Token::Identifier(name) => name,
            _ => return Err(RustqlError::ParseError("Expected table name".to_string())),
        };

        self.consume(Token::Set)?;

        let assignments = self.parse_assignments()?;

        let where_clause = if *self.current_token() == Token::Where {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Statement::Update(UpdateStatement {
            table,
            assignments,
            where_clause,
        }))
    }

    fn parse_assignments(&mut self) -> Result<Vec<Assignment>, RustqlError> {
        let mut assignments = Vec::new();

        loop {
            let column = match self.advance() {
                Token::Identifier(name) => name,
                _ => return Err(RustqlError::ParseError("Expected column name".to_string())),
            };

            self.consume(Token::Equal)?;

            let value = self.parse_value()?;

            assignments.push(Assignment { column, value });

            if *self.current_token() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(assignments)
    }

    fn parse_delete(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Delete)?;
        self.consume(Token::From)?;

        let table = match self.advance() {
            Token::Identifier(name) => name,
            _ => return Err(RustqlError::ParseError("Expected table name".to_string())),
        };

        let where_clause = if *self.current_token() == Token::Where {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        Ok(Statement::Delete(DeleteStatement {
            table,
            where_clause,
        }))
    }

    fn parse_create(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Create)?;

        match self.current_token() {
            Token::Table => {
                self.advance();
                let name = match self.advance() {
                    Token::Identifier(name) => name,
                    _ => return Err(RustqlError::ParseError("Expected table name".to_string())),
                };

                self.consume(Token::LeftParen)?;

                let columns = self.parse_column_definitions()?;

                self.consume(Token::RightParen)?;

                Ok(Statement::CreateTable(CreateTableStatement {
                    name,
                    columns,
                }))
            }
            Token::Index => {
                self.advance();
                let index_name = match self.advance() {
                    Token::Identifier(name) => name,
                    _ => return Err(RustqlError::ParseError("Expected index name".to_string())),
                };

                self.consume(Token::On)?;

                let table = match self.advance() {
                    Token::Identifier(name) => name,
                    _ => return Err(RustqlError::ParseError("Expected table name".to_string())),
                };

                self.consume(Token::LeftParen)?;

                let mut columns = Vec::new();
                loop {
                    match self.advance() {
                        Token::Identifier(name) => columns.push(name),
                        _ => {
                            return Err(RustqlError::ParseError(
                                "Expected column name".to_string(),
                            ));
                        }
                    }
                    if *self.current_token() == Token::Comma {
                        self.advance();
                    } else {
                        break;
                    }
                }

                self.consume(Token::RightParen)?;

                Ok(Statement::CreateIndex(CreateIndexStatement {
                    name: index_name,
                    table,
                    columns,
                }))
            }
            _ => Err(RustqlError::ParseError(
                "Expected TABLE or INDEX after CREATE".to_string(),
            )),
        }
    }

    fn parse_column_definitions(&mut self) -> Result<Vec<ColumnDefinition>, RustqlError> {
        let mut columns = Vec::new();

        loop {
            let name = match self.advance() {
                Token::Identifier(name) => name,
                _ => return Err(RustqlError::ParseError("Expected column name".to_string())),
            };

            let data_type = self.parse_data_type()?;

            let mut primary_key = false;
            let mut unique = false;
            let mut default_value = None;
            let mut nullable = true;
            let mut check = None;
            let mut auto_increment = false;

            if *self.current_token() == Token::Primary {
                self.advance();
                self.consume(Token::Key)?;
                primary_key = true;
                nullable = false;
            }

            if *self.current_token() == Token::Not {
                self.advance();
                self.consume(Token::Null)?;
                nullable = false;
            }

            if *self.current_token() == Token::Unique {
                self.advance();
                unique = true;
            }

            if *self.current_token() == Token::Default {
                self.advance();
                default_value = Some(self.parse_value()?);
            }

            if *self.current_token() == Token::Check {
                self.advance();
                self.consume(Token::LeftParen)?;
                let mut depth = 1;
                let mut check_str = String::new();
                while depth > 0 {
                    let tok = self.advance();
                    match tok {
                        Token::LeftParen => {
                            depth += 1;
                            check_str.push('(');
                        }
                        Token::RightParen => {
                            depth -= 1;
                            if depth > 0 {
                                check_str.push(')');
                            }
                        }
                        _ => {
                            if !check_str.is_empty() {
                                check_str.push(' ');
                            }
                            check_str.push_str(&token_to_string(&tok));
                        }
                    }
                }
                check = Some(check_str);
            }

            if *self.current_token() == Token::Autoincrement {
                self.advance();
                auto_increment = true;
            }

            let foreign_key = if *self.current_token() == Token::Foreign {
                self.advance();
                if *self.current_token() == Token::Key {
                    self.advance();
                }
                self.consume(Token::References)?;
                true
            } else if *self.current_token() == Token::References {
                self.advance();
                true
            } else {
                false
            };
            let foreign_key = if foreign_key {
                let ref_table = match self.advance() {
                    Token::Identifier(name) => name,
                    _ => {
                        return Err(RustqlError::ParseError(
                            "Expected table name after REFERENCES".to_string(),
                        ));
                    }
                };
                self.consume(Token::LeftParen)?;
                let ref_column = match self.advance() {
                    Token::Identifier(name) => name,
                    _ => {
                        return Err(RustqlError::ParseError(
                            "Expected column name after REFERENCES table".to_string(),
                        ));
                    }
                };
                self.consume(Token::RightParen)?;

                let mut on_delete = crate::ast::ForeignKeyAction::Restrict;
                let mut on_update = crate::ast::ForeignKeyAction::Restrict;

                while *self.current_token() == Token::On {
                    self.advance();
                    let action_type = match self.current_token() {
                        Token::Delete => {
                            self.advance();
                            &mut on_delete
                        }
                        Token::Update => {
                            self.advance();
                            &mut on_update
                        }
                        _ => {
                            return Err(RustqlError::ParseError(
                                "Expected DELETE or UPDATE after ON".to_string(),
                            ));
                        }
                    };

                    *action_type = match self.current_token() {
                        Token::Cascade => {
                            self.advance();
                            crate::ast::ForeignKeyAction::Cascade
                        }
                        Token::Restrict => {
                            self.advance();
                            crate::ast::ForeignKeyAction::Restrict
                        }
                        Token::Set => {
                            self.advance();
                            self.consume(Token::Null)?;
                            crate::ast::ForeignKeyAction::SetNull
                        }
                        Token::No => {
                            self.advance();
                            self.consume(Token::Action)?;
                            crate::ast::ForeignKeyAction::NoAction
                        }
                        _ => {
                            return Err(RustqlError::ParseError(
                                "Expected CASCADE, RESTRICT, SET NULL, or NO ACTION".to_string(),
                            ));
                        }
                    };
                }

                Some(crate::ast::ForeignKeyConstraint {
                    referenced_table: ref_table,
                    referenced_column: ref_column,
                    on_delete,
                    on_update,
                })
            } else {
                None
            };

            columns.push(ColumnDefinition {
                name,
                data_type,
                nullable,
                primary_key,
                unique,
                default_value,
                foreign_key,
                check,
                auto_increment,
            });

            if *self.current_token() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(columns)
    }

    fn parse_data_type(&mut self) -> Result<DataType, RustqlError> {
        match self.advance() {
            Token::Boolean => Ok(DataType::Boolean),
            Token::Date => Ok(DataType::Date),
            Token::Time => Ok(DataType::Time),
            Token::DateTime => Ok(DataType::DateTime),
            Token::Identifier(name) => match name.to_uppercase().as_str() {
                "INT" | "INTEGER" => Ok(DataType::Integer),
                "FLOAT" | "REAL" | "DOUBLE" => Ok(DataType::Float),
                "TEXT" | "VARCHAR" | "STRING" => Ok(DataType::Text),
                "BOOL" => Ok(DataType::Boolean),
                "DATETIME" | "TIMESTAMP" => Ok(DataType::DateTime),
                _ => Err(RustqlError::ParseError(format!(
                    "Unknown data type: {}",
                    name
                ))),
            },
            _ => Err(RustqlError::ParseError("Expected data type".to_string())),
        }
    }

    fn parse_drop(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Drop)?;

        match self.current_token() {
            Token::Table => {
                self.advance();
                let name = match self.advance() {
                    Token::Identifier(name) => name,
                    _ => return Err(RustqlError::ParseError("Expected table name".to_string())),
                };

                Ok(Statement::DropTable(DropTableStatement { name }))
            }
            Token::Index => {
                self.advance();
                let name = match self.advance() {
                    Token::Identifier(name) => name,
                    _ => return Err(RustqlError::ParseError("Expected index name".to_string())),
                };

                Ok(Statement::DropIndex(DropIndexStatement { name }))
            }
            _ => Err(RustqlError::ParseError(
                "Expected TABLE or INDEX after DROP".to_string(),
            )),
        }
    }

    fn parse_alter(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Alter)?;
        self.consume(Token::Table)?;

        let table = match self.advance() {
            Token::Identifier(name) => name,
            _ => return Err(RustqlError::ParseError("Expected table name".to_string())),
        };

        let operation = match self.current_token() {
            Token::Add => {
                self.advance();
                self.consume(Token::Column)?;
                let name = match self.advance() {
                    Token::Identifier(n) => n,
                    _ => return Err(RustqlError::ParseError("Expected column name".to_string())),
                };
                let data_type = self.parse_data_type()?;
                AlterOperation::AddColumn(ColumnDefinition {
                    name,
                    data_type,
                    nullable: true,
                    primary_key: false,
                    unique: false,
                    default_value: None,
                    foreign_key: None,
                    check: None,
                    auto_increment: false,
                })
            }
            Token::Drop => {
                self.advance();
                self.consume(Token::Column)?;
                match self.advance() {
                    Token::Identifier(name) => AlterOperation::DropColumn(name),
                    _ => return Err(RustqlError::ParseError("Expected column name".to_string())),
                }
            }
            Token::Rename => {
                self.advance();
                self.consume(Token::Column)?;
                let old = match self.advance() {
                    Token::Identifier(n) => n,
                    _ => return Err(RustqlError::ParseError("Expected column name".to_string())),
                };
                self.consume(Token::To)?;
                let new = match self.advance() {
                    Token::Identifier(n) => n,
                    _ => {
                        return Err(RustqlError::ParseError(
                            "Expected new column name".to_string(),
                        ));
                    }
                };
                AlterOperation::RenameColumn { old, new }
            }
            _ => {
                return Err(RustqlError::ParseError(
                    "Expected ADD, DROP, or RENAME after ALTER TABLE".to_string(),
                ));
            }
        };

        Ok(Statement::AlterTable(AlterTableStatement {
            table,
            operation,
        }))
    }

    fn parse_expression(&mut self) -> Result<Expression, RustqlError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expression, RustqlError> {
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

    fn parse_and(&mut self) -> Result<Expression, RustqlError> {
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

    fn parse_comparison(&mut self) -> Result<Expression, RustqlError> {
        let mut expr = self.parse_term()?;

        loop {
            match self.current_token() {
                Token::Equal => {
                    self.advance();
                    let right = self.parse_term()?;
                    expr = Expression::BinaryOp {
                        left: Box::new(expr),
                        op: BinaryOperator::Equal,
                        right: Box::new(right),
                    };
                }
                Token::NotEqual => {
                    self.advance();
                    let right = self.parse_term()?;
                    expr = Expression::BinaryOp {
                        left: Box::new(expr),
                        op: BinaryOperator::NotEqual,
                        right: Box::new(right),
                    };
                }
                Token::LessThan => {
                    self.advance();
                    let right = self.parse_term()?;
                    expr = Expression::BinaryOp {
                        left: Box::new(expr),
                        op: BinaryOperator::LessThan,
                        right: Box::new(right),
                    };
                }
                Token::LessThanOrEqual => {
                    self.advance();
                    let right = self.parse_term()?;
                    expr = Expression::BinaryOp {
                        left: Box::new(expr),
                        op: BinaryOperator::LessThanOrEqual,
                        right: Box::new(right),
                    };
                }
                Token::GreaterThan => {
                    self.advance();
                    let right = self.parse_term()?;
                    expr = Expression::BinaryOp {
                        left: Box::new(expr),
                        op: BinaryOperator::GreaterThan,
                        right: Box::new(right),
                    };
                }
                Token::GreaterThanOrEqual => {
                    self.advance();
                    let right = self.parse_term()?;
                    expr = Expression::BinaryOp {
                        left: Box::new(expr),
                        op: BinaryOperator::GreaterThanOrEqual,
                        right: Box::new(right),
                    };
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
                    self.consume(Token::Null)?;
                    expr = Expression::IsNull {
                        expr: Box::new(expr),
                        not,
                    };
                }
                _ => break,
            }
        }

        Ok(expr)
    }

    fn parse_term(&mut self) -> Result<Expression, RustqlError> {
        let mut expr = self.parse_factor()?;

        loop {
            let op = match self.current_token() {
                Token::Plus => BinaryOperator::Plus,
                Token::Minus => BinaryOperator::Minus,
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

    fn parse_factor(&mut self) -> Result<Expression, RustqlError> {
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

    fn parse_unary(&mut self) -> Result<Expression, RustqlError> {
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

    fn parse_primary(&mut self) -> Result<Expression, RustqlError> {
        match self.current_token().clone() {
            Token::Exists => {
                self.advance();
                self.consume(Token::LeftParen)?;
                let sub = self.parse_select_inner(Vec::new())?;
                self.consume(Token::RightParen)?;
                Ok(Expression::Exists(Box::new(sub)))
            }
            Token::Case => self.parse_case_expression(),
            Token::Upper
            | Token::Lower
            | Token::Length
            | Token::Substring
            | Token::Abs
            | Token::Round
            | Token::Coalesce => self.parse_scalar_function(),
            Token::Count | Token::Sum | Token::Avg | Token::Min | Token::Max => {
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
            _ => Err(RustqlError::ParseError(format!(
                "Unexpected token in expression: {:?}",
                self.current_token()
            ))),
        }
    }

    fn parse_order_by(&mut self) -> Result<Vec<OrderByExpr>, RustqlError> {
        let mut order_exprs = Vec::new();

        loop {
            let expr = self.parse_expression()?;

            let asc = if *self.current_token() == Token::Asc {
                self.advance();
                true
            } else if *self.current_token() == Token::Desc {
                self.advance();
                false
            } else {
                true
            };

            order_exprs.push(OrderByExpr { expr, asc });

            if *self.current_token() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(order_exprs)
    }

    fn parse_begin_transaction(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Begin)?;
        if *self.current_token() == Token::Transaction {
            self.advance();
        }
        Ok(Statement::BeginTransaction)
    }

    fn parse_commit_transaction(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Commit)?;
        if *self.current_token() == Token::Transaction {
            self.advance();
        }
        Ok(Statement::CommitTransaction)
    }

    fn parse_rollback(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Rollback)?;
        if *self.current_token() == Token::Transaction {
            self.advance();
            return Ok(Statement::RollbackTransaction);
        }
        if *self.current_token() == Token::To {
            self.advance();
            if *self.current_token() == Token::Savepoint {
                self.advance();
            }
            let name = match self.advance() {
                Token::Identifier(name) => name,
                _ => {
                    return Err(RustqlError::ParseError(
                        "Expected savepoint name after ROLLBACK TO".to_string(),
                    ));
                }
            };
            return Ok(Statement::RollbackToSavepoint(name));
        }
        Ok(Statement::RollbackTransaction)
    }

    fn parse_savepoint(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Savepoint)?;
        let name = match self.advance() {
            Token::Identifier(name) => name,
            _ => {
                return Err(RustqlError::ParseError(
                    "Expected savepoint name".to_string(),
                ));
            }
        };
        Ok(Statement::Savepoint(name))
    }

    fn parse_release_savepoint(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Release)?;
        if *self.current_token() == Token::Savepoint {
            self.advance();
        }
        let name = match self.advance() {
            Token::Identifier(name) => name,
            _ => {
                return Err(RustqlError::ParseError(
                    "Expected savepoint name after RELEASE".to_string(),
                ));
            }
        };
        Ok(Statement::ReleaseSavepoint(name))
    }

    fn parse_explain(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Explain)?;
        let select_stmt = if *self.current_token() == Token::With {
            self.parse_with_select()?
        } else {
            self.parse_select_statement(Vec::new())?
        };
        if let Statement::Select(select_stmt) = select_stmt {
            Ok(Statement::Explain(select_stmt))
        } else {
            Err(RustqlError::ParseError(
                "EXPLAIN must be followed by a SELECT statement".to_string(),
            ))
        }
    }

    fn parse_describe(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Describe)?;
        let table_name = match self.advance() {
            Token::Identifier(name) => name,
            _ => {
                return Err(RustqlError::ParseError(
                    "Expected table name after DESCRIBE".to_string(),
                ));
            }
        };
        Ok(Statement::Describe(table_name))
    }

    fn parse_show(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Show)?;
        if *self.current_token() == Token::Tables {
            self.advance();
            Ok(Statement::ShowTables)
        } else {
            Err(RustqlError::ParseError(
                "SHOW must be followed by TABLES".to_string(),
            ))
        }
    }

    fn parse_analyze(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Analyze)?;
        let table_name = match self.advance() {
            Token::Identifier(name) => name,
            _ => {
                return Err(RustqlError::ParseError(
                    "Expected table name after ANALYZE".to_string(),
                ));
            }
        };
        Ok(Statement::Analyze(table_name))
    }
}

fn token_to_string(tok: &Token) -> String {
    match tok {
        Token::Identifier(s) => s.clone(),
        Token::Number(n) => n.to_string(),
        Token::Float(f) => f.to_string(),
        Token::StringLiteral(s) => format!("'{}'", s),
        Token::Equal => "=".to_string(),
        Token::NotEqual => "<>".to_string(),
        Token::LessThan => "<".to_string(),
        Token::LessThanOrEqual => "<=".to_string(),
        Token::GreaterThan => ">".to_string(),
        Token::GreaterThanOrEqual => ">=".to_string(),
        Token::Plus => "+".to_string(),
        Token::Minus => "-".to_string(),
        Token::Star => "*".to_string(),
        Token::Divide => "/".to_string(),
        Token::And => "AND".to_string(),
        Token::Or => "OR".to_string(),
        Token::Not => "NOT".to_string(),
        Token::Null => "NULL".to_string(),
        Token::Is => "IS".to_string(),
        Token::In => "IN".to_string(),
        Token::Like => "LIKE".to_string(),
        Token::Between => "BETWEEN".to_string(),
        Token::Comma => ",".to_string(),
        _ => format!("{:?}", tok),
    }
}

pub fn parse(tokens: Vec<Token>) -> Result<Statement, RustqlError> {
    let mut parser = Parser::new(tokens);
    parser.parse_statement()
}
