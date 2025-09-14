use crate::ast::*;
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

    fn consume(&mut self, expected: Token) -> Result<(), String> {
        if *self.current_token() == expected {
            self.current += 1;
            Ok(())
        } else {
            Err(format!(
                "Expected {:?}, found {:?}",
                expected,
                self.current_token()
            ))
        }
    }

    fn advance(&mut self) -> Token {
        let token = self.current_token().clone();
        self.current += 1;
        token
    }

    fn parse_statement(&mut self) -> Result<Statement, String> {
        match self.current_token() {
            Token::Select => self.parse_select(),
            Token::Insert => self.parse_insert(),
            Token::Update => self.parse_update(),
            Token::Delete => self.parse_delete(),
            Token::Create => self.parse_create(),
            Token::Drop => self.parse_drop(),
            _ => Err(format!("Unexpected token: {:?}", self.current_token())),
        }
    }

    fn parse_select(&mut self) -> Result<Statement, String> {
        self.consume(Token::Select)?;

        let columns = self.parse_columns()?;

        self.consume(Token::From)?;

        let table = match self.advance() {
            Token::Identifier(name) => name,
            _ => return Err("Expected table name".to_string()),
        };

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
                _ => return Err("Expected number after LIMIT".to_string()),
            }
        } else {
            None
        };

        let offset = if *self.current_token() == Token::Offset {
            self.advance();
            match self.advance() {
                Token::Number(n) => Some(n as usize),
                _ => return Err("Expected number after OFFSET".to_string()),
            }
        } else {
            None
        };

        Ok(Statement::Select(SelectStatement {
            columns,
            from: table,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
        }))
    }

    fn parse_columns(&mut self) -> Result<Vec<Column>, String> {
        let mut columns = Vec::new();

        if *self.current_token() == Token::Star {
            self.advance();
            columns.push(Column::All);
        } else {
            loop {
                let column = match self.current_token() {
                    Token::Count | Token::Sum | Token::Avg | Token::Min | Token::Max => {
                        self.parse_aggregate_function()?
                    }
                    Token::Identifier(name) => {
                        let name = name.clone();
                        self.advance();

                        if *self.current_token() == Token::As {
                            self.advance();
                            match self.advance() {
                                Token::Identifier(_alias) => Column::Named(name),
                                _ => return Err("Expected alias after AS".to_string()),
                            }
                        } else {
                            Column::Named(name)
                        }
                    }
                    _ => break,
                };

                columns.push(column);

                if *self.current_token() != Token::Comma {
                    break;
                }
                self.advance();
            }
        }

        if columns.is_empty() {
            return Err("Expected column names or *".to_string());
        }

        Ok(columns)
    }

    fn parse_aggregate_function(&mut self) -> Result<Column, String> {
        let func_type = match self.advance() {
            Token::Count => AggregateFunctionType::Count,
            Token::Sum => AggregateFunctionType::Sum,
            Token::Avg => AggregateFunctionType::Avg,
            Token::Min => AggregateFunctionType::Min,
            Token::Max => AggregateFunctionType::Max,
            _ => return Err("Expected aggregate function".to_string()),
        };

        self.consume(Token::LeftParen)?;

        let expr = if *self.current_token() == Token::Star {
            self.advance();
            Box::new(Expression::Column("*".to_string()))
        } else {
            Box::new(self.parse_expression()?)
        };

        self.consume(Token::RightParen)?;

        let alias = if *self.current_token() == Token::As {
            self.advance();
            match self.advance() {
                Token::Identifier(name) => Some(name),
                _ => return Err("Expected alias after AS".to_string()),
            }
        } else {
            None
        };

        Ok(Column::Function(AggregateFunction {
            function: func_type,
            expr,
            alias,
        }))
    }

    fn parse_group_by(&mut self) -> Result<Vec<String>, String> {
        let mut columns = Vec::new();

        loop {
            match self.advance() {
                Token::Identifier(name) => columns.push(name),
                _ => return Err("Expected column name in GROUP BY".to_string()),
            }

            if *self.current_token() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(columns)
    }

    fn parse_insert(&mut self) -> Result<Statement, String> {
        self.consume(Token::Insert)?;
        self.consume(Token::Into)?;

        let table = match self.advance() {
            Token::Identifier(name) => name,
            _ => return Err("Expected table name".to_string()),
        };

        let columns = if *self.current_token() == Token::LeftParen {
            self.advance();
            let mut cols = Vec::new();

            loop {
                match self.current_token() {
                    Token::Identifier(name) => {
                        cols.push(name.clone());
                        self.advance();
                    }
                    _ => break,
                }

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

        self.consume(Token::Values)?;

        let values = self.parse_values()?;

        Ok(Statement::Insert(InsertStatement {
            table,
            columns,
            values,
        }))
    }

    fn parse_values(&mut self) -> Result<Vec<Vec<Value>>, String> {
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

    fn parse_value(&mut self) -> Result<Value, String> {
        match self.advance() {
            Token::Number(n) => Ok(Value::Integer(n)),
            Token::Float(f) => Ok(Value::Float(f)),
            Token::StringLiteral(s) => Ok(Value::Text(s)),
            _ => Err("Expected value".to_string()),
        }
    }

    fn parse_update(&mut self) -> Result<Statement, String> {
        self.consume(Token::Update)?;

        let table = match self.advance() {
            Token::Identifier(name) => name,
            _ => return Err("Expected table name".to_string()),
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

    fn parse_assignments(&mut self) -> Result<Vec<Assignment>, String> {
        let mut assignments = Vec::new();

        loop {
            let column = match self.advance() {
                Token::Identifier(name) => name,
                _ => return Err("Expected column name".to_string()),
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

    fn parse_delete(&mut self) -> Result<Statement, String> {
        self.consume(Token::Delete)?;
        self.consume(Token::From)?;

        let table = match self.advance() {
            Token::Identifier(name) => name,
            _ => return Err("Expected table name".to_string()),
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

    fn parse_create(&mut self) -> Result<Statement, String> {
        self.consume(Token::Create)?;
        self.consume(Token::Table)?;

        let name = match self.advance() {
            Token::Identifier(name) => name,
            _ => return Err("Expected table name".to_string()),
        };

        self.consume(Token::LeftParen)?;

        let columns = self.parse_column_definitions()?;

        self.consume(Token::RightParen)?;

        Ok(Statement::CreateTable(CreateTableStatement {
            name,
            columns,
        }))
    }

    fn parse_column_definitions(&mut self) -> Result<Vec<ColumnDefinition>, String> {
        let mut columns = Vec::new();

        loop {
            let name = match self.advance() {
                Token::Identifier(name) => name,
                _ => return Err("Expected column name".to_string()),
            };

            let data_type = self.parse_data_type()?;

            columns.push(ColumnDefinition {
                name,
                data_type,
                nullable: true,
            });

            if *self.current_token() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok(columns)
    }

    fn parse_data_type(&mut self) -> Result<DataType, String> {
        match self.advance() {
            Token::Identifier(name) => match name.to_uppercase().as_str() {
                "INT" | "INTEGER" => Ok(DataType::Integer),
                "FLOAT" | "REAL" | "DOUBLE" => Ok(DataType::Float),
                "TEXT" | "VARCHAR" | "STRING" => Ok(DataType::Text),
                "BOOL" | "BOOLEAN" => Ok(DataType::Boolean),
                _ => Err(format!("Unknown data type: {}", name)),
            },
            _ => Err("Expected data type".to_string()),
        }
    }

    fn parse_drop(&mut self) -> Result<Statement, String> {
        self.consume(Token::Drop)?;
        self.consume(Token::Table)?;

        let name = match self.advance() {
            Token::Identifier(name) => name,
            _ => return Err("Expected table name".to_string()),
        };

        Ok(Statement::DropTable(DropTableStatement { name }))
    }

    fn parse_expression(&mut self) -> Result<Expression, String> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<Expression, String> {
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

    fn parse_and(&mut self) -> Result<Expression, String> {
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

    fn parse_comparison(&mut self) -> Result<Expression, String> {
        let mut expr = self.parse_term()?;

        loop {
            let op = match self.current_token() {
                Token::Equal => BinaryOperator::Equal,
                Token::NotEqual => BinaryOperator::NotEqual,
                Token::LessThan => BinaryOperator::LessThan,
                Token::LessThanOrEqual => BinaryOperator::LessThanOrEqual,
                Token::GreaterThan => BinaryOperator::GreaterThan,
                Token::GreaterThanOrEqual => BinaryOperator::GreaterThanOrEqual,
                _ => break,
            };

            self.advance();
            let right = self.parse_term()?;
            expr = Expression::BinaryOp {
                left: Box::new(expr),
                op,
                right: Box::new(right),
            };
        }

        Ok(expr)
    }

    fn parse_term(&mut self) -> Result<Expression, String> {
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

    fn parse_factor(&mut self) -> Result<Expression, String> {
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

    fn parse_unary(&mut self) -> Result<Expression, String> {
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

    fn parse_primary(&mut self) -> Result<Expression, String> {
        match self.current_token().clone() {
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
                let expr = self.parse_expression()?;
                self.consume(Token::RightParen)?;
                Ok(expr)
            }
            _ => Err(format!(
                "Unexpected token in expression: {:?}",
                self.current_token()
            )),
        }
    }

    fn parse_order_by(&mut self) -> Result<Vec<OrderByExpr>, String> {
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
}

pub fn parse(tokens: Vec<Token>) -> Result<Statement, String> {
    let mut parser = Parser::new(tokens);
    parser.parse_statement()
}
