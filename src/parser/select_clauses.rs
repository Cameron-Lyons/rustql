use super::*;

impl Parser {
    pub(super) fn parse_with_select(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::With)?;
        let recursive = if *self.current_token() == Token::Recursive {
            self.advance();
            true
        } else {
            false
        };
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
            ctes.push(Cte {
                name,
                query,
                recursive,
            });
            if *self.current_token() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }
        self.parse_select_statement(ctes)
    }

    pub(super) fn parse_select_statement(
        &mut self,
        ctes: Vec<Cte>,
    ) -> Result<Statement, RustqlError> {
        let stmt = self.parse_select_inner(ctes)?;
        Ok(Statement::Select(stmt))
    }

    pub(super) fn parse_select_inner(
        &mut self,
        ctes: Vec<Cte>,
    ) -> Result<SelectStatement, RustqlError> {
        self.consume(Token::Select)?;

        let (distinct, distinct_on) = if *self.current_token() == Token::Distinct {
            self.advance();
            if *self.current_token() == Token::On {
                self.advance();
                self.consume(Token::LeftParen)?;
                let mut exprs = Vec::new();
                loop {
                    exprs.push(self.parse_expression()?);
                    if *self.current_token() == Token::Comma {
                        self.advance();
                    } else {
                        break;
                    }
                }
                self.consume(Token::RightParen)?;
                (true, Some(exprs))
            } else {
                (true, None)
            }
        } else {
            (false, None)
        };

        let columns = self.parse_columns()?;

        let mut from_values = None;
        let (table, from_subquery, from_alias, from_function) = if *self.current_token()
            == Token::From
        {
            self.advance();
            if *self.current_token() == Token::LeftParen
                && self.current + 1 < self.tokens.len()
                && self.tokens[self.current + 1] == Token::Values
            {
                self.advance();
                self.advance();
                let mut value_rows = Vec::new();
                loop {
                    self.consume(Token::LeftParen)?;
                    let mut row = Vec::new();
                    loop {
                        row.push(self.parse_expression()?);
                        if *self.current_token() == Token::Comma {
                            self.advance();
                        } else {
                            break;
                        }
                    }
                    self.consume(Token::RightParen)?;
                    value_rows.push(row);
                    if *self.current_token() == Token::Comma {
                        self.advance();
                    } else {
                        break;
                    }
                }
                self.consume(Token::RightParen)?;
                if *self.current_token() == Token::As {
                    self.advance();
                }
                let table_alias = match self.advance() {
                    Token::Identifier(name) => name,
                    _ => {
                        return Err(RustqlError::ParseError(
                            "Expected alias for VALUES".to_string(),
                        ));
                    }
                };
                let mut col_aliases = Vec::new();
                if *self.current_token() == Token::LeftParen {
                    self.advance();
                    loop {
                        match self.advance() {
                            Token::Identifier(name) => col_aliases.push(name),
                            _ => {
                                return Err(RustqlError::ParseError(
                                    "Expected column alias".to_string(),
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
                }
                from_values = Some((value_rows, table_alias.clone(), col_aliases));
                (table_alias, None, None, None)
            } else if *self.current_token() == Token::LeftParen
                && self.current + 1 < self.tokens.len()
                && self.tokens[self.current + 1] == Token::Select
            {
                self.advance();
                let subquery = self.parse_select_inner(Vec::new())?;
                self.consume(Token::RightParen)?;
                if *self.current_token() == Token::As {
                    self.advance();
                }
                let alias = match self.advance() {
                    Token::Identifier(name) => name,
                    _ => {
                        return Err(RustqlError::ParseError(
                            "Expected alias for derived table".to_string(),
                        ));
                    }
                };
                (alias.clone(), Some((Box::new(subquery), alias)), None, None)
            } else if *self.current_token() == Token::GenerateSeries {
                self.advance();
                self.consume(Token::LeftParen)?;
                let mut args = Vec::new();
                loop {
                    args.push(self.parse_expression()?);
                    if *self.current_token() == Token::Comma {
                        self.advance();
                    } else {
                        break;
                    }
                }
                self.consume(Token::RightParen)?;
                let alias = if self.is_alias_token() {
                    if *self.current_token() == Token::As {
                        self.advance();
                    }
                    match self.advance() {
                        Token::Identifier(a) => Some(a),
                        _ => {
                            return Err(RustqlError::ParseError("Expected alias name".to_string()));
                        }
                    }
                } else {
                    None
                };
                let tf = TableFunction {
                    name: "generate_series".to_string(),
                    args,
                    alias: alias.clone(),
                };
                (alias.unwrap_or_default(), None, None, Some(tf))
            } else {
                let name = match self.advance() {
                    Token::Identifier(name) => name,
                    _ => return Err(RustqlError::ParseError("Expected table name".to_string())),
                };
                let alias = if self.is_alias_token() {
                    if *self.current_token() == Token::As {
                        self.advance();
                    }
                    match self.advance() {
                        Token::Identifier(a) => Some(a),
                        _ => {
                            return Err(RustqlError::ParseError("Expected alias name".to_string()));
                        }
                    }
                } else {
                    None
                };
                (name, None, alias, None)
            }
        } else {
            ("".to_string(), None, None, None)
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

                let lateral = if *self.current_token() == Token::Lateral {
                    self.advance();
                    true
                } else {
                    false
                };

                let (join_table, join_subquery) = if *self.current_token() == Token::LeftParen
                    && self.current + 1 < self.tokens.len()
                    && self.tokens[self.current + 1] == Token::Select
                {
                    self.advance();
                    let subquery = self.parse_select_inner(Vec::new())?;
                    self.consume(Token::RightParen)?;
                    if *self.current_token() == Token::As {
                        self.advance();
                    }
                    let alias = match self.advance() {
                        Token::Identifier(name) => name,
                        _ => {
                            return Err(RustqlError::ParseError(
                                "Expected alias for subquery in JOIN".to_string(),
                            ));
                        }
                    };
                    (alias.clone(), Some((Box::new(subquery), alias)))
                } else {
                    let name = match self.advance() {
                        Token::Identifier(name) => name,
                        _ => {
                            return Err(RustqlError::ParseError(
                                "Expected table name after JOIN".to_string(),
                            ));
                        }
                    };
                    (name, None)
                };

                let join_alias = if join_subquery.is_none() && self.is_alias_token() {
                    if *self.current_token() == Token::As {
                        self.advance();
                    }
                    match self.advance() {
                        Token::Identifier(a) => Some(a),
                        _ => {
                            return Err(RustqlError::ParseError("Expected alias name".to_string()));
                        }
                    }
                } else {
                    None
                };

                let (on_expr, using_cols) =
                    if matches!(join_type, JoinType::Cross | JoinType::Natural)
                        || (lateral && join_subquery.is_some())
                    {
                        if *self.current_token() == Token::On {
                            self.advance();
                            (Some(self.parse_expression()?), None)
                        } else {
                            (None, None)
                        }
                    } else if *self.current_token() == Token::Using {
                        self.advance();
                        self.consume(Token::LeftParen)?;
                        let mut cols = Vec::new();
                        loop {
                            match self.advance() {
                                Token::Identifier(name) => cols.push(name),
                                _ => {
                                    return Err(RustqlError::ParseError(
                                        "Expected column name in USING clause".to_string(),
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
                        (None, Some(cols))
                    } else {
                        self.consume(Token::On)?;
                        (Some(self.parse_expression()?), None)
                    };

                joins.push(Join {
                    join_type,
                    table: join_table,
                    table_alias: join_alias,
                    on: on_expr,
                    using_columns: using_cols,
                    lateral,
                    subquery: join_subquery,
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

        let window_definitions = if *self.current_token() == Token::Window {
            self.advance();
            let mut defs = Vec::new();
            loop {
                let name = match self.advance() {
                    Token::Identifier(n) => n,
                    _ => {
                        return Err(RustqlError::ParseError("Expected window name".to_string()));
                    }
                };
                self.consume(Token::As)?;
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
                let frame = if *self.current_token() == Token::Rows
                    || *self.current_token() == Token::RangeFrame
                {
                    let mode = if *self.current_token() == Token::Rows {
                        self.advance();
                        WindowFrameMode::Rows
                    } else {
                        self.advance();
                        WindowFrameMode::Range
                    };
                    self.consume(Token::Between)?;
                    let start = self.parse_window_frame_bound()?;
                    self.consume(Token::And)?;
                    let end = self.parse_window_frame_bound()?;
                    Some(WindowFrame { mode, start, end })
                } else {
                    None
                };
                self.consume(Token::RightParen)?;
                defs.push(WindowDefinition {
                    name,
                    partition_by,
                    order_by,
                    frame,
                });
                if *self.current_token() == Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            defs
        } else {
            Vec::new()
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

        let fetch = if *self.current_token() == Token::Fetch {
            self.advance();
            if *self.current_token() == Token::First || *self.current_token() == Token::Next {
                self.advance();
            }
            let count = match self.advance() {
                Token::Number(n) => n as usize,
                _ => {
                    return Err(RustqlError::ParseError(
                        "Expected number after FETCH FIRST/NEXT".to_string(),
                    ));
                }
            };
            if *self.current_token() == Token::Row || *self.current_token() == Token::Rows {
                self.advance();
            }
            let with_ties = if *self.current_token() == Token::With {
                self.advance();
                self.consume(Token::Ties)?;
                true
            } else {
                if *self.current_token() == Token::Only {
                    self.advance();
                }
                false
            };
            Some(FetchClause { count, with_ties })
        } else {
            None
        };

        let set_op = if matches!(
            self.current_token(),
            Token::Union | Token::Intersect | Token::Except
        ) {
            let op_token = self.advance();
            let is_all = *self.current_token() == Token::All;
            if is_all {
                self.advance();
            }
            let set_op_type = match op_token {
                Token::Union => {
                    if is_all {
                        crate::ast::SetOperation::UnionAll
                    } else {
                        crate::ast::SetOperation::Union
                    }
                }
                Token::Intersect => {
                    if is_all {
                        crate::ast::SetOperation::IntersectAll
                    } else {
                        crate::ast::SetOperation::Intersect
                    }
                }
                Token::Except => {
                    if is_all {
                        crate::ast::SetOperation::ExceptAll
                    } else {
                        crate::ast::SetOperation::Except
                    }
                }
                token => {
                    return Err(RustqlError::ParseError(format!(
                        "Expected set operation, found {:?}",
                        token
                    )));
                }
            };
            let other_stmt = self.parse_select_inner(Vec::new())?;
            Some((set_op_type, Box::new(other_stmt)))
        } else {
            None
        };

        Ok(SelectStatement {
            ctes,
            distinct,
            distinct_on,
            columns,
            from: table,
            from_alias,
            from_subquery,
            from_function,
            joins,
            where_clause,
            group_by,
            having,
            order_by,
            limit,
            offset,
            fetch,
            set_op,
            window_definitions,
            from_values,
        })
    }

    pub(super) fn parse_columns(&mut self) -> Result<Vec<Column>, RustqlError> {
        let mut columns = Vec::new();

        if *self.current_token() == Token::Star {
            self.advance();
            columns.push(Column::All);
        } else {
            loop {
                let column = match self.current_token() {
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
                    Token::RowNumber
                    | Token::Rank
                    | Token::DenseRank
                    | Token::Lag
                    | Token::Lead
                    | Token::Ntile
                    | Token::FirstValue
                    | Token::LastValue
                    | Token::NthValue
                    | Token::PercentRank
                    | Token::CumeDist => {
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
                    Token::Cast => {
                        let expr = self.parse_cast_expression()?;
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
                    | Token::RegexpReplace => {
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
                    Token::Left | Token::Right => {
                        if self.current + 1 < self.tokens.len()
                            && self.tokens[self.current + 1] == Token::LeftParen
                        {
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
                        } else {
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
                                    break;
                                }
                            }
                        }
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

    pub(super) fn parse_window_function_column(&mut self) -> Result<Column, RustqlError> {
        let func_type = match self.advance() {
            Token::RowNumber => WindowFunctionType::RowNumber,
            Token::Rank => WindowFunctionType::Rank,
            Token::DenseRank => WindowFunctionType::DenseRank,
            Token::Lag => WindowFunctionType::Lag,
            Token::Lead => WindowFunctionType::Lead,
            Token::Ntile => WindowFunctionType::Ntile,
            Token::FirstValue => WindowFunctionType::FirstValue,
            Token::LastValue => WindowFunctionType::LastValue,
            Token::NthValue => WindowFunctionType::NthValue,
            Token::PercentRank => WindowFunctionType::PercentRank,
            Token::CumeDist => WindowFunctionType::CumeDist,
            _ => {
                return Err(RustqlError::ParseError(
                    "Expected window function".to_string(),
                ));
            }
        };

        self.consume(Token::LeftParen)?;
        let mut args = Vec::new();
        let takes_args = matches!(
            func_type,
            WindowFunctionType::Lag
                | WindowFunctionType::Lead
                | WindowFunctionType::Ntile
                | WindowFunctionType::FirstValue
                | WindowFunctionType::LastValue
                | WindowFunctionType::NthValue
        );
        if takes_args && *self.current_token() != Token::RightParen {
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

        let (partition_by, order_by, frame) = self.parse_over_clause()?;

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
                args,
                partition_by,
                order_by,
                frame,
            },
            alias,
        })
    }

    pub(super) fn parse_over_clause(&mut self) -> ParseOverClauseResult {
        self.consume(Token::Over)?;
        if let Token::Identifier(ref _name) = *self.current_token() {
            let window_name = match self.advance() {
                Token::Identifier(n) => n,
                token => {
                    return Err(RustqlError::ParseError(format!(
                        "Expected window name, found {:?}",
                        token
                    )));
                }
            };
            return Ok((
                vec![Expression::Column(format!("__window_ref:{}", window_name))],
                Vec::new(),
                None,
            ));
        }
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

        let frame =
            if *self.current_token() == Token::Rows || *self.current_token() == Token::RangeFrame {
                let mode = if *self.current_token() == Token::Rows {
                    self.advance();
                    WindowFrameMode::Rows
                } else {
                    self.advance();
                    WindowFrameMode::Range
                };
                self.consume(Token::Between)?;
                let start = self.parse_window_frame_bound()?;
                self.consume(Token::And)?;
                let end = self.parse_window_frame_bound()?;
                Some(WindowFrame { mode, start, end })
            } else {
                None
            };

        self.consume(Token::RightParen)?;
        Ok((partition_by, order_by, frame))
    }

    pub(super) fn parse_window_frame_bound(&mut self) -> Result<WindowFrameBound, RustqlError> {
        if *self.current_token() == Token::Unbounded {
            self.advance();
            if *self.current_token() == Token::Preceding {
                self.advance();
                Ok(WindowFrameBound::UnboundedPreceding)
            } else if *self.current_token() == Token::Following {
                self.advance();
                Ok(WindowFrameBound::UnboundedFollowing)
            } else {
                Err(RustqlError::ParseError(
                    "Expected PRECEDING or FOLLOWING after UNBOUNDED".to_string(),
                ))
            }
        } else if *self.current_token() == Token::Current {
            self.advance();
            self.consume(Token::Rows)?;
            Ok(WindowFrameBound::CurrentRow)
        } else if let Token::Number(n) = self.current_token().clone() {
            self.advance();
            if *self.current_token() == Token::Preceding {
                self.advance();
                Ok(WindowFrameBound::Preceding(n as usize))
            } else if *self.current_token() == Token::Following {
                self.advance();
                Ok(WindowFrameBound::Following(n as usize))
            } else {
                Err(RustqlError::ParseError(
                    "Expected PRECEDING or FOLLOWING after number".to_string(),
                ))
            }
        } else {
            Err(RustqlError::ParseError(
                "Expected window frame bound".to_string(),
            ))
        }
    }

    pub(super) fn parse_column_expression(
        &mut self,
    ) -> Result<(Expression, Option<String>), RustqlError> {
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

    pub(super) fn parse_group_by(&mut self) -> Result<GroupByClause, RustqlError> {
        if *self.current_token() == Token::Rollup {
            self.advance();
            self.consume(Token::LeftParen)?;
            let mut exprs = Vec::new();
            loop {
                exprs.push(self.parse_expression()?);
                if *self.current_token() == Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            self.consume(Token::RightParen)?;
            return Ok(GroupByClause::Rollup(exprs));
        }

        if *self.current_token() == Token::Cube {
            self.advance();
            self.consume(Token::LeftParen)?;
            let mut exprs = Vec::new();
            loop {
                exprs.push(self.parse_expression()?);
                if *self.current_token() == Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            self.consume(Token::RightParen)?;
            return Ok(GroupByClause::Cube(exprs));
        }

        if *self.current_token() == Token::Grouping {
            self.advance();
            self.consume(Token::Sets)?;
            self.consume(Token::LeftParen)?;
            let mut sets = Vec::new();
            loop {
                self.consume(Token::LeftParen)?;
                let mut exprs = Vec::new();
                if *self.current_token() != Token::RightParen {
                    loop {
                        exprs.push(self.parse_expression()?);
                        if *self.current_token() == Token::Comma {
                            self.advance();
                        } else {
                            break;
                        }
                    }
                }
                self.consume(Token::RightParen)?;
                sets.push(exprs);
                if *self.current_token() == Token::Comma {
                    self.advance();
                } else {
                    break;
                }
            }
            self.consume(Token::RightParen)?;
            return Ok(GroupByClause::GroupingSets(sets));
        }

        let mut exprs = Vec::new();
        loop {
            exprs.push(self.parse_expression()?);
            if *self.current_token() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }
        Ok(GroupByClause::Simple(exprs))
    }

    pub(super) fn is_alias_token(&self) -> bool {
        if *self.current_token() == Token::As {
            return true;
        }
        if let Token::Identifier(_) = self.current_token() {
            !matches!(
                self.current_token(),
                Token::Where
                    | Token::Join
                    | Token::Inner
                    | Token::Left
                    | Token::Right
                    | Token::Full
                    | Token::Cross
                    | Token::Natural
                    | Token::On
                    | Token::Group
                    | Token::Having
                    | Token::Order
                    | Token::Limit
                    | Token::Offset
                    | Token::Union
                    | Token::Intersect
                    | Token::Except
                    | Token::Set
                    | Token::From
                    | Token::Using
                    | Token::Returning
                    | Token::Fetch
            )
        } else {
            false
        }
    }

    pub(super) fn parse_order_by(&mut self) -> Result<Vec<OrderByExpr>, RustqlError> {
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
