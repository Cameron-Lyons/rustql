use crate::ast::*;
use crate::error::RustqlError;
use crate::lexer::{SourceSpan, SpannedToken, Token};

type ParseOverClauseResult =
    Result<(Vec<Expression>, Vec<OrderByExpr>, Option<WindowFrame>), RustqlError>;

pub struct Parser {
    tokens: Vec<Token>,
    spans: Vec<Option<SourceSpan>>,
    current: usize,
}

impl Parser {
    fn new(tokens: Vec<Token>) -> Self {
        let spans = vec![None; tokens.len()];
        Parser {
            tokens,
            spans,
            current: 0,
        }
    }

    fn new_spanned(tokens: Vec<SpannedToken>) -> Self {
        let (tokens, spans): (Vec<_>, Vec<_>) = tokens
            .into_iter()
            .map(|spanned| (spanned.token, Some(spanned.span)))
            .unzip();
        Parser {
            tokens,
            spans,
            current: 0,
        }
    }

    fn current_token(&self) -> &Token {
        self.tokens.get(self.current).unwrap_or(&Token::Eof)
    }

    fn current_span(&self) -> Option<SourceSpan> {
        self.spans.get(self.current).copied().flatten()
    }

    fn with_current_location(&self, err: RustqlError) -> RustqlError {
        match (err, self.current_span()) {
            (RustqlError::ParseError(message), Some(span)) if !message.contains(" at line ") => {
                RustqlError::ParseError(format!(
                    "{} at line {}, column {}",
                    message, span.start.line, span.start.column
                ))
            }
            (err, _) => err,
        }
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
            Token::Truncate => self.parse_truncate(),
            Token::Merge => self.parse_merge(),
            Token::Do => self.parse_do_block(),
            _ => Err(RustqlError::ParseError(format!(
                "Unexpected token: {:?}",
                self.current_token()
            ))),
        }
    }

    fn finish_single_statement(&mut self, statement: Statement) -> Result<Statement, RustqlError> {
        while *self.current_token() == Token::Semicolon {
            self.advance();
        }

        if *self.current_token() != Token::Eof {
            return Err(RustqlError::ParseError(format!(
                "Unexpected trailing token: {:?}",
                self.current_token()
            )));
        }

        Ok(statement)
    }

    fn parse_with_select(&mut self) -> Result<Statement, RustqlError> {
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

    fn parse_select_statement(&mut self, ctes: Vec<Cte>) -> Result<Statement, RustqlError> {
        let stmt = self.parse_select_inner(ctes)?;
        Ok(Statement::Select(stmt))
    }

    fn parse_select_inner(&mut self, ctes: Vec<Cte>) -> Result<SelectStatement, RustqlError> {
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

    fn parse_columns(&mut self) -> Result<Vec<Column>, RustqlError> {
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

    fn parse_window_function_column(&mut self) -> Result<Column, RustqlError> {
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

    fn parse_over_clause(&mut self) -> ParseOverClauseResult {
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

    fn parse_window_frame_bound(&mut self) -> Result<WindowFrameBound, RustqlError> {
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

    fn parse_cast_expression(&mut self) -> Result<Expression, RustqlError> {
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

    fn parse_scalar_function(&mut self) -> Result<Expression, RustqlError> {
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

    fn parse_aggregate_function(&mut self) -> Result<Column, RustqlError> {
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

    fn parse_group_by(&mut self) -> Result<GroupByClause, RustqlError> {
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

    fn is_alias_token(&self) -> bool {
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

    fn parse_returning(&mut self) -> Result<Option<Vec<Column>>, RustqlError> {
        if *self.current_token() == Token::Returning {
            self.advance();
            let cols = self.parse_columns()?;
            Ok(Some(cols))
        } else {
            Ok(None)
        }
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
            let returning = self.parse_returning()?;
            return Ok(Statement::Insert(InsertStatement {
                table,
                columns,
                values: Vec::new(),
                source_query: Some(Box::new(source_query)),
                on_conflict: None,
                returning,
            }));
        }

        self.consume(Token::Values)?;

        let values = self.parse_values()?;

        let on_conflict = if *self.current_token() == Token::On
            && self.current + 1 < self.tokens.len()
            && self.tokens[self.current + 1] == Token::Conflict
        {
            self.advance();
            self.advance();
            self.consume(Token::LeftParen)?;
            let mut conflict_columns = Vec::new();
            loop {
                match self.advance() {
                    Token::Identifier(name) => conflict_columns.push(name),
                    _ => {
                        return Err(RustqlError::ParseError(
                            "Expected column name in ON CONFLICT".to_string(),
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
            self.consume(Token::Do)?;
            let action = if *self.current_token() == Token::Nothing {
                self.advance();
                OnConflictAction::DoNothing
            } else if *self.current_token() == Token::Update {
                self.advance();
                self.consume(Token::Set)?;
                let assignments = self.parse_assignments()?;
                OnConflictAction::DoUpdate { assignments }
            } else {
                return Err(RustqlError::ParseError(
                    "Expected NOTHING or UPDATE after DO".to_string(),
                ));
            };
            Some(OnConflictClause {
                columns: conflict_columns,
                action,
            })
        } else {
            None
        };

        let returning = self.parse_returning()?;

        Ok(Statement::Insert(InsertStatement {
            table,
            columns,
            values,
            source_query: None,
            on_conflict,
            returning,
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

        let from = if *self.current_token() == Token::From {
            self.advance();
            let from_table = match self.advance() {
                Token::Identifier(name) => name,
                _ => {
                    return Err(RustqlError::ParseError(
                        "Expected table name after FROM".to_string(),
                    ));
                }
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
                    self.consume(Token::On)?;
                    let on_expr = self.parse_expression()?;
                    joins.push(Join {
                        join_type,
                        table: join_table,
                        table_alias: None,
                        on: Some(on_expr),
                        using_columns: None,
                        lateral: false,
                        subquery: None,
                    });
                } else {
                    break;
                }
            }
            Some(UpdateFrom {
                table: from_table,
                joins,
            })
        } else {
            None
        };

        let where_clause = if *self.current_token() == Token::Where {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        let returning = self.parse_returning()?;

        Ok(Statement::Update(UpdateStatement {
            table,
            assignments,
            where_clause,
            from,
            returning,
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

            let value = self.parse_expression()?;

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

        let using = if *self.current_token() == Token::Using {
            self.advance();
            let using_table = match self.advance() {
                Token::Identifier(name) => name,
                _ => {
                    return Err(RustqlError::ParseError(
                        "Expected table name after USING".to_string(),
                    ));
                }
            };
            let using_alias = if self.is_alias_token()
                && !matches!(
                    self.current_token(),
                    Token::Where
                        | Token::Join
                        | Token::Inner
                        | Token::Left
                        | Token::Right
                        | Token::Full
                ) {
                if *self.current_token() == Token::As {
                    self.advance();
                }
                match self.advance() {
                    Token::Identifier(a) => Some(a),
                    _ => None,
                }
            } else {
                None
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
                    self.consume(Token::On)?;
                    let on_expr = self.parse_expression()?;
                    joins.push(Join {
                        join_type,
                        table: join_table,
                        table_alias: None,
                        on: Some(on_expr),
                        using_columns: None,
                        lateral: false,
                        subquery: None,
                    });
                } else {
                    break;
                }
            }
            Some(DeleteUsing {
                table: using_table,
                alias: using_alias,
                joins,
            })
        } else {
            None
        };

        let where_clause = if *self.current_token() == Token::Where {
            self.advance();
            Some(self.parse_expression()?)
        } else {
            None
        };

        let returning = self.parse_returning()?;

        Ok(Statement::Delete(DeleteStatement {
            table,
            where_clause,
            using,
            returning,
        }))
    }

    fn parse_create(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Create)?;

        match self.current_token() {
            Token::Table => {
                self.advance();
                let if_not_exists = if *self.current_token() == Token::If {
                    self.advance();
                    self.consume(Token::Not)?;
                    self.consume(Token::Exists)?;
                    true
                } else {
                    false
                };
                let name = match self.advance() {
                    Token::Identifier(name) => name,
                    _ => return Err(RustqlError::ParseError("Expected table name".to_string())),
                };

                if *self.current_token() == Token::As {
                    self.advance();
                    let query_stmt = self.parse_select_statement(Vec::new())?;
                    let query = if let Statement::Select(s) = query_stmt {
                        s
                    } else {
                        return Err(RustqlError::ParseError(
                            "Expected SELECT after AS".to_string(),
                        ));
                    };
                    return Ok(Statement::CreateTable(CreateTableStatement {
                        name,
                        columns: Vec::new(),
                        constraints: Vec::new(),
                        as_query: Some(Box::new(query)),
                        if_not_exists,
                    }));
                }

                self.consume(Token::LeftParen)?;

                let (columns, constraints) = self.parse_column_definitions()?;

                self.consume(Token::RightParen)?;

                Ok(Statement::CreateTable(CreateTableStatement {
                    name,
                    columns,
                    constraints,
                    as_query: None,
                    if_not_exists,
                }))
            }
            Token::Index => {
                self.advance();
                let if_not_exists = if *self.current_token() == Token::If {
                    self.advance();
                    self.consume(Token::Not)?;
                    self.consume(Token::Exists)?;
                    true
                } else {
                    false
                };
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

                let where_clause = if *self.current_token() == Token::Where {
                    self.advance();
                    Some(self.parse_expression()?)
                } else {
                    None
                };

                Ok(Statement::CreateIndex(CreateIndexStatement {
                    name: index_name,
                    table,
                    columns,
                    if_not_exists,
                    where_clause,
                }))
            }
            Token::View => {
                self.advance();
                let name = match self.advance() {
                    Token::Identifier(name) => name,
                    _ => return Err(RustqlError::ParseError("Expected view name".to_string())),
                };
                self.consume(Token::As)?;
                let remaining_start = self.current;
                let mut query_sql_parts = Vec::new();
                for tok in &self.tokens[remaining_start..] {
                    if matches!(tok, Token::Eof | Token::Semicolon) {
                        break;
                    }
                    query_sql_parts.push(token_to_sql(tok));
                }
                let query_sql = query_sql_parts.join(" ");
                let _query_stmt = self.parse_select_statement(Vec::new())?;
                Ok(Statement::CreateView { name, query_sql })
            }
            _ => Err(RustqlError::ParseError(
                "Expected TABLE, INDEX, or VIEW after CREATE".to_string(),
            )),
        }
    }

    fn parse_column_definitions(
        &mut self,
    ) -> Result<(Vec<ColumnDefinition>, Vec<crate::ast::TableConstraint>), RustqlError> {
        let mut columns = Vec::new();
        let mut table_constraints: Vec<crate::ast::TableConstraint> = Vec::new();

        loop {
            if *self.current_token() == Token::Primary {
                self.advance();
                self.consume(Token::Key)?;
                self.consume(Token::LeftParen)?;
                let mut cols = Vec::new();
                loop {
                    match self.advance() {
                        Token::Identifier(name) => cols.push(name),
                        _ => {
                            return Err(RustqlError::ParseError(
                                "Expected column name in PRIMARY KEY".to_string(),
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
                table_constraints.push(crate::ast::TableConstraint::PrimaryKey {
                    name: None,
                    columns: cols,
                });
                if *self.current_token() == Token::Comma {
                    self.advance();
                    continue;
                } else {
                    break;
                }
            }

            if *self.current_token() == Token::Unique {
                self.advance();
                self.consume(Token::LeftParen)?;
                let mut cols = Vec::new();
                loop {
                    match self.advance() {
                        Token::Identifier(name) => cols.push(name),
                        _ => {
                            return Err(RustqlError::ParseError(
                                "Expected column name in UNIQUE".to_string(),
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
                table_constraints.push(crate::ast::TableConstraint::Unique {
                    name: None,
                    columns: cols,
                });
                if *self.current_token() == Token::Comma {
                    self.advance();
                    continue;
                } else {
                    break;
                }
            }

            if *self.current_token() == Token::Constraint {
                self.advance();
                let constraint_name = match self.advance() {
                    Token::Identifier(name) => name,
                    _ => {
                        return Err(RustqlError::ParseError(
                            "Expected constraint name after CONSTRAINT".to_string(),
                        ));
                    }
                };
                if *self.current_token() == Token::Primary {
                    self.advance();
                    self.consume(Token::Key)?;
                    self.consume(Token::LeftParen)?;
                    let mut cols = Vec::new();
                    loop {
                        match self.advance() {
                            Token::Identifier(name) => cols.push(name),
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
                    table_constraints.push(crate::ast::TableConstraint::PrimaryKey {
                        name: Some(constraint_name),
                        columns: cols,
                    });
                } else if *self.current_token() == Token::Unique {
                    self.advance();
                    self.consume(Token::LeftParen)?;
                    let mut cols = Vec::new();
                    loop {
                        match self.advance() {
                            Token::Identifier(name) => cols.push(name),
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
                    table_constraints.push(crate::ast::TableConstraint::Unique {
                        name: Some(constraint_name),
                        columns: cols,
                    });
                } else {
                    return Err(RustqlError::ParseError(
                        "Expected PRIMARY KEY or UNIQUE after CONSTRAINT name".to_string(),
                    ));
                }
                if *self.current_token() == Token::Comma {
                    self.advance();
                    continue;
                } else {
                    break;
                }
            }

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

            let generated = if *self.current_token() == Token::Generated {
                self.advance();
                let always = if *self.current_token() == Token::Always {
                    self.advance();
                    true
                } else {
                    false
                };
                self.consume(Token::As)?;
                self.consume(Token::LeftParen)?;
                let mut depth = 1;
                let mut expr_sql = String::new();
                while depth > 0 {
                    let tok = self.advance();
                    match tok {
                        Token::LeftParen => {
                            depth += 1;
                            expr_sql.push('(');
                        }
                        Token::RightParen => {
                            depth -= 1;
                            if depth > 0 {
                                expr_sql.push(')');
                            }
                        }
                        _ => {
                            if !expr_sql.is_empty() {
                                expr_sql.push(' ');
                            }
                            expr_sql.push_str(&token_to_string(&tok));
                        }
                    }
                }
                if *self.current_token() == Token::Stored {
                    self.advance();
                }
                Some(GeneratedColumn { expr_sql, always })
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
                generated,
            });

            if *self.current_token() == Token::Comma {
                self.advance();
            } else {
                break;
            }
        }

        Ok((columns, table_constraints))
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
                let if_exists = if *self.current_token() == Token::If {
                    self.advance();
                    self.consume(Token::Exists)?;
                    true
                } else {
                    false
                };
                let name = match self.advance() {
                    Token::Identifier(name) => name,
                    _ => return Err(RustqlError::ParseError("Expected table name".to_string())),
                };

                Ok(Statement::DropTable(DropTableStatement { name, if_exists }))
            }
            Token::Index => {
                self.advance();
                let if_exists = if *self.current_token() == Token::If {
                    self.advance();
                    self.consume(Token::Exists)?;
                    true
                } else {
                    false
                };
                let name = match self.advance() {
                    Token::Identifier(name) => name,
                    _ => return Err(RustqlError::ParseError("Expected index name".to_string())),
                };

                Ok(Statement::DropIndex(DropIndexStatement { name, if_exists }))
            }
            Token::View => {
                self.advance();
                let if_exists = if *self.current_token() == Token::If {
                    self.advance();
                    self.consume(Token::Exists)?;
                    true
                } else {
                    false
                };
                let name = match self.advance() {
                    Token::Identifier(name) => name,
                    _ => return Err(RustqlError::ParseError("Expected view name".to_string())),
                };
                Ok(Statement::DropView { name, if_exists })
            }
            _ => Err(RustqlError::ParseError(
                "Expected TABLE, INDEX, or VIEW after DROP".to_string(),
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
                match self.current_token() {
                    Token::Column => {
                        self.advance();
                        let name = match self.advance() {
                            Token::Identifier(n) => n,
                            _ => {
                                return Err(RustqlError::ParseError(
                                    "Expected column name".to_string(),
                                ));
                            }
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
                            generated: None,
                        })
                    }
                    Token::Constraint => {
                        self.advance();
                        let constraint_name = match self.advance() {
                            Token::Identifier(n) => n,
                            _ => {
                                return Err(RustqlError::ParseError(
                                    "Expected constraint name".to_string(),
                                ));
                            }
                        };
                        if *self.current_token() == Token::Primary {
                            self.advance();
                            self.consume(Token::Key)?;
                            self.consume(Token::LeftParen)?;
                            let mut cols = Vec::new();
                            loop {
                                match self.advance() {
                                    Token::Identifier(name) => cols.push(name),
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
                            AlterOperation::AddConstraint(crate::ast::TableConstraint::PrimaryKey {
                                name: Some(constraint_name),
                                columns: cols,
                            })
                        } else if *self.current_token() == Token::Unique {
                            self.advance();
                            self.consume(Token::LeftParen)?;
                            let mut cols = Vec::new();
                            loop {
                                match self.advance() {
                                    Token::Identifier(name) => cols.push(name),
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
                            AlterOperation::AddConstraint(crate::ast::TableConstraint::Unique {
                                name: Some(constraint_name),
                                columns: cols,
                            })
                        } else {
                            return Err(RustqlError::ParseError(
                                "Expected PRIMARY KEY or UNIQUE after constraint name".to_string(),
                            ));
                        }
                    }
                    Token::Primary => {
                        self.advance();
                        self.consume(Token::Key)?;
                        self.consume(Token::LeftParen)?;
                        let mut cols = Vec::new();
                        loop {
                            match self.advance() {
                                Token::Identifier(name) => cols.push(name),
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
                        AlterOperation::AddConstraint(crate::ast::TableConstraint::PrimaryKey {
                            name: None,
                            columns: cols,
                        })
                    }
                    Token::Unique => {
                        self.advance();
                        self.consume(Token::LeftParen)?;
                        let mut cols = Vec::new();
                        loop {
                            match self.advance() {
                                Token::Identifier(name) => cols.push(name),
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
                        AlterOperation::AddConstraint(crate::ast::TableConstraint::Unique {
                            name: None,
                            columns: cols,
                        })
                    }
                    _ => {
                        let name = match self.advance() {
                            Token::Identifier(n) => n,
                            _ => {
                                return Err(RustqlError::ParseError(
                                    "Expected COLUMN, CONSTRAINT, PRIMARY, or UNIQUE after ADD"
                                        .to_string(),
                                ));
                            }
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
                            generated: None,
                        })
                    }
                }
            }
            Token::Drop => {
                self.advance();
                if *self.current_token() == Token::Constraint {
                    self.advance();
                    match self.advance() {
                        Token::Identifier(name) => AlterOperation::DropConstraint(name),
                        _ => {
                            return Err(RustqlError::ParseError(
                                "Expected constraint name after DROP CONSTRAINT".to_string(),
                            ));
                        }
                    }
                } else {
                    if *self.current_token() == Token::Column {
                        self.advance();
                    }
                    match self.advance() {
                        Token::Identifier(name) => AlterOperation::DropColumn(name),
                        _ => {
                            return Err(RustqlError::ParseError(
                                "Expected column name".to_string(),
                            ));
                        }
                    }
                }
            }
            Token::Rename => {
                self.advance();
                if *self.current_token() == Token::To {
                    self.advance();
                    let new_name = match self.advance() {
                        Token::Identifier(n) => n,
                        _ => {
                            return Err(RustqlError::ParseError(
                                "Expected new table name after RENAME TO".to_string(),
                            ));
                        }
                    };
                    AlterOperation::RenameTable(new_name)
                } else {
                    if *self.current_token() == Token::Column {
                        self.advance();
                    }
                    let old = match self.advance() {
                        Token::Identifier(n) => n,
                        _ => {
                            return Err(RustqlError::ParseError(
                                "Expected column name".to_string(),
                            ));
                        }
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

    fn try_parse_any_all(
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

    fn parse_comparison(&mut self) -> Result<Expression, RustqlError> {
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

    fn parse_term(&mut self) -> Result<Expression, RustqlError> {
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

    fn parse_primary_inner(&mut self) -> Result<Expression, RustqlError> {
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
        let analyze = if *self.current_token() == Token::Analyze {
            self.advance();
            true
        } else {
            false
        };
        let select_stmt = if *self.current_token() == Token::With {
            self.parse_with_select()?
        } else {
            self.parse_select_statement(Vec::new())?
        };
        if let Statement::Select(select_stmt) = select_stmt {
            if analyze {
                Ok(Statement::ExplainAnalyze(select_stmt))
            } else {
                Ok(Statement::Explain(select_stmt))
            }
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

    fn parse_truncate(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Truncate)?;
        if *self.current_token() == Token::Table {
            self.advance();
        }
        let table_name = match self.advance() {
            Token::Identifier(name) => name,
            _ => {
                return Err(RustqlError::ParseError(
                    "Expected table name after TRUNCATE".to_string(),
                ));
            }
        };
        Ok(Statement::TruncateTable { table_name })
    }

    fn parse_merge(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Merge)?;
        self.consume(Token::Into)?;
        let target_table = match self.advance() {
            Token::Identifier(name) => name,
            _ => {
                return Err(RustqlError::ParseError(
                    "Expected target table name after MERGE INTO".to_string(),
                ));
            }
        };
        self.consume(Token::Using)?;
        let source = if *self.current_token() == Token::LeftParen {
            self.advance();
            let query = self.parse_select_inner(Vec::new())?;
            self.consume(Token::RightParen)?;
            if *self.current_token() == Token::As {
                self.advance();
            }
            let alias = match self.advance() {
                Token::Identifier(name) => name,
                _ => {
                    return Err(RustqlError::ParseError(
                        "Expected alias for MERGE source".to_string(),
                    ));
                }
            };
            MergeSource::Subquery {
                query: Box::new(query),
                alias,
            }
        } else {
            let name = match self.advance() {
                Token::Identifier(name) => name,
                _ => {
                    return Err(RustqlError::ParseError(
                        "Expected source table name after USING".to_string(),
                    ));
                }
            };
            let alias = if self.is_alias_token() {
                if *self.current_token() == Token::As {
                    self.advance();
                }
                match self.advance() {
                    Token::Identifier(a) => Some(a),
                    _ => None,
                }
            } else {
                None
            };
            MergeSource::Table { name, alias }
        };
        self.consume(Token::On)?;
        let on_condition = self.parse_expression()?;
        let mut when_clauses = Vec::new();
        while *self.current_token() == Token::When {
            self.advance();
            let is_matched = if *self.current_token() == Token::Matched {
                self.advance();
                true
            } else if *self.current_token() == Token::Not {
                self.advance();
                self.consume(Token::Matched)?;
                false
            } else {
                return Err(RustqlError::ParseError(
                    "Expected MATCHED or NOT MATCHED after WHEN".to_string(),
                ));
            };
            let condition = if *self.current_token() == Token::And {
                self.advance();
                Some(self.parse_expression()?)
            } else {
                None
            };
            self.consume(Token::Then)?;
            if is_matched {
                if *self.current_token() == Token::Update {
                    self.advance();
                    self.consume(Token::Set)?;
                    let assignments = self.parse_assignments()?;
                    when_clauses.push(MergeWhenClause::Matched {
                        condition,
                        action: MergeMatchedAction::Update { assignments },
                    });
                } else if *self.current_token() == Token::Delete {
                    self.advance();
                    when_clauses.push(MergeWhenClause::Matched {
                        condition,
                        action: MergeMatchedAction::Delete,
                    });
                } else {
                    return Err(RustqlError::ParseError(
                        "Expected UPDATE or DELETE after WHEN MATCHED THEN".to_string(),
                    ));
                }
            } else {
                self.consume(Token::Insert)?;
                let columns = if *self.current_token() == Token::LeftParen {
                    self.advance();
                    let mut cols = Vec::new();
                    loop {
                        match self.advance() {
                            Token::Identifier(name) => cols.push(name),
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
                    Some(cols)
                } else {
                    None
                };
                self.consume(Token::Values)?;
                self.consume(Token::LeftParen)?;
                let mut values = Vec::new();
                loop {
                    values.push(self.parse_expression()?);
                    if *self.current_token() == Token::Comma {
                        self.advance();
                    } else {
                        break;
                    }
                }
                self.consume(Token::RightParen)?;
                when_clauses.push(MergeWhenClause::NotMatched {
                    condition,
                    action: MergeNotMatchedAction::Insert { columns, values },
                });
            }
        }
        Ok(Statement::Merge(MergeStatement {
            target_table,
            source,
            on_condition,
            when_clauses,
        }))
    }

    fn parse_do_block(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Do)?;
        self.consume(Token::Begin)?;
        let mut statements = Vec::new();
        while *self.current_token() != Token::End && *self.current_token() != Token::Eof {
            let stmt = self.parse_statement()?;
            statements.push(stmt);
            if *self.current_token() == Token::Semicolon {
                self.advance();
            }
        }
        self.consume(Token::End)?;
        Ok(Statement::Do { statements })
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
        Token::ILike => "ILIKE".to_string(),
        Token::Between => "BETWEEN".to_string(),
        Token::Comma => ",".to_string(),
        Token::Intersect => "INTERSECT".to_string(),
        Token::Except => "EXCEPT".to_string(),
        Token::Constraint => "CONSTRAINT".to_string(),
        Token::Nullif => "NULLIF".to_string(),
        Token::Greatest => "GREATEST".to_string(),
        Token::Least => "LEAST".to_string(),
        Token::Lpad => "LPAD".to_string(),
        Token::Rpad => "RPAD".to_string(),
        Token::Reverse => "REVERSE".to_string(),
        Token::Repeat => "REPEAT".to_string(),
        Token::Log => "LOG".to_string(),
        Token::Exp => "EXP".to_string(),
        Token::Sign => "SIGN".to_string(),
        Token::DateTrunc => "DATE_TRUNC".to_string(),
        Token::Extract => "EXTRACT".to_string(),
        Token::Ltrim => "LTRIM".to_string(),
        Token::Rtrim => "RTRIM".to_string(),
        Token::Ascii => "ASCII".to_string(),
        Token::Chr => "CHR".to_string(),
        Token::Sin => "SIN".to_string(),
        Token::Cos => "COS".to_string(),
        Token::Tan => "TAN".to_string(),
        Token::Asin => "ASIN".to_string(),
        Token::Acos => "ACOS".to_string(),
        Token::Atan => "ATAN".to_string(),
        Token::Atan2 => "ATAN2".to_string(),
        Token::Random => "RANDOM".to_string(),
        Token::Degrees => "DEGREES".to_string(),
        Token::Radians => "RADIANS".to_string(),
        Token::Quarter => "QUARTER".to_string(),
        Token::Week => "WEEK".to_string(),
        Token::DayOfWeek => "DAYOFWEEK".to_string(),
        Token::Any => "ANY".to_string(),
        Token::Filter => "FILTER".to_string(),
        Token::Lateral => "LATERAL".to_string(),
        Token::Grouping => "GROUPING".to_string(),
        Token::Sets => "SETS".to_string(),
        Token::Cube => "CUBE".to_string(),
        Token::Rollup => "ROLLUP".to_string(),
        Token::Fetch => "FETCH".to_string(),
        Token::First => "FIRST".to_string(),
        Token::Next => "NEXT".to_string(),
        Token::Only => "ONLY".to_string(),
        Token::Ties => "TIES".to_string(),
        Token::Row => "ROW".to_string(),
        Token::DoubleColon => "::".to_string(),
        Token::GenerateSeries => "GENERATE_SERIES".to_string(),
        Token::Window => "WINDOW".to_string(),
        Token::Merge => "MERGE".to_string(),
        Token::Matched => "MATCHED".to_string(),
        Token::Generated => "GENERATED".to_string(),
        Token::Always => "ALWAYS".to_string(),
        Token::Stored => "STORED".to_string(),
        Token::Pi => "PI".to_string(),
        Token::Trunc => "TRUNC".to_string(),
        Token::Log10 => "LOG10".to_string(),
        Token::Log2 => "LOG2".to_string(),
        Token::Cbrt => "CBRT".to_string(),
        Token::Gcd => "GCD".to_string(),
        Token::Lcm => "LCM".to_string(),
        Token::Initcap => "INITCAP".to_string(),
        Token::SplitPart => "SPLIT_PART".to_string(),
        Token::Translate => "TRANSLATE".to_string(),
        Token::RegexpMatch => "REGEXP_MATCH".to_string(),
        Token::RegexpReplace => "REGEXP_REPLACE".to_string(),
        _ => format!("{:?}", tok),
    }
}

fn token_to_sql(tok: &Token) -> String {
    match tok {
        Token::Identifier(s) => s.clone(),
        Token::Number(n) => n.to_string(),
        Token::Float(f) => f.to_string(),
        Token::StringLiteral(s) => format!("'{}'", s),
        Token::LeftParen => "(".to_string(),
        Token::RightParen => ")".to_string(),
        Token::Comma => ",".to_string(),
        Token::Semicolon => ";".to_string(),
        Token::Dot => ".".to_string(),
        Token::Star => "*".to_string(),
        Token::Plus => "+".to_string(),
        Token::Minus => "-".to_string(),
        Token::Divide => "/".to_string(),
        Token::Equal => "=".to_string(),
        Token::NotEqual => "<>".to_string(),
        Token::LessThan => "<".to_string(),
        Token::LessThanOrEqual => "<=".to_string(),
        Token::GreaterThan => ">".to_string(),
        Token::GreaterThanOrEqual => ">=".to_string(),
        Token::Concat => "||".to_string(),
        Token::Select => "SELECT".to_string(),
        Token::Exists => "EXISTS".to_string(),
        Token::Distinct => "DISTINCT".to_string(),
        Token::From => "FROM".to_string(),
        Token::Where => "WHERE".to_string(),
        Token::Insert => "INSERT".to_string(),
        Token::Into => "INTO".to_string(),
        Token::Values => "VALUES".to_string(),
        Token::Update => "UPDATE".to_string(),
        Token::Set => "SET".to_string(),
        Token::Delete => "DELETE".to_string(),
        Token::Create => "CREATE".to_string(),
        Token::Table => "TABLE".to_string(),
        Token::Drop => "DROP".to_string(),
        Token::Alter => "ALTER".to_string(),
        Token::Add => "ADD".to_string(),
        Token::Column => "COLUMN".to_string(),
        Token::Rename => "RENAME".to_string(),
        Token::To => "TO".to_string(),
        Token::And => "AND".to_string(),
        Token::Or => "OR".to_string(),
        Token::Not => "NOT".to_string(),
        Token::Order => "ORDER".to_string(),
        Token::By => "BY".to_string(),
        Token::Asc => "ASC".to_string(),
        Token::Desc => "DESC".to_string(),
        Token::Limit => "LIMIT".to_string(),
        Token::Offset => "OFFSET".to_string(),
        Token::Group => "GROUP".to_string(),
        Token::Having => "HAVING".to_string(),
        Token::Count => "COUNT".to_string(),
        Token::Sum => "SUM".to_string(),
        Token::Avg => "AVG".to_string(),
        Token::Min => "MIN".to_string(),
        Token::Max => "MAX".to_string(),
        Token::As => "AS".to_string(),
        Token::Join => "JOIN".to_string(),
        Token::Inner => "INNER".to_string(),
        Token::Left => "LEFT".to_string(),
        Token::Right => "RIGHT".to_string(),
        Token::Full => "FULL".to_string(),
        Token::On => "ON".to_string(),
        Token::In => "IN".to_string(),
        Token::Like => "LIKE".to_string(),
        Token::Between => "BETWEEN".to_string(),
        Token::Is => "IS".to_string(),
        Token::Null => "NULL".to_string(),
        Token::Boolean => "BOOLEAN".to_string(),
        Token::Date => "DATE".to_string(),
        Token::Time => "TIME".to_string(),
        Token::DateTime => "DATETIME".to_string(),
        Token::Foreign => "FOREIGN".to_string(),
        Token::Key => "KEY".to_string(),
        Token::References => "REFERENCES".to_string(),
        Token::Cascade => "CASCADE".to_string(),
        Token::Restrict => "RESTRICT".to_string(),
        Token::No => "NO".to_string(),
        Token::Action => "ACTION".to_string(),
        Token::Union => "UNION".to_string(),
        Token::All => "ALL".to_string(),
        Token::Primary => "PRIMARY".to_string(),
        Token::Unique => "UNIQUE".to_string(),
        Token::Default => "DEFAULT".to_string(),
        Token::Index => "INDEX".to_string(),
        Token::Case => "CASE".to_string(),
        Token::When => "WHEN".to_string(),
        Token::Then => "THEN".to_string(),
        Token::Else => "ELSE".to_string(),
        Token::End => "END".to_string(),
        Token::Upper => "UPPER".to_string(),
        Token::Lower => "LOWER".to_string(),
        Token::Length => "LENGTH".to_string(),
        Token::Substring => "SUBSTRING".to_string(),
        Token::Abs => "ABS".to_string(),
        Token::Round => "ROUND".to_string(),
        Token::Coalesce => "COALESCE".to_string(),
        Token::Cross => "CROSS".to_string(),
        Token::Natural => "NATURAL".to_string(),
        Token::Check => "CHECK".to_string(),
        Token::With => "WITH".to_string(),
        Token::Over => "OVER".to_string(),
        Token::Partition => "PARTITION".to_string(),
        Token::RowNumber => "ROW_NUMBER".to_string(),
        Token::Rank => "RANK".to_string(),
        Token::DenseRank => "DENSE_RANK".to_string(),
        Token::Stddev => "STDDEV".to_string(),
        Token::Variance => "VARIANCE".to_string(),
        Token::Lag => "LAG".to_string(),
        Token::Lead => "LEAD".to_string(),
        Token::Ntile => "NTILE".to_string(),
        Token::GroupConcat => "GROUP_CONCAT".to_string(),
        Token::StringAgg => "STRING_AGG".to_string(),
        Token::BoolAnd => "BOOL_AND".to_string(),
        Token::BoolOr => "BOOL_OR".to_string(),
        Token::Every => "EVERY".to_string(),
        Token::Median => "MEDIAN".to_string(),
        Token::Mode => "MODE".to_string(),
        Token::PercentileCont => "PERCENTILE_CONT".to_string(),
        Token::PercentileDisc => "PERCENTILE_DISC".to_string(),
        Token::FirstValue => "FIRST_VALUE".to_string(),
        Token::LastValue => "LAST_VALUE".to_string(),
        Token::NthValue => "NTH_VALUE".to_string(),
        Token::PercentRank => "PERCENT_RANK".to_string(),
        Token::CumeDist => "CUME_DIST".to_string(),
        Token::Separator => "SEPARATOR".to_string(),
        Token::Within => "WITHIN".to_string(),
        Token::Rows => "ROWS".to_string(),
        Token::RangeFrame => "RANGE".to_string(),
        Token::Unbounded => "UNBOUNDED".to_string(),
        Token::Preceding => "PRECEDING".to_string(),
        Token::Following => "FOLLOWING".to_string(),
        Token::Current => "CURRENT".to_string(),
        Token::Cast => "CAST".to_string(),
        Token::ConcatFn => "CONCAT".to_string(),
        Token::Trim => "TRIM".to_string(),
        Token::Replace => "REPLACE".to_string(),
        Token::Position => "POSITION".to_string(),
        Token::Instr => "INSTR".to_string(),
        Token::Ceil => "CEIL".to_string(),
        Token::Ceiling => "CEILING".to_string(),
        Token::Floor => "FLOOR".to_string(),
        Token::Sqrt => "SQRT".to_string(),
        Token::Power => "POWER".to_string(),
        Token::Mod => "MOD".to_string(),
        Token::Now => "NOW".to_string(),
        Token::Year => "YEAR".to_string(),
        Token::Month => "MONTH".to_string(),
        Token::Day => "DAY".to_string(),
        Token::DateAdd => "DATE_ADD".to_string(),
        Token::Datediff => "DATEDIFF".to_string(),
        Token::Truncate => "TRUNCATE".to_string(),
        Token::View => "VIEW".to_string(),
        Token::ILike => "ILIKE".to_string(),
        Token::Intersect => "INTERSECT".to_string(),
        Token::Except => "EXCEPT".to_string(),
        Token::Constraint => "CONSTRAINT".to_string(),
        Token::Nullif => "NULLIF".to_string(),
        Token::Greatest => "GREATEST".to_string(),
        Token::Least => "LEAST".to_string(),
        Token::Lpad => "LPAD".to_string(),
        Token::Rpad => "RPAD".to_string(),
        Token::Reverse => "REVERSE".to_string(),
        Token::Repeat => "REPEAT".to_string(),
        Token::Log => "LOG".to_string(),
        Token::Exp => "EXP".to_string(),
        Token::Sign => "SIGN".to_string(),
        Token::DateTrunc => "DATE_TRUNC".to_string(),
        Token::Extract => "EXTRACT".to_string(),
        Token::Conflict => "CONFLICT".to_string(),
        Token::Do => "DO".to_string(),
        Token::Nothing => "NOTHING".to_string(),
        Token::Autoincrement => "AUTOINCREMENT".to_string(),
        Token::Analyze => "ANALYZE".to_string(),
        Token::Begin => "BEGIN".to_string(),
        Token::Commit => "COMMIT".to_string(),
        Token::Rollback => "ROLLBACK".to_string(),
        Token::Transaction => "TRANSACTION".to_string(),
        Token::Explain => "EXPLAIN".to_string(),
        Token::Describe => "DESCRIBE".to_string(),
        Token::Show => "SHOW".to_string(),
        Token::Tables => "TABLES".to_string(),
        Token::Savepoint => "SAVEPOINT".to_string(),
        Token::Release => "RELEASE".to_string(),
        Token::If => "IF".to_string(),
        Token::Using => "USING".to_string(),
        Token::Returning => "RETURNING".to_string(),
        Token::Recursive => "RECURSIVE".to_string(),
        Token::Ltrim => "LTRIM".to_string(),
        Token::Rtrim => "RTRIM".to_string(),
        Token::Ascii => "ASCII".to_string(),
        Token::Chr => "CHR".to_string(),
        Token::Sin => "SIN".to_string(),
        Token::Cos => "COS".to_string(),
        Token::Tan => "TAN".to_string(),
        Token::Asin => "ASIN".to_string(),
        Token::Acos => "ACOS".to_string(),
        Token::Atan => "ATAN".to_string(),
        Token::Atan2 => "ATAN2".to_string(),
        Token::Random => "RANDOM".to_string(),
        Token::Degrees => "DEGREES".to_string(),
        Token::Radians => "RADIANS".to_string(),
        Token::Quarter => "QUARTER".to_string(),
        Token::Week => "WEEK".to_string(),
        Token::DayOfWeek => "DAYOFWEEK".to_string(),
        Token::Any => "ANY".to_string(),
        Token::Filter => "FILTER".to_string(),
        Token::Lateral => "LATERAL".to_string(),
        Token::Grouping => "GROUPING".to_string(),
        Token::Sets => "SETS".to_string(),
        Token::Cube => "CUBE".to_string(),
        Token::Rollup => "ROLLUP".to_string(),
        Token::Fetch => "FETCH".to_string(),
        Token::First => "FIRST".to_string(),
        Token::Next => "NEXT".to_string(),
        Token::Only => "ONLY".to_string(),
        Token::Ties => "TIES".to_string(),
        Token::Row => "ROW".to_string(),
        Token::DoubleColon => "::".to_string(),
        Token::GenerateSeries => "GENERATE_SERIES".to_string(),
        _ => format!("{:?}", tok),
    }
}

pub fn parse(tokens: Vec<Token>) -> Result<Statement, RustqlError> {
    let mut parser = Parser::new(tokens);
    let statement = parser
        .parse_statement()
        .map_err(|err| parser.with_current_location(err))?;
    parser
        .finish_single_statement(statement)
        .map_err(|err| parser.with_current_location(err))
}

pub fn parse_spanned(tokens: Vec<SpannedToken>) -> Result<Statement, RustqlError> {
    let mut parser = Parser::new_spanned(tokens);
    let statement = parser
        .parse_statement()
        .map_err(|err| parser.with_current_location(err))?;
    parser
        .finish_single_statement(statement)
        .map_err(|err| parser.with_current_location(err))
}

pub fn parse_script(tokens: Vec<Token>) -> Result<Vec<Statement>, RustqlError> {
    let mut parser = Parser::new(tokens);
    let mut statements = Vec::new();

    while *parser.current_token() != Token::Eof {
        if *parser.current_token() == Token::Semicolon {
            parser.advance();
            continue;
        }

        let statement = parser
            .parse_statement()
            .map_err(|err| parser.with_current_location(err))?;
        statements.push(statement);

        if *parser.current_token() == Token::Semicolon {
            parser.advance();
        } else if *parser.current_token() != Token::Eof {
            return Err(
                parser.with_current_location(RustqlError::ParseError(format!(
                    "Expected semicolon or end of input, found {:?}",
                    parser.current_token()
                ))),
            );
        }
    }

    Ok(statements)
}

pub fn parse_script_spanned(tokens: Vec<SpannedToken>) -> Result<Vec<Statement>, RustqlError> {
    let mut parser = Parser::new_spanned(tokens);
    let mut statements = Vec::new();

    while *parser.current_token() != Token::Eof {
        if *parser.current_token() == Token::Semicolon {
            parser.advance();
            continue;
        }

        let statement = parser
            .parse_statement()
            .map_err(|err| parser.with_current_location(err))?;
        statements.push(statement);

        if *parser.current_token() == Token::Semicolon {
            parser.advance();
        } else if *parser.current_token() != Token::Eof {
            return Err(
                parser.with_current_location(RustqlError::ParseError(format!(
                    "Expected semicolon or end of input, found {:?}",
                    parser.current_token()
                ))),
            );
        }
    }

    Ok(statements)
}

#[cfg(test)]
mod tests {
    use super::parse_script;

    struct Lcg(u64);

    impl Lcg {
        fn new(seed: u64) -> Self {
            Self(seed)
        }

        fn next_u64(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0
        }
    }

    fn fuzz_sql(seed: u64) -> String {
        const ALPHABET: &[u8] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_ ',;()*=<>!+-/\n\t";

        let mut rng = Lcg::new(seed);
        let len = (rng.next_u64() % 128) as usize;
        let mut sql = String::with_capacity(len);
        for _ in 0..len {
            let index = (rng.next_u64() as usize) % ALPHABET.len();
            sql.push(ALPHABET[index] as char);
        }
        sql
    }

    #[test]
    fn parser_fuzz_inputs_do_not_panic() {
        for case in 0..512u64 {
            let sql = fuzz_sql(case.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(1));
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                if let Ok(tokens) = crate::lexer::tokenize(&sql) {
                    let _ = parse_script(tokens);
                }
            }));
            assert!(
                result.is_ok(),
                "parser panicked on fuzz case {case}: {sql:?}"
            );
        }
    }
}
