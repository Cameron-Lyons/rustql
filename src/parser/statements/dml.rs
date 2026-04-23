use super::*;

impl Parser {
    pub(crate) fn parse_returning(&mut self) -> Result<Option<Vec<Column>>, RustqlError> {
        if *self.current_token() == Token::Returning {
            self.advance();
            let cols = self.parse_columns()?;
            Ok(Some(cols))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn parse_insert(&mut self) -> Result<Statement, RustqlError> {
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

    pub(crate) fn parse_values(&mut self) -> Result<Vec<Vec<Value>>, RustqlError> {
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

    pub(crate) fn parse_value(&mut self) -> Result<Value, RustqlError> {
        match self.advance() {
            Token::Null => Ok(Value::Null),
            Token::Number(n) => Ok(Value::Integer(n)),
            Token::Float(f) => Ok(Value::Float(f)),
            Token::StringLiteral(s) => Ok(Value::Text(s)),
            _ => Err(RustqlError::ParseError("Expected value".to_string())),
        }
    }

    pub(crate) fn parse_update(&mut self) -> Result<Statement, RustqlError> {
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

    pub(crate) fn parse_assignments(&mut self) -> Result<Vec<Assignment>, RustqlError> {
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

    pub(crate) fn parse_delete(&mut self) -> Result<Statement, RustqlError> {
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

    pub(crate) fn parse_merge(&mut self) -> Result<Statement, RustqlError> {
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
}
