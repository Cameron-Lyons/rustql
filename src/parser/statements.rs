use super::*;

impl Parser {
    pub(super) fn parse_returning(&mut self) -> Result<Option<Vec<Column>>, RustqlError> {
        if *self.current_token() == Token::Returning {
            self.advance();
            let cols = self.parse_columns()?;
            Ok(Some(cols))
        } else {
            Ok(None)
        }
    }

    pub(super) fn parse_insert(&mut self) -> Result<Statement, RustqlError> {
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

    pub(super) fn parse_values(&mut self) -> Result<Vec<Vec<Value>>, RustqlError> {
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

    pub(super) fn parse_value(&mut self) -> Result<Value, RustqlError> {
        match self.advance() {
            Token::Null => Ok(Value::Null),
            Token::Number(n) => Ok(Value::Integer(n)),
            Token::Float(f) => Ok(Value::Float(f)),
            Token::StringLiteral(s) => Ok(Value::Text(s)),
            _ => Err(RustqlError::ParseError("Expected value".to_string())),
        }
    }

    pub(super) fn parse_update(&mut self) -> Result<Statement, RustqlError> {
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

    pub(super) fn parse_assignments(&mut self) -> Result<Vec<Assignment>, RustqlError> {
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

    pub(super) fn parse_delete(&mut self) -> Result<Statement, RustqlError> {
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

    pub(super) fn parse_create(&mut self) -> Result<Statement, RustqlError> {
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

    pub(super) fn parse_column_definitions(
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

    pub(super) fn parse_data_type(&mut self) -> Result<DataType, RustqlError> {
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

    pub(super) fn parse_drop(&mut self) -> Result<Statement, RustqlError> {
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

    pub(super) fn parse_alter(&mut self) -> Result<Statement, RustqlError> {
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

    pub(super) fn parse_begin_transaction(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Begin)?;
        if *self.current_token() == Token::Transaction {
            self.advance();
        }
        Ok(Statement::BeginTransaction)
    }

    pub(super) fn parse_commit_transaction(&mut self) -> Result<Statement, RustqlError> {
        self.consume(Token::Commit)?;
        if *self.current_token() == Token::Transaction {
            self.advance();
        }
        Ok(Statement::CommitTransaction)
    }

    pub(super) fn parse_rollback(&mut self) -> Result<Statement, RustqlError> {
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

    pub(super) fn parse_savepoint(&mut self) -> Result<Statement, RustqlError> {
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

    pub(super) fn parse_release_savepoint(&mut self) -> Result<Statement, RustqlError> {
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

    pub(super) fn parse_explain(&mut self) -> Result<Statement, RustqlError> {
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

    pub(super) fn parse_describe(&mut self) -> Result<Statement, RustqlError> {
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

    pub(super) fn parse_show(&mut self) -> Result<Statement, RustqlError> {
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

    pub(super) fn parse_analyze(&mut self) -> Result<Statement, RustqlError> {
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

    pub(super) fn parse_truncate(&mut self) -> Result<Statement, RustqlError> {
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

    pub(super) fn parse_merge(&mut self) -> Result<Statement, RustqlError> {
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

    pub(super) fn parse_do_block(&mut self) -> Result<Statement, RustqlError> {
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
