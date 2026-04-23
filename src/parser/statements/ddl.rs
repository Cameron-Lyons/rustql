use super::*;

impl Parser {
    pub(crate) fn parse_create(&mut self) -> Result<Statement, RustqlError> {
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

    pub(crate) fn parse_column_definitions(
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

    pub(crate) fn parse_data_type(&mut self) -> Result<DataType, RustqlError> {
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

    pub(crate) fn parse_drop(&mut self) -> Result<Statement, RustqlError> {
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

    pub(crate) fn parse_alter(&mut self) -> Result<Statement, RustqlError> {
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
}
