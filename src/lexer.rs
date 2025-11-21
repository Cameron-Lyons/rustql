#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    Dot,
    Select,
    Exists,
    Distinct,
    From,
    Where,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    Create,
    Table,
    Drop,
    Alter,
    Add,
    Column,
    Rename,
    To,
    And,
    Or,
    Not,
    Order,
    By,
    Asc,
    Desc,
    Limit,
    Offset,
    Group,
    Having,
    Count,
    Sum,
    Avg,
    Min,
    Max,
    As,
    Join,
    Inner,
    Left,
    Right,
    Full,
    On,
    In,
    Like,
    Between,
    Is,
    Null,
    Boolean,
    Date,
    Time,
    DateTime,
    Foreign,
    Key,
    References,
    Cascade,
    Restrict,
    No,
    Action,
    Union,
    Primary,
    Default,

    Identifier(String),
    Number(i64),
    Float(f64),
    StringLiteral(String),

    LeftParen,
    RightParen,
    Comma,
    Semicolon,
    Star,

    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,

    Plus,
    Minus,
    #[allow(dead_code)]
    Multiply,
    Divide,

    Eof,
}

pub fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(&ch) = chars.peek() {
        match ch {
            ' ' | '\t' | '\n' | '\r' => {
                chars.next();
            }
            '(' => {
                tokens.push(Token::LeftParen);
                chars.next();
            }
            ')' => {
                tokens.push(Token::RightParen);
                chars.next();
            }
            ',' => {
                tokens.push(Token::Comma);
                chars.next();
            }
            ';' => {
                tokens.push(Token::Semicolon);
                chars.next();
            }
            '*' => {
                tokens.push(Token::Star);
                chars.next();
            }
            '+' => {
                tokens.push(Token::Plus);
                chars.next();
            }
            '-' => {
                chars.next();
                if let Some(&next_ch) = chars.peek() {
                    if next_ch.is_ascii_digit() {
                        let num = read_number(&mut chars);
                        if num.contains('.') {
                            tokens.push(Token::Float(-num.parse::<f64>().unwrap()));
                        } else {
                            tokens.push(Token::Number(-num.parse::<i64>().unwrap()));
                        }
                    } else {
                        tokens.push(Token::Minus);
                    }
                } else {
                    tokens.push(Token::Minus);
                }
            }
            '/' => {
                tokens.push(Token::Divide);
                chars.next();
            }
            '=' => {
                tokens.push(Token::Equal);
                chars.next();
            }
            '<' => {
                chars.next();
                if let Some(&'=') = chars.peek() {
                    tokens.push(Token::LessThanOrEqual);
                    chars.next();
                } else if let Some(&'>') = chars.peek() {
                    tokens.push(Token::NotEqual);
                    chars.next();
                } else {
                    tokens.push(Token::LessThan);
                }
            }
            '>' => {
                chars.next();
                if let Some(&'=') = chars.peek() {
                    tokens.push(Token::GreaterThanOrEqual);
                    chars.next();
                } else {
                    tokens.push(Token::GreaterThan);
                }
            }
            '!' => {
                chars.next();
                if let Some(&'=') = chars.peek() {
                    tokens.push(Token::NotEqual);
                    chars.next();
                } else {
                    return Err("Unexpected character: !".to_string());
                }
            }
            '\'' => {
                chars.next();
                let string_val = read_string(&mut chars, '\'')?;
                tokens.push(Token::StringLiteral(string_val));
            }
            '"' => {
                chars.next();
                let string_val = read_string(&mut chars, '"')?;
                tokens.push(Token::StringLiteral(string_val));
            }
            _ if ch.is_ascii_digit() => {
                let num = read_number(&mut chars);
                if num.contains('.') {
                    tokens.push(Token::Float(num.parse::<f64>().unwrap()));
                } else {
                    tokens.push(Token::Number(num.parse::<i64>().unwrap()));
                }
            }
            _ if ch.is_ascii_alphabetic() || ch == '_' => {
                let ident = read_identifier(&mut chars);
                tokens.push(match_keyword(&ident));
            }
            '.' => {
                if let Some(Token::Identifier(last_ident)) = tokens.last_mut() {
                    chars.next();
                    if let Some(&ch) = chars.peek() {
                        if ch.is_ascii_alphanumeric() || ch == '_' {
                            let rest = read_identifier(&mut chars);
                            last_ident.push('.');
                            last_ident.push_str(&rest);
                        } else {
                            tokens.push(Token::Dot);
                            chars.next();
                        }
                    } else {
                        tokens.push(Token::Dot);
                        chars.next();
                    }
                } else {
                    tokens.push(Token::Dot);
                    chars.next();
                }
            }
            _ => {
                return Err(format!("Unexpected character: {}", ch));
            }
        }
    }

    tokens.push(Token::Eof);
    Ok(tokens)
}

fn read_identifier(chars: &mut std::iter::Peekable<std::str::Chars>) -> String {
    let mut ident = String::new();
    while let Some(&ch) = chars.peek() {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            ident.push(ch);
            chars.next();
        } else {
            break;
        }
    }
    ident
}

fn read_number(chars: &mut std::iter::Peekable<std::str::Chars>) -> String {
    let mut num = String::new();
    let mut has_dot = false;

    while let Some(&ch) = chars.peek() {
        if ch.is_ascii_digit() {
            num.push(ch);
            chars.next();
        } else if ch == '.' && !has_dot {
            has_dot = true;
            num.push(ch);
            chars.next();
        } else {
            break;
        }
    }
    num
}

fn read_string(
    chars: &mut std::iter::Peekable<std::str::Chars>,
    delimiter: char,
) -> Result<String, String> {
    let mut string_val = String::new();
    let mut escaped = false;

    while let Some(&ch) = chars.peek() {
        chars.next();

        if escaped {
            string_val.push(ch);
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else if ch == delimiter {
            return Ok(string_val);
        } else {
            string_val.push(ch);
        }
    }

    Err("Unterminated string literal".to_string())
}

fn match_keyword(ident: &str) -> Token {
    match ident.to_uppercase().as_str() {
        "SELECT" => Token::Select,
        "EXISTS" => Token::Exists,
        "DISTINCT" => Token::Distinct,
        "FROM" => Token::From,
        "WHERE" => Token::Where,
        "INSERT" => Token::Insert,
        "INTO" => Token::Into,
        "VALUES" => Token::Values,
        "UPDATE" => Token::Update,
        "SET" => Token::Set,
        "DELETE" => Token::Delete,
        "CREATE" => Token::Create,
        "TABLE" => Token::Table,
        "DROP" => Token::Drop,
        "ALTER" => Token::Alter,
        "ADD" => Token::Add,
        "COLUMN" => Token::Column,
        "RENAME" => Token::Rename,
        "TO" => Token::To,
        "AND" => Token::And,
        "OR" => Token::Or,
        "NOT" => Token::Not,
        "ORDER" => Token::Order,
        "BY" => Token::By,
        "ASC" => Token::Asc,
        "DESC" => Token::Desc,
        "LIMIT" => Token::Limit,
        "OFFSET" => Token::Offset,
        "GROUP" => Token::Group,
        "HAVING" => Token::Having,
        "COUNT" => Token::Count,
        "SUM" => Token::Sum,
        "AVG" => Token::Avg,
        "MIN" => Token::Min,
        "MAX" => Token::Max,
        "AS" => Token::As,
        "JOIN" => Token::Join,
        "INNER" => Token::Inner,
        "LEFT" => Token::Left,
        "RIGHT" => Token::Right,
        "FULL" => Token::Full,
        "ON" => Token::On,
        "IN" => Token::In,
        "LIKE" => Token::Like,
        "BETWEEN" => Token::Between,
        "IS" => Token::Is,
        "NULL" => Token::Null,
        "BOOLEAN" => Token::Boolean,
        "DATE" => Token::Date,
        "TIME" => Token::Time,
        "DATETIME" => Token::DateTime,
        "FOREIGN" => Token::Foreign,
        "KEY" => Token::Key,
        "REFERENCES" => Token::References,
        "CASCADE" => Token::Cascade,
        "RESTRICT" => Token::Restrict,
        "NO" => Token::No,
        "ACTION" => Token::Action,
        "UNION" => Token::Union,
        "PRIMARY" => Token::Primary,
        "DEFAULT" => Token::Default,
        _ => Token::Identifier(ident.to_string()),
    }
}
