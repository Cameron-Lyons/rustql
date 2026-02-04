use crate::error::RustqlError;

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
    All,
    Primary,
    Unique,
    Default,
    Index,
    Begin,
    Commit,
    Rollback,
    Transaction,
    Explain,
    Describe,
    Show,
    Tables,
    Case,
    When,
    Then,
    Else,
    End,
    Upper,
    Lower,
    Length,
    Substring,
    Abs,
    Round,
    Coalesce,
    Cross,
    Natural,
    Check,
    Autoincrement,
    Savepoint,
    Release,
    With,
    Over,
    Partition,
    RowNumber,
    Rank,
    DenseRank,
    Analyze,
    Stddev,
    Variance,
    Lag,
    Lead,
    Ntile,
    GroupConcat,
    StringAgg,
    BoolAnd,
    BoolOr,
    Every,
    Median,
    Mode,
    PercentileCont,
    PercentileDisc,
    FirstValue,
    LastValue,
    NthValue,
    PercentRank,
    CumeDist,
    Separator,
    Within,
    Rows,
    RangeFrame,
    Unbounded,
    Preceding,
    Following,
    Current,
    Cast,
    Concat,
    ConcatFn,
    Trim,
    Replace,
    Position,
    Instr,
    Ceil,
    Ceiling,
    Floor,
    Sqrt,
    Power,
    Mod,
    Now,
    Year,
    Month,
    Day,
    DateAdd,
    Datediff,
    Truncate,
    View,
    Conflict,
    Do,
    Nothing,

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

pub fn tokenize(input: &str) -> Result<Vec<Token>, RustqlError> {
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
            '|' => {
                chars.next();
                if let Some(&'|') = chars.peek() {
                    tokens.push(Token::Concat);
                    chars.next();
                } else {
                    return Err(RustqlError::ParseError(
                        "Unexpected character: |".to_string(),
                    ));
                }
            }
            '!' => {
                chars.next();
                if let Some(&'=') = chars.peek() {
                    tokens.push(Token::NotEqual);
                    chars.next();
                } else {
                    return Err(RustqlError::ParseError(
                        "Unexpected character: !".to_string(),
                    ));
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
            '`' => {
                chars.next();
                let mut ident = String::new();
                loop {
                    match chars.next() {
                        Some('`') => break,
                        Some(ch) => ident.push(ch),
                        None => {
                            return Err(RustqlError::ParseError(
                                "Unterminated quoted identifier".to_string(),
                            ));
                        }
                    }
                }
                if ident.is_empty() {
                    return Err(RustqlError::ParseError(
                        "Empty quoted identifier".to_string(),
                    ));
                }
                tokens.push(Token::Identifier(ident));
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
                return Err(RustqlError::ParseError(format!(
                    "Unexpected character: {}",
                    ch
                )));
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
) -> Result<String, RustqlError> {
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

    Err(RustqlError::ParseError(
        "Unterminated string literal".to_string(),
    ))
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
        "ALL" => Token::All,
        "PRIMARY" => Token::Primary,
        "UNIQUE" => Token::Unique,
        "DEFAULT" => Token::Default,
        "INDEX" => Token::Index,
        "BEGIN" => Token::Begin,
        "COMMIT" => Token::Commit,
        "ROLLBACK" => Token::Rollback,
        "TRANSACTION" => Token::Transaction,
        "EXPLAIN" => Token::Explain,
        "DESCRIBE" => Token::Describe,
        "SHOW" => Token::Show,
        "TABLES" => Token::Tables,
        "CASE" => Token::Case,
        "WHEN" => Token::When,
        "THEN" => Token::Then,
        "ELSE" => Token::Else,
        "END" => Token::End,
        "UPPER" => Token::Upper,
        "LOWER" => Token::Lower,
        "LENGTH" => Token::Length,
        "SUBSTRING" | "SUBSTR" => Token::Substring,
        "ABS" => Token::Abs,
        "ROUND" => Token::Round,
        "COALESCE" => Token::Coalesce,
        "CROSS" => Token::Cross,
        "NATURAL" => Token::Natural,
        "CHECK" => Token::Check,
        "AUTOINCREMENT" | "AUTO_INCREMENT" => Token::Autoincrement,
        "SAVEPOINT" => Token::Savepoint,
        "RELEASE" => Token::Release,
        "WITH" => Token::With,
        "OVER" => Token::Over,
        "PARTITION" => Token::Partition,
        "ROW_NUMBER" => Token::RowNumber,
        "RANK" => Token::Rank,
        "DENSE_RANK" => Token::DenseRank,
        "ANALYZE" => Token::Analyze,
        "STDDEV" | "STDDEV_POP" => Token::Stddev,
        "VARIANCE" | "VAR_POP" => Token::Variance,
        "LAG" => Token::Lag,
        "LEAD" => Token::Lead,
        "NTILE" => Token::Ntile,
        "GROUP_CONCAT" => Token::GroupConcat,
        "STRING_AGG" => Token::StringAgg,
        "BOOL_AND" => Token::BoolAnd,
        "BOOL_OR" => Token::BoolOr,
        "EVERY" => Token::Every,
        "MEDIAN" => Token::Median,
        "MODE" => Token::Mode,
        "PERCENTILE_CONT" => Token::PercentileCont,
        "PERCENTILE_DISC" => Token::PercentileDisc,
        "FIRST_VALUE" => Token::FirstValue,
        "LAST_VALUE" => Token::LastValue,
        "NTH_VALUE" => Token::NthValue,
        "PERCENT_RANK" => Token::PercentRank,
        "CUME_DIST" => Token::CumeDist,
        "SEPARATOR" => Token::Separator,
        "WITHIN" => Token::Within,
        "ROWS" => Token::Rows,
        "RANGE" => Token::RangeFrame,
        "UNBOUNDED" => Token::Unbounded,
        "PRECEDING" => Token::Preceding,
        "FOLLOWING" => Token::Following,
        "CURRENT" => Token::Current,
        "CAST" => Token::Cast,
        "CONCAT" => Token::ConcatFn,
        "TRIM" => Token::Trim,
        "REPLACE" => Token::Replace,
        "POSITION" => Token::Position,
        "INSTR" => Token::Instr,
        "CEIL" => Token::Ceil,
        "CEILING" => Token::Ceiling,
        "FLOOR" => Token::Floor,
        "SQRT" => Token::Sqrt,
        "POWER" => Token::Power,
        "MOD" => Token::Mod,
        "NOW" => Token::Now,
        "YEAR" => Token::Year,
        "MONTH" => Token::Month,
        "DAY" => Token::Day,
        "DATE_ADD" | "DATEADD" => Token::DateAdd,
        "DATEDIFF" | "DATE_DIFF" => Token::Datediff,
        "TRUNCATE" => Token::Truncate,
        "VIEW" => Token::View,
        "CONFLICT" => Token::Conflict,
        "DO" => Token::Do,
        "NOTHING" => Token::Nothing,
        _ => Token::Identifier(ident.to_string()),
    }
}
