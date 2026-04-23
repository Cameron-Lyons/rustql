use crate::error::{RustqlError, SourceLocation, SourceSpan};

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
    ILike,
    Intersect,
    Except,
    Constraint,
    Nullif,
    Greatest,
    Least,
    Lpad,
    Rpad,
    Reverse,
    Repeat,
    Log,
    Exp,
    Sign,
    DateTrunc,
    Extract,
    If,
    Using,
    Returning,
    Recursive,
    Ltrim,
    Rtrim,
    Ascii,
    Chr,
    Sin,
    Cos,
    Tan,
    Asin,
    Acos,
    Atan,
    Atan2,
    Random,
    Degrees,
    Radians,
    Quarter,
    Week,
    DayOfWeek,
    Any,
    Filter,
    Lateral,
    Grouping,
    Sets,
    Cube,
    Rollup,
    Fetch,
    First,
    Next,
    Only,
    Ties,
    Row,
    DoubleColon,
    GenerateSeries,
    Window,
    Merge,
    Matched,
    Generated,
    Always,
    Stored,
    Initcap,
    SplitPart,
    Translate,
    RegexpMatch,
    RegexpReplace,
    Pi,
    Trunc,
    Log10,
    Log2,
    Cbrt,
    Gcd,
    Lcm,

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
    Divide,

    Eof,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SpannedToken {
    pub token: Token,
    pub span: SourceSpan,
}

pub fn tokenize_spanned(input: &str) -> Result<Vec<SpannedToken>, RustqlError> {
    let tokens = tokenize(input)?;
    Ok(assign_spans(input, tokens))
}

pub fn tokenize(input: &str) -> Result<Vec<Token>, RustqlError> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();
    let mut can_extend_last_identifier_with_dot = false;

    while let Some(&ch) = chars.peek() {
        match ch {
            ' ' | '\t' | '\n' | '\r' => {
                chars.next();
                can_extend_last_identifier_with_dot = false;
            }
            '(' => {
                tokens.push(Token::LeftParen);
                chars.next();
                can_extend_last_identifier_with_dot = false;
            }
            ')' => {
                tokens.push(Token::RightParen);
                chars.next();
                can_extend_last_identifier_with_dot = false;
            }
            ',' => {
                tokens.push(Token::Comma);
                chars.next();
                can_extend_last_identifier_with_dot = false;
            }
            ';' => {
                tokens.push(Token::Semicolon);
                chars.next();
                can_extend_last_identifier_with_dot = false;
            }
            '*' => {
                tokens.push(Token::Star);
                chars.next();
                can_extend_last_identifier_with_dot = false;
            }
            '+' => {
                tokens.push(Token::Plus);
                chars.next();
                can_extend_last_identifier_with_dot = false;
            }
            '-' => {
                chars.next();
                if let Some(&next_ch) = chars.peek() {
                    if next_ch.is_ascii_digit() {
                        let num = read_number(&mut chars);
                        let literal = format!("-{}", num);
                        if num.contains('.') {
                            tokens.push(Token::Float(parse_float_literal(&literal)?));
                        } else {
                            tokens.push(Token::Number(parse_integer_literal(&literal)?));
                        }
                    } else {
                        tokens.push(Token::Minus);
                    }
                } else {
                    tokens.push(Token::Minus);
                }
                can_extend_last_identifier_with_dot = false;
            }
            '/' => {
                tokens.push(Token::Divide);
                chars.next();
                can_extend_last_identifier_with_dot = false;
            }
            '=' => {
                tokens.push(Token::Equal);
                chars.next();
                can_extend_last_identifier_with_dot = false;
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
                can_extend_last_identifier_with_dot = false;
            }
            '>' => {
                chars.next();
                if let Some(&'=') = chars.peek() {
                    tokens.push(Token::GreaterThanOrEqual);
                    chars.next();
                } else {
                    tokens.push(Token::GreaterThan);
                }
                can_extend_last_identifier_with_dot = false;
            }
            '|' => {
                chars.next();
                if let Some(&'|') = chars.peek() {
                    tokens.push(Token::Concat);
                    chars.next();
                    can_extend_last_identifier_with_dot = false;
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
                    can_extend_last_identifier_with_dot = false;
                } else {
                    return Err(RustqlError::ParseError(
                        "Unexpected character: !".to_string(),
                    ));
                }
            }
            ':' => {
                chars.next();
                if let Some(&':') = chars.peek() {
                    tokens.push(Token::DoubleColon);
                    chars.next();
                    can_extend_last_identifier_with_dot = false;
                } else {
                    return Err(RustqlError::ParseError(
                        "Unexpected character: :".to_string(),
                    ));
                }
            }
            '\'' => {
                chars.next();
                let string_val = read_string(&mut chars, '\'')?;
                tokens.push(Token::StringLiteral(string_val));
                can_extend_last_identifier_with_dot = false;
            }
            '"' => {
                chars.next();
                let string_val = read_string(&mut chars, '"')?;
                tokens.push(Token::StringLiteral(string_val));
                can_extend_last_identifier_with_dot = false;
            }
            _ if ch.is_ascii_digit() => {
                let num = read_number(&mut chars);
                if num.contains('.') {
                    tokens.push(Token::Float(parse_float_literal(&num)?));
                } else {
                    tokens.push(Token::Number(parse_integer_literal(&num)?));
                }
                can_extend_last_identifier_with_dot = false;
            }
            _ if ch.is_ascii_alphabetic() || ch == '_' => {
                let ident = read_identifier(&mut chars);
                tokens.push(match_keyword(&ident));
                can_extend_last_identifier_with_dot =
                    matches!(tokens.last(), Some(Token::Identifier(_)));
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
                can_extend_last_identifier_with_dot = true;
            }
            '.' => {
                if can_extend_last_identifier_with_dot
                    && let Some(Token::Identifier(last_ident)) = tokens.last_mut()
                {
                    chars.next();
                    if let Some(&ch) = chars.peek() {
                        if ch.is_ascii_alphanumeric() || ch == '_' {
                            let rest = read_identifier(&mut chars);
                            last_ident.push('.');
                            last_ident.push_str(&rest);
                            can_extend_last_identifier_with_dot = true;
                        } else {
                            tokens.push(Token::Dot);
                            can_extend_last_identifier_with_dot = false;
                        }
                    } else {
                        tokens.push(Token::Dot);
                        can_extend_last_identifier_with_dot = false;
                    }
                } else {
                    tokens.push(Token::Dot);
                    chars.next();
                    can_extend_last_identifier_with_dot = false;
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

struct SpanCursor<'a> {
    input: &'a str,
    byte_index: usize,
    line: usize,
    column: usize,
}

impl<'a> SpanCursor<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            input,
            byte_index: 0,
            line: 1,
            column: 1,
        }
    }

    fn location(&self) -> SourceLocation {
        SourceLocation {
            line: self.line,
            column: self.column,
        }
    }

    fn peek(&self) -> Option<char> {
        self.input.get(self.byte_index..)?.chars().next()
    }

    fn peek_next(&self) -> Option<char> {
        let mut chars = self.input.get(self.byte_index..)?.chars();
        chars.next()?;
        chars.next()
    }

    fn bump(&mut self) -> Option<char> {
        let ch = self.peek()?;
        self.byte_index += ch.len_utf8();
        if ch == '\n' {
            self.line += 1;
            self.column = 1;
        } else {
            self.column += 1;
        }
        Some(ch)
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek(), Some(' ' | '\t' | '\n' | '\r')) {
            self.bump();
        }
    }

    fn consume_fixed(&mut self, text: &str) {
        for _ in text.chars() {
            self.bump();
        }
    }

    fn consume_number_token(&mut self) {
        if self.peek() == Some('-') {
            self.bump();
        }

        let mut has_dot = false;
        while let Some(ch) = self.peek() {
            if ch.is_ascii_digit() {
                self.bump();
            } else if ch == '.' && !has_dot {
                has_dot = true;
                self.bump();
            } else {
                break;
            }
        }
    }

    fn consume_identifier_token(&mut self) {
        if self.peek() == Some('`') {
            self.bump();
            while let Some(ch) = self.bump() {
                if ch == '`' {
                    break;
                }
            }
            return;
        }

        self.consume_identifier_part();
        while self.peek() == Some('.')
            && self
                .peek_next()
                .is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_')
        {
            self.bump();
            self.consume_identifier_part();
        }
    }

    fn consume_identifier_part(&mut self) {
        while let Some(ch) = self.peek() {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                self.bump();
            } else {
                break;
            }
        }
    }

    fn consume_string_token(&mut self) {
        let Some(delimiter @ ('\'' | '"')) = self.peek() else {
            return;
        };
        self.bump();

        let mut escaped = false;
        while let Some(ch) = self.bump() {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == delimiter {
                break;
            }
        }
    }
}

fn assign_spans(input: &str, tokens: Vec<Token>) -> Vec<SpannedToken> {
    let mut cursor = SpanCursor::new(input);
    tokens
        .into_iter()
        .map(|token| {
            cursor.skip_whitespace();
            let start = cursor.location();
            match &token {
                Token::Eof => {}
                Token::Identifier(_) => cursor.consume_identifier_token(),
                Token::Number(_) | Token::Float(_) => cursor.consume_number_token(),
                Token::StringLiteral(_) => cursor.consume_string_token(),
                _ => {
                    if let Some(text) = fixed_token_text(&token) {
                        cursor.consume_fixed(text);
                    } else {
                        cursor.consume_identifier_part();
                    }
                }
            }
            let end = cursor.location();
            SpannedToken {
                token,
                span: SourceSpan { start, end },
            }
        })
        .collect()
}

fn fixed_token_text(token: &Token) -> Option<&'static str> {
    match token {
        Token::Dot => Some("."),
        Token::LeftParen => Some("("),
        Token::RightParen => Some(")"),
        Token::Comma => Some(","),
        Token::Semicolon => Some(";"),
        Token::Star => Some("*"),
        Token::Plus => Some("+"),
        Token::Minus => Some("-"),
        Token::Divide => Some("/"),
        Token::Equal => Some("="),
        Token::NotEqual => Some("<>"),
        Token::LessThan => Some("<"),
        Token::LessThanOrEqual => Some("<="),
        Token::GreaterThan => Some(">"),
        Token::GreaterThanOrEqual => Some(">="),
        Token::Concat => Some("||"),
        Token::DoubleColon => Some("::"),
        _ => None,
    }
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

fn parse_integer_literal(literal: &str) -> Result<i64, RustqlError> {
    literal
        .parse::<i64>()
        .map_err(|_| RustqlError::ParseError(format!("Invalid integer literal: {}", literal)))
}

fn parse_float_literal(literal: &str) -> Result<f64, RustqlError> {
    let value = literal
        .parse::<f64>()
        .map_err(|_| RustqlError::ParseError(format!("Invalid float literal: {}", literal)))?;
    if value.is_finite() {
        Ok(value)
    } else {
        Err(RustqlError::ParseError(format!(
            "Invalid float literal: {}",
            literal
        )))
    }
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
        "ILIKE" => Token::ILike,
        "INTERSECT" => Token::Intersect,
        "EXCEPT" => Token::Except,
        "CONSTRAINT" => Token::Constraint,
        "NULLIF" => Token::Nullif,
        "GREATEST" => Token::Greatest,
        "LEAST" => Token::Least,
        "LPAD" => Token::Lpad,
        "RPAD" => Token::Rpad,
        "REVERSE" => Token::Reverse,
        "REPEAT" => Token::Repeat,
        "LOG" | "LN" => Token::Log,
        "EXP" => Token::Exp,
        "SIGN" => Token::Sign,
        "DATE_TRUNC" => Token::DateTrunc,
        "EXTRACT" => Token::Extract,
        "IF" => Token::If,
        "USING" => Token::Using,
        "RETURNING" => Token::Returning,
        "RECURSIVE" => Token::Recursive,
        "LTRIM" => Token::Ltrim,
        "RTRIM" => Token::Rtrim,
        "ASCII" => Token::Ascii,
        "CHR" => Token::Chr,
        "SIN" => Token::Sin,
        "COS" => Token::Cos,
        "TAN" => Token::Tan,
        "ASIN" => Token::Asin,
        "ACOS" => Token::Acos,
        "ATAN" => Token::Atan,
        "ATAN2" => Token::Atan2,
        "RANDOM" | "RAND" => Token::Random,
        "DEGREES" => Token::Degrees,
        "RADIANS" => Token::Radians,
        "QUARTER" => Token::Quarter,
        "WEEK" | "WEEKOFYEAR" => Token::Week,
        "DAYOFWEEK" | "WEEKDAY" => Token::DayOfWeek,
        "ANY" | "SOME" => Token::Any,
        "FILTER" => Token::Filter,
        "LATERAL" => Token::Lateral,
        "GROUPING" => Token::Grouping,
        "SETS" => Token::Sets,
        "CUBE" => Token::Cube,
        "ROLLUP" => Token::Rollup,
        "FETCH" => Token::Fetch,
        "FIRST" => Token::First,
        "NEXT" => Token::Next,
        "ONLY" => Token::Only,
        "TIES" => Token::Ties,
        "ROW" => Token::Row,
        "GENERATE_SERIES" => Token::GenerateSeries,
        "WINDOW" => Token::Window,
        "MERGE" => Token::Merge,
        "MATCHED" => Token::Matched,
        "GENERATED" => Token::Generated,
        "ALWAYS" => Token::Always,
        "STORED" => Token::Stored,
        "INITCAP" => Token::Initcap,
        "SPLIT_PART" => Token::SplitPart,
        "TRANSLATE" => Token::Translate,
        "REGEXP_MATCH" => Token::RegexpMatch,
        "REGEXP_REPLACE" => Token::RegexpReplace,
        "PI" => Token::Pi,
        "TRUNC" => Token::Trunc,
        "LOG10" => Token::Log10,
        "LOG2" => Token::Log2,
        "CBRT" => Token::Cbrt,
        "GCD" => Token::Gcd,
        "LCM" => Token::Lcm,
        _ => Token::Identifier(ident.to_string()),
    }
}
