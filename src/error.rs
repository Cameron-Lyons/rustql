use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceLocation {
    pub line: usize,
    pub column: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceSpan {
    pub start: SourceLocation,
    pub end: SourceLocation,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConstraintKind {
    NotNull,
    PrimaryKey,
    Unique,
    ForeignKey,
    Check,
}

impl fmt::Display for ConstraintKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConstraintKind::NotNull => write!(f, "NOT NULL"),
            ConstraintKind::PrimaryKey => write!(f, "Primary key"),
            ConstraintKind::Unique => write!(f, "Unique"),
            ConstraintKind::ForeignKey => write!(f, "Foreign key"),
            ConstraintKind::Check => write!(f, "CHECK"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryClause {
    Having,
}

impl fmt::Display for QueryClause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryClause::Having => write!(f, "HAVING"),
        }
    }
}

#[derive(Debug)]
pub enum RustqlError {
    TableNotFound(String),
    TableAlreadyExists(String),
    ColumnNotFound(String),
    AmbiguousColumn(String),
    ColumnAlreadyExists {
        name: String,
    },
    ColumnDoesNotExist {
        name: String,
    },
    ColumnNotFoundInClause {
        name: String,
        clause: QueryClause,
    },
    ConstraintViolation {
        kind: ConstraintKind,
        message: String,
    },
    ParseError(String),
    ParseErrorAt {
        message: String,
        span: SourceSpan,
    },
    StorageError(String),
    TypeMismatch(String),
    TransactionError(String),
    AggregateError(String),
    IndexError(String),
    IndexNotFound {
        name: String,
    },
    DivisionByZero,
    Internal(String),
}

impl fmt::Display for RustqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RustqlError::TableNotFound(name) => {
                write!(f, "Table '{}' does not exist", name)
            }
            RustqlError::TableAlreadyExists(name) => {
                write!(f, "Table '{}' already exists", name)
            }
            RustqlError::ColumnNotFound(name) => {
                write!(f, "Column '{}' not found", name)
            }
            RustqlError::AmbiguousColumn(name) => {
                write!(f, "Column '{}' is ambiguous", name)
            }
            RustqlError::ColumnAlreadyExists { name } => {
                write!(f, "Column '{}' already exists", name)
            }
            RustqlError::ColumnDoesNotExist { name } => {
                write!(f, "Column '{}' does not exist", name)
            }
            RustqlError::ColumnNotFoundInClause { name, clause } => {
                write!(f, "Column '{}' not found in {} clause", name, clause)
            }
            RustqlError::ConstraintViolation { kind: _, message } => {
                write!(f, "{}", message)
            }
            RustqlError::ParseError(msg) => write!(f, "{}", msg),
            RustqlError::ParseErrorAt { message, span } => write!(
                f,
                "{} at line {}, column {}",
                message, span.start.line, span.start.column
            ),
            RustqlError::StorageError(msg) => write!(f, "{}", msg),
            RustqlError::TypeMismatch(msg) => write!(f, "{}", msg),
            RustqlError::TransactionError(msg) => write!(f, "{}", msg),
            RustqlError::AggregateError(msg) => write!(f, "{}", msg),
            RustqlError::IndexError(msg) => write!(f, "{}", msg),
            RustqlError::IndexNotFound { name } => {
                write!(f, "Index '{}' does not exist", name)
            }
            RustqlError::DivisionByZero => write!(f, "Division by zero"),
            RustqlError::Internal(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for RustqlError {}

pub type Result<T> = std::result::Result<T, RustqlError>;

impl RustqlError {
    pub fn span(&self) -> Option<SourceSpan> {
        match self {
            RustqlError::ParseErrorAt { span, .. } => Some(*span),
            _ => None,
        }
    }

    pub fn with_parse_span(self, span: SourceSpan) -> Self {
        match self {
            RustqlError::ParseError(message) => RustqlError::ParseErrorAt { message, span },
            err => err,
        }
    }
}

impl From<std::io::Error> for RustqlError {
    fn from(e: std::io::Error) -> Self {
        RustqlError::StorageError(e.to_string())
    }
}

impl From<serde_json::Error> for RustqlError {
    fn from(e: serde_json::Error) -> Self {
        RustqlError::StorageError(e.to_string())
    }
}

impl From<String> for RustqlError {
    fn from(message: String) -> Self {
        RustqlError::Internal(message)
    }
}
