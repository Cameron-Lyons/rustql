use std::fmt;

#[derive(Debug, Clone, PartialEq)]
pub enum ConstraintKind {
    NotNull,
    PrimaryKey,
    Unique,
    ForeignKey,
}

impl fmt::Display for ConstraintKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConstraintKind::NotNull => write!(f, "NOT NULL"),
            ConstraintKind::PrimaryKey => write!(f, "Primary key"),
            ConstraintKind::Unique => write!(f, "Unique"),
            ConstraintKind::ForeignKey => write!(f, "Foreign key"),
        }
    }
}

#[derive(Debug)]
pub enum RustqlError {
    TableNotFound(String),
    TableAlreadyExists(String),
    ColumnNotFound(String),
    ConstraintViolation {
        kind: ConstraintKind,
        message: String,
    },
    ParseError(String),
    StorageError(String),
    TypeMismatch(String),
    TransactionError(String),
    AggregateError(String),
    IndexError(String),
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
            RustqlError::ConstraintViolation { kind: _, message } => {
                write!(f, "{}", message)
            }
            RustqlError::ParseError(msg) => write!(f, "{}", msg),
            RustqlError::StorageError(msg) => write!(f, "{}", msg),
            RustqlError::TypeMismatch(msg) => write!(f, "{}", msg),
            RustqlError::TransactionError(msg) => write!(f, "{}", msg),
            RustqlError::AggregateError(msg) => write!(f, "{}", msg),
            RustqlError::IndexError(msg) => write!(f, "{}", msg),
            RustqlError::DivisionByZero => write!(f, "Division by zero"),
            RustqlError::Internal(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for RustqlError {}

pub type Result<T> = std::result::Result<T, RustqlError>;

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
    fn from(s: String) -> Self {
        RustqlError::Internal(s)
    }
}
