//! SQL errors with PostgreSQL SQLSTATE codes (sent in ErrorResponse at
//! step 13).

use crate::access::btree::BTreeError;
use crate::access::btree::key::KeyError;
use crate::access::heap::HeapError;
use crate::access::tuple::TupleError;
use crate::catalog::CatalogError;
use crate::lock::LockError;
use crate::txn::TransactionError;
use crate::types::TypeError;
use crate::vacuum::VacuumError;

/// SQLSTATE codes used by this crate (PostgreSQL's errcodes.txt).
pub mod code {
    pub const FEATURE_NOT_SUPPORTED: &str = "0A000";
    pub const CARDINALITY_VIOLATION: &str = "21000";
    pub const STRING_DATA_RIGHT_TRUNCATION: &str = "22001";
    pub const NUMERIC_VALUE_OUT_OF_RANGE: &str = "22003";
    pub const DATETIME_FIELD_OVERFLOW: &str = "22008";
    pub const DIVISION_BY_ZERO: &str = "22012";
    pub const INVALID_TEXT_REPRESENTATION: &str = "22P02";
    pub const INVALID_DATETIME_FORMAT: &str = "22007";
    pub const INVALID_PARAMETER_VALUE: &str = "22023";
    pub const CHARACTER_NOT_IN_REPERTOIRE: &str = "22021";
    pub const NOT_NULL_VIOLATION: &str = "23502";
    pub const UNIQUE_VIOLATION: &str = "23505";
    pub const CHECK_VIOLATION: &str = "23514";
    pub const DEPENDENT_OBJECTS_STILL_EXIST: &str = "2BP01";
    pub const DUPLICATE_OBJECT: &str = "42710";
    pub const INVALID_TABLE_DEFINITION: &str = "42P16";
    pub const INVALID_OBJECT_DEFINITION: &str = "42P17";
    pub const ACTIVE_SQL_TRANSACTION: &str = "25001";
    pub const NO_ACTIVE_SQL_TRANSACTION: &str = "25P01";
    pub const IN_FAILED_SQL_TRANSACTION: &str = "25P02";
    pub const SERIALIZATION_FAILURE: &str = "40001";
    pub const DEADLOCK_DETECTED: &str = "40P01";
    pub const SYNTAX_ERROR: &str = "42601";
    pub const UNDEFINED_COLUMN: &str = "42703";
    pub const UNDEFINED_TABLE: &str = "42P01";
    pub const UNDEFINED_OBJECT: &str = "42704";
    pub const UNDEFINED_FUNCTION: &str = "42883";
    pub const DUPLICATE_TABLE: &str = "42P07";
    pub const DUPLICATE_COLUMN: &str = "42701";
    pub const AMBIGUOUS_COLUMN: &str = "42702";
    pub const DATATYPE_MISMATCH: &str = "42804";
    pub const GROUPING_ERROR: &str = "42803";
    pub const INVALID_COLUMN_REFERENCE: &str = "42P10";
    pub const DUPLICATE_ALIAS: &str = "42712";
    pub const WRONG_OBJECT_TYPE: &str = "42809";
    pub const CANNOT_COERCE: &str = "42846";
    pub const LOCK_NOT_AVAILABLE: &str = "55P03";
    pub const OUT_OF_MEMORY: &str = "53200";
    pub const FOREIGN_KEY_VIOLATION: &str = "23503";
    pub const INVALID_FOREIGN_KEY: &str = "42830";
    pub const PROGRAM_LIMIT_EXCEEDED: &str = "54000";
    pub const TOO_MANY_COLUMNS: &str = "54011";
    pub const INTERNAL_ERROR: &str = "XX000";
}

/// An error reported to the client: SQLSTATE plus message.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct SqlError {
    pub code: &'static str,
    pub message: String,
}

/// Result alias for the SQL layer.
pub type Result<T> = std::result::Result<T, SqlError>;

impl SqlError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn internal(message: impl std::fmt::Display) -> Self {
        Self::new(code::INTERNAL_ERROR, message.to_string())
    }

    pub fn unsupported(what: impl std::fmt::Display) -> Self {
        Self::new(
            code::FEATURE_NOT_SUPPORTED,
            format!("{what} is not supported"),
        )
    }
}

impl From<LockError> for SqlError {
    fn from(e: LockError) -> Self {
        let c = match e {
            LockError::Timeout => code::LOCK_NOT_AVAILABLE,
            LockError::Deadlock => code::DEADLOCK_DETECTED,
            LockError::OutOfKeyLocks(_) => code::OUT_OF_MEMORY,
        };
        Self::new(c, e.to_string())
    }
}

impl From<crate::txn::TxnError> for SqlError {
    fn from(e: crate::txn::TxnError) -> Self {
        Self::internal(e)
    }
}

impl From<TransactionError> for SqlError {
    fn from(e: TransactionError) -> Self {
        match e {
            TransactionError::Lock(l) => l.into(),
            TransactionError::SerializationFailure => {
                Self::new(code::SERIALIZATION_FAILURE, e.to_string())
            }
            other => Self::internal(other),
        }
    }
}

impl From<HeapError> for SqlError {
    fn from(e: HeapError) -> Self {
        match e {
            HeapError::Transaction(t) => t.into(),
            HeapError::Tuple(t) => t.into(),
            other => Self::internal(other),
        }
    }
}

impl From<TypeError> for SqlError {
    fn from(e: TypeError) -> Self {
        let c = match e {
            TypeError::StringTooLong { .. } => code::STRING_DATA_RIGHT_TRUNCATION,
            TypeError::NulInText | TypeError::InvalidUtf8 => code::CHARACTER_NOT_IN_REPERTOIRE,
            TypeError::ValueTooLarge { .. } => code::FEATURE_NOT_SUPPORTED,
            _ => code::INTERNAL_ERROR,
        };
        Self::new(c, e.to_string())
    }
}

impl From<TupleError> for SqlError {
    fn from(e: TupleError) -> Self {
        match e {
            TupleError::Type(t) => t.into(),
            TupleError::TooLarge { .. } => Self::new(code::PROGRAM_LIMIT_EXCEEDED, e.to_string()),
            TupleError::TooManyAttributes => Self::new(code::TOO_MANY_COLUMNS, e.to_string()),
            other => Self::internal(other),
        }
    }
}

impl From<KeyError> for SqlError {
    fn from(e: KeyError) -> Self {
        match e {
            KeyError::Type(t) => t.into(),
            other => Self::internal(other),
        }
    }
}

impl From<BTreeError> for SqlError {
    fn from(e: BTreeError) -> Self {
        match e {
            BTreeError::Key(k) => k.into(),
            BTreeError::KeyTooLarge { .. } => Self::new(code::FEATURE_NOT_SUPPORTED, e.to_string()),
            BTreeError::External(inner) => match inner.downcast::<HeapError>() {
                Ok(h) => (*h).into(),
                Err(other) => Self::internal(other),
            },
            other => Self::internal(other),
        }
    }
}

impl From<VacuumError> for SqlError {
    fn from(e: VacuumError) -> Self {
        match e {
            VacuumError::Heap(h) => h.into(),
            VacuumError::BTree(b) => b.into(),
            VacuumError::Transaction(t) => t.into(),
        }
    }
}

impl From<CatalogError> for SqlError {
    fn from(e: CatalogError) -> Self {
        let msg = e.to_string();
        match e {
            CatalogError::Heap(h) => h.into(),
            CatalogError::BTree(b) => b.into(),
            CatalogError::Tuple(t) => t.into(),
            CatalogError::Key(k) => k.into(),
            CatalogError::Transaction(t) => t.into(),
            CatalogError::Vacuum(v) => v.into(),
            CatalogError::DuplicateRelation(_) => Self::new(code::DUPLICATE_TABLE, msg),
            CatalogError::UndefinedRelation(_) => Self::new(code::UNDEFINED_TABLE, msg),
            CatalogError::UniqueViolation(_) => Self::new(code::UNIQUE_VIOLATION, msg),
            CatalogError::UndefinedColumn(_) => Self::new(code::UNDEFINED_COLUMN, msg),
            CatalogError::DuplicateConstraint { .. } => Self::new(code::DUPLICATE_OBJECT, msg),
            CatalogError::MultiplePrimaryKeys(_) => Self::new(code::INVALID_TABLE_DEFINITION, msg),
            CatalogError::KeyColumnMissing(_) => Self::new(code::UNDEFINED_COLUMN, msg),
            CatalogError::InvalidForeignKey(_) => Self::new(code::INVALID_FOREIGN_KEY, msg),
            CatalogError::ForeignKeyTypes { .. } => Self::new(code::DATATYPE_MISMATCH, msg),
            CatalogError::DependentObjects { .. } => {
                Self::new(code::DEPENDENT_OBJECTS_STILL_EXIST, msg)
            }
            CatalogError::IndexRequiredByConstraint { .. } => {
                Self::new(code::DEPENDENT_OBJECTS_STILL_EXIST, msg)
            }
            CatalogError::Recovery(_) | CatalogError::Corrupt(_) => Self::internal(msg),
        }
    }
}

impl From<sqlparser::parser::ParserError> for SqlError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        let msg = match e {
            sqlparser::parser::ParserError::TokenizerError(m)
            | sqlparser::parser::ParserError::ParserError(m) => m,
            sqlparser::parser::ParserError::RecursionLimitExceeded => {
                "statement is too deeply nested".to_owned()
            }
        };
        Self::new(code::SYNTAX_ERROR, format!("syntax error: {msg}"))
    }
}
