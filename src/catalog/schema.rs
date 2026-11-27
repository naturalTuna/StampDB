//! System catalog layout: relation ids, row formats, and index keys.
//!
//! Relation ids borrow PostgreSQL's OIDs for the corresponding catalogs.
//! OIDs are stored as INT8; an index's column list (`indkey`) is TEXT in
//! the form of PostgreSQL's int2vector output ("1 3").

use crate::access::tuple::TupleDesc;
use crate::storage::backend::RelFileId;
use crate::types::{DataType, Value};

use super::{CatalogError, Result};

pub const SYS_CLASS: RelFileId = RelFileId(1259);
pub const SYS_ATTRIBUTE: RelFileId = RelFileId(1249);
pub const SYS_INDEX: RelFileId = RelFileId(2610);
pub const SYS_CLASS_OID_INDEX: RelFileId = RelFileId(2662);
pub const SYS_CLASS_RELNAME_INDEX: RelFileId = RelFileId(2663);
pub const SYS_ATTRIBUTE_RELID_ATTNUM_INDEX: RelFileId = RelFileId(2659);
pub const SYS_INDEX_INDRELID_INDEX: RelFileId = RelFileId(2679);
pub const SYS_CONSTRAINT: RelFileId = RelFileId(2606);
pub const SYS_CONSTRAINT_CONRELID_INDEX: RelFileId = RelFileId(2665);

/// `relkind` values.
pub const RELKIND_TABLE: &str = "r";
pub const RELKIND_INDEX: &str = "i";

/// `contype` values.
pub const CONTYPE_PRIMARY: &str = "p";
pub const CONTYPE_UNIQUE: &str = "u";
pub const CONTYPE_CHECK: &str = "c";

/// sys_class: oid, relname, relkind, relfilenode, relnatts.
pub fn class_desc() -> TupleDesc {
    TupleDesc {
        attrs: vec![
            DataType::Int8,
            DataType::Text,
            DataType::Text,
            DataType::Int8,
            DataType::Int4,
        ],
    }
}

/// sys_attribute: attrelid, attnum, attname, atttypid, atttypmod,
/// attnotnull, attdefault (DEFAULT expression as SQL text, or NULL).
pub fn attribute_desc() -> TupleDesc {
    TupleDesc {
        attrs: vec![
            DataType::Int8,
            DataType::Int4,
            DataType::Text,
            DataType::Int4,
            DataType::Int4,
            DataType::Bool,
            DataType::Text,
        ],
    }
}

/// sys_constraint: oid, conname, contype, conrelid, conindid (0 for
/// CHECK), conkey (column list as in indkey), consrc (CHECK expression as
/// SQL text, or NULL).
pub fn constraint_desc() -> TupleDesc {
    TupleDesc {
        attrs: vec![
            DataType::Int8,
            DataType::Text,
            DataType::Text,
            DataType::Int8,
            DataType::Int8,
            DataType::Text,
            DataType::Text,
        ],
    }
}

/// sys_index: indexrelid, indrelid, indkey, indisunique, indisprimary.
pub fn index_desc() -> TupleDesc {
    TupleDesc {
        attrs: vec![
            DataType::Int8,
            DataType::Int8,
            DataType::Text,
            DataType::Bool,
            DataType::Bool,
        ],
    }
}

/// A system index: its relation, the catalog it indexes, the indexed
/// columns (1-based), and whether it is unique.
#[derive(Debug, Clone, Copy)]
pub struct SysIndex {
    pub rel: RelFileId,
    pub name: &'static str,
    pub table: RelFileId,
    pub columns: &'static [u16],
    pub unique: bool,
}

pub const SYS_INDEXES: [SysIndex; 5] = [
    SysIndex {
        rel: SYS_CLASS_OID_INDEX,
        name: "sys_class_oid_index",
        table: SYS_CLASS,
        columns: &[1],
        unique: true,
    },
    SysIndex {
        rel: SYS_CLASS_RELNAME_INDEX,
        name: "sys_class_relname_index",
        table: SYS_CLASS,
        columns: &[2],
        unique: true,
    },
    SysIndex {
        rel: SYS_ATTRIBUTE_RELID_ATTNUM_INDEX,
        name: "sys_attribute_relid_attnum_index",
        table: SYS_ATTRIBUTE,
        columns: &[1, 2],
        unique: true,
    },
    SysIndex {
        rel: SYS_INDEX_INDRELID_INDEX,
        name: "sys_index_indrelid_index",
        table: SYS_INDEX,
        columns: &[2],
        unique: false,
    },
    SysIndex {
        rel: SYS_CONSTRAINT_CONRELID_INDEX,
        name: "sys_constraint_conrelid_index",
        table: SYS_CONSTRAINT,
        columns: &[4],
        unique: false,
    },
];

/// A system table: relation id, name, and (column name, type) pairs.
pub type SysTable = (RelFileId, &'static str, Vec<(&'static str, DataType)>);

/// The system tables with their column names and types.
pub fn sys_tables() -> [SysTable; 4] {
    [
        (
            SYS_CLASS,
            "sys_class",
            vec![
                ("oid", DataType::Int8),
                ("relname", DataType::Text),
                ("relkind", DataType::Text),
                ("relfilenode", DataType::Int8),
                ("relnatts", DataType::Int4),
            ],
        ),
        (
            SYS_ATTRIBUTE,
            "sys_attribute",
            vec![
                ("attrelid", DataType::Int8),
                ("attnum", DataType::Int4),
                ("attname", DataType::Text),
                ("atttypid", DataType::Int4),
                ("atttypmod", DataType::Int4),
                ("attnotnull", DataType::Bool),
                ("attdefault", DataType::Text),
            ],
        ),
        (
            SYS_INDEX,
            "sys_index",
            vec![
                ("indexrelid", DataType::Int8),
                ("indrelid", DataType::Int8),
                ("indkey", DataType::Text),
                ("indisunique", DataType::Bool),
                ("indisprimary", DataType::Bool),
            ],
        ),
        (
            SYS_CONSTRAINT,
            "sys_constraint",
            vec![
                ("oid", DataType::Int8),
                ("conname", DataType::Text),
                ("contype", DataType::Text),
                ("conrelid", DataType::Int8),
                ("conindid", DataType::Int8),
                ("conkey", DataType::Text),
                ("consrc", DataType::Text),
            ],
        ),
    ]
}

/// (type OID, typmod) of a data type, as stored in sys_attribute.
pub fn type_to_oid(ty: DataType) -> (i32, i32) {
    let typmod = match ty {
        DataType::Varchar(Some(n)) => n as i32,
        _ => -1,
    };
    (ty.oid() as i32, typmod)
}

/// The data type for a (type OID, typmod) pair.
pub fn oid_to_type(oid: i32, typmod: i32) -> Result<DataType> {
    Ok(match oid {
        16 => DataType::Bool,
        20 => DataType::Int8,
        23 => DataType::Int4,
        25 => DataType::Text,
        701 => DataType::Float8,
        1043 => DataType::Varchar((typmod >= 0).then_some(typmod as u32)),
        1114 => DataType::Timestamp,
        _ => return Err(CatalogError::Corrupt("unknown type oid")),
    })
}

/// Formats an index column list as stored in `indkey`.
pub fn format_indkey(columns: &[u16]) -> String {
    columns
        .iter()
        .map(u16::to_string)
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn parse_indkey(s: &str) -> Result<Vec<u16>> {
    s.split_whitespace()
        .map(|c| c.parse().map_err(|_| CatalogError::Corrupt("bad indkey")))
        .collect()
}

/// Reads column `i` of a catalog row.
pub fn int(v: &[Value], i: usize) -> Result<i64> {
    match v.get(i) {
        Some(Value::Int8(x)) => Ok(*x),
        Some(Value::Int4(x)) => Ok(i64::from(*x)),
        _ => Err(CatalogError::Corrupt("expected an integer column")),
    }
}

/// Reads a nullable text column.
pub fn opt_text(v: &[Value], i: usize) -> Result<Option<String>> {
    match v.get(i) {
        Some(Value::Null) => Ok(None),
        Some(Value::Text(s)) => Ok(Some(s.clone())),
        _ => Err(CatalogError::Corrupt("expected a text column")),
    }
}

pub fn text(v: &[Value], i: usize) -> Result<String> {
    match v.get(i) {
        Some(Value::Text(s)) => Ok(s.clone()),
        _ => Err(CatalogError::Corrupt("expected a text column")),
    }
}

pub fn boolean(v: &[Value], i: usize) -> Result<bool> {
    match v.get(i) {
        Some(Value::Bool(b)) => Ok(*b),
        _ => Err(CatalogError::Corrupt("expected a boolean column")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_oids_roundtrip() {
        for ty in [
            DataType::Bool,
            DataType::Int4,
            DataType::Int8,
            DataType::Float8,
            DataType::Timestamp,
            DataType::Text,
            DataType::Varchar(None),
            DataType::Varchar(Some(30)),
        ] {
            let (oid, m) = type_to_oid(ty);
            assert_eq!(oid_to_type(oid, m).unwrap(), ty);
        }
        assert!(oid_to_type(9999, -1).is_err());
    }

    #[test]
    fn indkey_roundtrip() {
        assert_eq!(format_indkey(&[1, 3]), "1 3");
        assert_eq!(parse_indkey("1 3").unwrap(), vec![1, 3]);
        assert!(parse_indkey("x").is_err());
    }
}
