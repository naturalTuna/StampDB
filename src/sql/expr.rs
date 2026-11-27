//! Bound scalar expressions and their evaluation (PostgreSQL semantics:
//! three-valued logic, overflow and division-by-zero errors, C collation).
//!
//! The binder resolves names and types; an [`Expr`] refers to input columns
//! by position and its operands already have matching types.

use std::cmp::Ordering;

use super::datum::{format_value, parse_value};
use super::error::{Result, SqlError, code};
use crate::types::{DataType, Value};

/// Binary operators. Operand types match (the binder inserts casts).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    /// Text concatenation (`||`).
    Concat,
}

impl BinOp {
    pub fn is_comparison(self) -> bool {
        matches!(
            self,
            BinOp::Eq | BinOp::NotEq | BinOp::Lt | BinOp::LtEq | BinOp::Gt | BinOp::GtEq
        )
    }

    /// The operator as written in SQL.
    pub fn symbol(self) -> &'static str {
        match self {
            BinOp::Add => "+",
            BinOp::Sub => "-",
            BinOp::Mul => "*",
            BinOp::Div => "/",
            BinOp::Mod => "%",
            BinOp::Eq => "=",
            BinOp::NotEq => "<>",
            BinOp::Lt => "<",
            BinOp::LtEq => "<=",
            BinOp::Gt => ">",
            BinOp::GtEq => ">=",
            BinOp::Concat => "||",
        }
    }

    /// `a op b` == `b op.commute() a`.
    pub fn commute(self) -> Self {
        match self {
            BinOp::Lt => BinOp::Gt,
            BinOp::LtEq => BinOp::GtEq,
            BinOp::Gt => BinOp::Lt,
            BinOp::GtEq => BinOp::LtEq,
            other => other,
        }
    }
}

/// How a cast was requested (PostgreSQL's CoercionContext).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CastMode {
    /// `CAST(x AS t)` / `x::t`: VARCHAR(n) truncates.
    Explicit,
    /// Storing into a column: VARCHAR(n) rejects longer values (except
    /// trailing spaces).
    Assignment,
}

/// Built-in scalar functions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Func {
    Abs,
    Length,
    Lower,
    Upper,
}

/// A bound expression evaluated against one input row.
#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Const(Value),
    /// Input column by position.
    Column(usize),
    Neg(Box<Expr>),
    Binary {
        op: BinOp,
        l: Box<Expr>,
        r: Box<Expr>,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
    IsNull {
        arg: Box<Expr>,
        negated: bool,
    },
    Cast {
        arg: Box<Expr>,
        to: DataType,
        mode: CastMode,
    },
    /// Searched CASE (a simple CASE is bound to this with equalities).
    Case {
        whens: Vec<(Expr, Expr)>,
        otherwise: Option<Box<Expr>>,
    },
    Like {
        arg: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
        case_insensitive: bool,
    },
    InList {
        arg: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },
    /// First non-null argument.
    Coalesce(Vec<Expr>),
    Func {
        func: Func,
        args: Vec<Expr>,
    },
}

fn overflow(ty: &str) -> SqlError {
    SqlError::new(
        code::NUMERIC_VALUE_OUT_OF_RANGE,
        format!("{ty} out of range"),
    )
}

fn div_zero() -> SqlError {
    SqlError::new(code::DIVISION_BY_ZERO, "division by zero")
}

/// Orders two non-null values of the same type (C collation for text; NaN
/// above all other floats, as in PostgreSQL). `None` for mixed types.
pub fn compare(a: &Value, b: &Value) -> Option<Ordering> {
    Some(match (a, b) {
        (Value::Int4(x), Value::Int4(y)) => x.cmp(y),
        (Value::Int8(x), Value::Int8(y)) | (Value::Timestamp(x), Value::Timestamp(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Text(x), Value::Text(y)) => x.as_bytes().cmp(y.as_bytes()),
        (Value::Float8(x), Value::Float8(y)) => match (x.is_nan(), y.is_nan()) {
            (true, true) => Ordering::Equal,
            (true, false) => Ordering::Greater,
            (false, true) => Ordering::Less,
            (false, false) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        },
        _ => return None,
    })
}

/// Orders values for ORDER BY: NULLs last ascending (first descending,
/// via the caller reversing).
pub fn compare_nulls_last(a: &Value, b: &Value) -> Ordering {
    match (a.is_null(), b.is_null()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => compare(a, b).unwrap_or(Ordering::Equal),
    }
}

fn float_result(r: f64, a: f64, b: f64, zero_ok: bool) -> Result<Value> {
    if r.is_infinite() && !a.is_infinite() && !b.is_infinite() {
        return Err(SqlError::new(
            code::NUMERIC_VALUE_OUT_OF_RANGE,
            "value out of range: overflow",
        ));
    }
    if r == 0.0 && !zero_ok {
        return Err(SqlError::new(
            code::NUMERIC_VALUE_OUT_OF_RANGE,
            "value out of range: underflow",
        ));
    }
    Ok(Value::Float8(r))
}

fn arith(op: BinOp, a: &Value, b: &Value) -> Result<Value> {
    Ok(match (a, b) {
        (Value::Int4(x), Value::Int4(y)) => Value::Int4(
            match op {
                BinOp::Add => x.checked_add(*y),
                BinOp::Sub => x.checked_sub(*y),
                BinOp::Mul => x.checked_mul(*y),
                BinOp::Div if *y == 0 => return Err(div_zero()),
                BinOp::Div => x.checked_div(*y),
                BinOp::Mod if *y == 0 => return Err(div_zero()),
                // INT_MIN % -1 is 0 in PostgreSQL.
                BinOp::Mod => Some(x.checked_rem(*y).unwrap_or(0)),
                _ => None,
            }
            .ok_or_else(|| overflow("integer"))?,
        ),
        (Value::Int8(x), Value::Int8(y)) => Value::Int8(
            match op {
                BinOp::Add => x.checked_add(*y),
                BinOp::Sub => x.checked_sub(*y),
                BinOp::Mul => x.checked_mul(*y),
                BinOp::Div if *y == 0 => return Err(div_zero()),
                BinOp::Div => x.checked_div(*y),
                BinOp::Mod if *y == 0 => return Err(div_zero()),
                BinOp::Mod => Some(x.checked_rem(*y).unwrap_or(0)),
                _ => None,
            }
            .ok_or_else(|| overflow("bigint"))?,
        ),
        (Value::Float8(x), Value::Float8(y)) => {
            let (x, y) = (*x, *y);
            match op {
                BinOp::Add => float_result(x + y, x, y, true)?,
                BinOp::Sub => float_result(x - y, x, y, true)?,
                BinOp::Mul => float_result(x * y, x, y, x == 0.0 || y == 0.0)?,
                BinOp::Div if y == 0.0 => return Err(div_zero()),
                BinOp::Div => float_result(x / y, x, y, x == 0.0 || y.is_infinite())?,
                _ => return Err(SqlError::internal("bad float operator")),
            }
        }
        _ => return Err(SqlError::internal("arithmetic on mismatched types")),
    })
}

/// LIKE matching with `%`, `_`, and backslash escapes (on chars).
fn like(s: &[char], p: &[char]) -> bool {
    let Some((&c, rest)) = p.split_first() else {
        return s.is_empty();
    };
    match c {
        '%' => (0..=s.len()).any(|i| like(&s[i..], rest)),
        '_' => !s.is_empty() && like(&s[1..], rest),
        '\\' => match rest.split_first() {
            Some((&e, rest2)) => s.first() == Some(&e) && like(&s[1..], rest2),
            None => s.first() == Some(&'\\') && s.len() == 1,
        },
        _ => s.first() == Some(&c) && like(&s[1..], rest),
    }
}

/// Converts `v` to `to` (PostgreSQL's cast functions; the binder only
/// creates casts that exist).
pub fn cast(v: Value, to: DataType, mode: CastMode) -> Result<Value> {
    if v.is_null() {
        return Ok(Value::Null);
    }
    Ok(match (v, to) {
        (Value::Text(s), DataType::Varchar(Some(n))) => {
            let n = n as usize;
            match s.char_indices().nth(n) {
                None => Value::Text(s),
                Some((i, _)) => {
                    if mode == CastMode::Assignment && s[i..].chars().any(|c| c != ' ') {
                        return Err(SqlError::new(
                            code::STRING_DATA_RIGHT_TRUNCATION,
                            format!("value too long for type character varying({n})"),
                        ));
                    }
                    Value::Text(s[..i].to_owned())
                }
            }
        }
        (Value::Text(s), DataType::Text | DataType::Varchar(None)) => Value::Text(s),
        (Value::Text(s), ty) => parse_value(ty, &s)?,
        // bool's text cast spells the word out (booltext), unlike boolout.
        (Value::Bool(b), DataType::Text | DataType::Varchar(_)) => {
            let s = if b { "true" } else { "false" };
            cast(Value::Text(s.to_owned()), to, mode)?
        }
        (v, DataType::Text | DataType::Varchar(_)) => {
            let s = format_value(&v).unwrap_or_default();
            cast(Value::Text(s), to, mode)?
        }
        (Value::Int4(x), DataType::Int4) => Value::Int4(x),
        (Value::Int4(x), DataType::Int8) => Value::Int8(i64::from(x)),
        (Value::Int4(x), DataType::Float8) => Value::Float8(f64::from(x)),
        (Value::Int4(x), DataType::Bool) => Value::Bool(x != 0),
        (Value::Int8(x), DataType::Int8) => Value::Int8(x),
        (Value::Int8(x), DataType::Int4) => {
            Value::Int4(i32::try_from(x).map_err(|_| overflow("integer"))?)
        }
        (Value::Int8(x), DataType::Float8) => Value::Float8(x as f64),
        (Value::Float8(x), DataType::Float8) => Value::Float8(x),
        (Value::Float8(x), DataType::Int4) => {
            let r = x.round_ties_even();
            if r.is_nan() || !(-2147483648.0..2147483648.0).contains(&r) {
                return Err(overflow("integer"));
            }
            Value::Int4(r as i32)
        }
        (Value::Float8(x), DataType::Int8) => {
            let r = x.round_ties_even();
            if r.is_nan() || !(-9.223372036854776e18..9.223372036854776e18).contains(&r) {
                return Err(overflow("bigint"));
            }
            Value::Int8(r as i64)
        }
        (Value::Bool(b), DataType::Bool) => Value::Bool(b),
        (Value::Bool(b), DataType::Int4) => Value::Int4(i32::from(b)),
        (Value::Timestamp(t), DataType::Timestamp) => Value::Timestamp(t),
        (v, to) => {
            return Err(SqlError::new(
                code::CANNOT_COERCE,
                format!("cannot cast {v:?} to {to}"),
            ));
        }
    })
}

/// Truth value of a predicate result: only TRUE passes.
pub fn is_true(v: &Value) -> bool {
    matches!(v, Value::Bool(true))
}

impl Expr {
    pub fn col(i: usize) -> Self {
        Expr::Column(i)
    }

    pub fn eval(&self, row: &[Value]) -> Result<Value> {
        Ok(match self {
            Expr::Const(v) => v.clone(),
            Expr::Column(i) => row
                .get(*i)
                .cloned()
                .ok_or_else(|| SqlError::internal("column index out of range"))?,
            Expr::Neg(a) => match a.eval(row)? {
                Value::Null => Value::Null,
                Value::Int4(x) => Value::Int4(x.checked_neg().ok_or_else(|| overflow("integer"))?),
                Value::Int8(x) => Value::Int8(x.checked_neg().ok_or_else(|| overflow("bigint"))?),
                Value::Float8(x) => Value::Float8(-x),
                _ => return Err(SqlError::internal("negation of a non-number")),
            },
            Expr::Binary { op, l, r } => {
                let a = l.eval(row)?;
                let b = r.eval(row)?;
                if a.is_null() || b.is_null() {
                    return Ok(Value::Null);
                }
                match op {
                    BinOp::Concat => match (a, b) {
                        (Value::Text(mut x), Value::Text(y)) => {
                            x.push_str(&y);
                            Value::Text(x)
                        }
                        _ => return Err(SqlError::internal("|| on non-text")),
                    },
                    op if op.is_comparison() => {
                        let o = compare(&a, &b)
                            .ok_or_else(|| SqlError::internal("comparison of mismatched types"))?;
                        Value::Bool(match op {
                            BinOp::Eq => o == Ordering::Equal,
                            BinOp::NotEq => o != Ordering::Equal,
                            BinOp::Lt => o == Ordering::Less,
                            BinOp::LtEq => o != Ordering::Greater,
                            BinOp::Gt => o == Ordering::Greater,
                            _ => o != Ordering::Less,
                        })
                    }
                    op => arith(*op, &a, &b)?,
                }
            }
            Expr::And(l, r) => match l.eval(row)? {
                Value::Bool(false) => Value::Bool(false),
                lv => match (lv, r.eval(row)?) {
                    (_, Value::Bool(false)) => Value::Bool(false),
                    (Value::Bool(true), Value::Bool(true)) => Value::Bool(true),
                    _ => Value::Null,
                },
            },
            Expr::Or(l, r) => match l.eval(row)? {
                Value::Bool(true) => Value::Bool(true),
                lv => match (lv, r.eval(row)?) {
                    (_, Value::Bool(true)) => Value::Bool(true),
                    (Value::Bool(false), Value::Bool(false)) => Value::Bool(false),
                    _ => Value::Null,
                },
            },
            Expr::Not(a) => match a.eval(row)? {
                Value::Bool(b) => Value::Bool(!b),
                _ => Value::Null,
            },
            Expr::IsNull { arg, negated } => Value::Bool(arg.eval(row)?.is_null() != *negated),
            Expr::Cast { arg, to, mode } => cast(arg.eval(row)?, *to, *mode)?,
            Expr::Case { whens, otherwise } => {
                for (cond, val) in whens {
                    if is_true(&cond.eval(row)?) {
                        return val.eval(row);
                    }
                }
                match otherwise {
                    Some(e) => e.eval(row)?,
                    None => Value::Null,
                }
            }
            Expr::Like {
                arg,
                pattern,
                negated,
                case_insensitive,
            } => match (arg.eval(row)?, pattern.eval(row)?) {
                (Value::Text(s), Value::Text(p)) => {
                    let (s, p) = if *case_insensitive {
                        (s.to_ascii_lowercase(), p.to_ascii_lowercase())
                    } else {
                        (s, p)
                    };
                    let s: Vec<char> = s.chars().collect();
                    let p: Vec<char> = p.chars().collect();
                    Value::Bool(like(&s, &p) != *negated)
                }
                _ => Value::Null,
            },
            Expr::InList { arg, list, negated } => {
                let a = arg.eval(row)?;
                if a.is_null() {
                    return Ok(Value::Null);
                }
                let mut saw_null = false;
                for e in list {
                    let v = e.eval(row)?;
                    if v.is_null() {
                        saw_null = true;
                    } else if compare(&a, &v) == Some(Ordering::Equal) {
                        return Ok(Value::Bool(!negated));
                    }
                }
                if saw_null {
                    Value::Null
                } else {
                    Value::Bool(*negated)
                }
            }
            Expr::Coalesce(args) => {
                for e in args {
                    let v = e.eval(row)?;
                    if !v.is_null() {
                        return Ok(v);
                    }
                }
                Value::Null
            }
            Expr::Func { func, args } => {
                let vals = args
                    .iter()
                    .map(|a| a.eval(row))
                    .collect::<Result<Vec<_>>>()?;
                if vals.iter().any(Value::is_null) {
                    return Ok(Value::Null);
                }
                match (func, vals.as_slice()) {
                    (Func::Abs, [Value::Int4(x)]) => {
                        Value::Int4(x.checked_abs().ok_or_else(|| overflow("integer"))?)
                    }
                    (Func::Abs, [Value::Int8(x)]) => {
                        Value::Int8(x.checked_abs().ok_or_else(|| overflow("bigint"))?)
                    }
                    (Func::Abs, [Value::Float8(x)]) => Value::Float8(x.abs()),
                    (Func::Length, [Value::Text(s)]) => Value::Int4(s.chars().count() as i32),
                    (Func::Lower, [Value::Text(s)]) => Value::Text(s.to_ascii_lowercase()),
                    (Func::Upper, [Value::Text(s)]) => Value::Text(s.to_ascii_uppercase()),
                    _ => return Err(SqlError::internal("bad function arguments")),
                }
            }
        })
    }

    /// Whether the expression reads no input columns.
    pub fn is_constant(&self) -> bool {
        let mut cols = false;
        self.walk(&mut |e| cols |= matches!(e, Expr::Column(_)));
        !cols
    }

    /// Calls `f` on this expression and every subexpression.
    pub fn walk(&self, f: &mut dyn FnMut(&Expr)) {
        f(self);
        match self {
            Expr::Const(_) | Expr::Column(_) => {}
            Expr::Neg(a) | Expr::Not(a) => a.walk(f),
            Expr::IsNull { arg, .. } | Expr::Cast { arg, .. } => arg.walk(f),
            Expr::Binary { l, r, .. } | Expr::And(l, r) | Expr::Or(l, r) => {
                l.walk(f);
                r.walk(f);
            }
            Expr::Case { whens, otherwise } => {
                for (c, v) in whens {
                    c.walk(f);
                    v.walk(f);
                }
                if let Some(o) = otherwise {
                    o.walk(f);
                }
            }
            Expr::Like { arg, pattern, .. } => {
                arg.walk(f);
                pattern.walk(f);
            }
            Expr::InList { arg, list, .. } => {
                arg.walk(f);
                for e in list {
                    e.walk(f);
                }
            }
            Expr::Coalesce(args) | Expr::Func { args, .. } => {
                for e in args {
                    e.walk(f);
                }
            }
        }
    }

    /// Rewrites column references with `f` (e.g. to shift them when the
    /// input row layout changes).
    pub fn map_columns(&self, f: &dyn Fn(usize) -> usize) -> Expr {
        let m = |e: &Expr| Box::new(e.map_columns(f));
        match self {
            Expr::Const(v) => Expr::Const(v.clone()),
            Expr::Column(i) => Expr::Column(f(*i)),
            Expr::Neg(a) => Expr::Neg(m(a)),
            Expr::Not(a) => Expr::Not(m(a)),
            Expr::IsNull { arg, negated } => Expr::IsNull {
                arg: m(arg),
                negated: *negated,
            },
            Expr::Cast { arg, to, mode } => Expr::Cast {
                arg: m(arg),
                to: *to,
                mode: *mode,
            },
            Expr::Binary { op, l, r } => Expr::Binary {
                op: *op,
                l: m(l),
                r: m(r),
            },
            Expr::And(l, r) => Expr::And(m(l), m(r)),
            Expr::Or(l, r) => Expr::Or(m(l), m(r)),
            Expr::Case { whens, otherwise } => Expr::Case {
                whens: whens
                    .iter()
                    .map(|(c, v)| (c.map_columns(f), v.map_columns(f)))
                    .collect(),
                otherwise: otherwise.as_ref().map(|o| m(o)),
            },
            Expr::Like {
                arg,
                pattern,
                negated,
                case_insensitive,
            } => Expr::Like {
                arg: m(arg),
                pattern: m(pattern),
                negated: *negated,
                case_insensitive: *case_insensitive,
            },
            Expr::InList { arg, list, negated } => Expr::InList {
                arg: m(arg),
                list: list.iter().map(|e| e.map_columns(f)).collect(),
                negated: *negated,
            },
            Expr::Coalesce(args) => Expr::Coalesce(args.iter().map(|e| e.map_columns(f)).collect()),
            Expr::Func { func, args } => Expr::Func {
                func: *func,
                args: args.iter().map(|e| e.map_columns(f)).collect(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(v: Value) -> Box<Expr> {
        Box::new(Expr::Const(v))
    }

    fn bin(op: BinOp, a: Value, b: Value) -> Result<Value> {
        Expr::Binary {
            op,
            l: c(a),
            r: c(b),
        }
        .eval(&[])
    }

    #[test]
    fn integer_arithmetic_and_errors() {
        use Value::*;
        assert_eq!(bin(BinOp::Div, Int4(7), Int4(2)).unwrap(), Int4(3));
        assert_eq!(bin(BinOp::Div, Int4(-7), Int4(2)).unwrap(), Int4(-3));
        assert_eq!(bin(BinOp::Mod, Int4(-7), Int4(2)).unwrap(), Int4(-1));
        assert_eq!(bin(BinOp::Mod, Int4(i32::MIN), Int4(-1)).unwrap(), Int4(0));
        let e = bin(BinOp::Add, Int4(i32::MAX), Int4(1)).unwrap_err();
        assert_eq!(
            (e.code, e.message.as_str()),
            (code::NUMERIC_VALUE_OUT_OF_RANGE, "integer out of range")
        );
        assert_eq!(
            bin(BinOp::Div, Int4(i32::MIN), Int4(-1)).unwrap_err().code,
            code::NUMERIC_VALUE_OUT_OF_RANGE
        );
        assert_eq!(
            bin(BinOp::Mul, Int8(i64::MAX), Int8(2))
                .unwrap_err()
                .message,
            "bigint out of range"
        );
        let e = bin(BinOp::Div, Int8(1), Int8(0)).unwrap_err();
        assert_eq!(
            (e.code, e.message.as_str()),
            (code::DIVISION_BY_ZERO, "division by zero")
        );
        assert_eq!(bin(BinOp::Add, Null, Int4(1)).unwrap(), Null);
        let neg = Expr::Neg(c(Int4(i32::MIN))).eval(&[]);
        assert!(neg.is_err());
    }

    #[test]
    fn float_arithmetic() {
        use Value::*;
        assert_eq!(
            bin(BinOp::Div, Float8(1.0), Float8(4.0)).unwrap(),
            Float8(0.25)
        );
        assert_eq!(
            bin(BinOp::Div, Float8(1.0), Float8(0.0)).unwrap_err().code,
            code::DIVISION_BY_ZERO
        );
        assert_eq!(
            bin(BinOp::Mul, Float8(1e300), Float8(1e300))
                .unwrap_err()
                .message,
            "value out of range: overflow"
        );
        assert_eq!(
            bin(BinOp::Mul, Float8(1e-300), Float8(1e-300))
                .unwrap_err()
                .message,
            "value out of range: underflow"
        );
        assert_eq!(
            bin(BinOp::Add, Float8(f64::INFINITY), Float8(1.0)).unwrap(),
            Float8(f64::INFINITY)
        );
        assert_eq!(
            bin(BinOp::Eq, Float8(f64::NAN), Float8(f64::NAN)).unwrap(),
            Bool(true)
        );
        assert_eq!(
            bin(BinOp::Gt, Float8(f64::NAN), Float8(f64::INFINITY)).unwrap(),
            Bool(true)
        );
        assert_eq!(
            bin(BinOp::Eq, Float8(0.0), Float8(-0.0)).unwrap(),
            Bool(true)
        );
    }

    #[test]
    fn three_valued_logic() {
        use Value::*;
        let and = |a, b| Expr::And(c(a), c(b)).eval(&[]).unwrap();
        let or = |a, b| Expr::Or(c(a), c(b)).eval(&[]).unwrap();
        assert_eq!(and(Null, Bool(false)), Bool(false));
        assert_eq!(and(Null, Bool(true)), Null);
        assert_eq!(and(Bool(true), Bool(true)), Bool(true));
        assert_eq!(or(Null, Bool(true)), Bool(true));
        assert_eq!(or(Null, Bool(false)), Null);
        assert_eq!(Expr::Not(c(Null)).eval(&[]).unwrap(), Null);
        // Short circuit: the right side is not evaluated.
        let boom = Expr::Binary {
            op: BinOp::Eq,
            l: Box::new(Expr::Binary {
                op: BinOp::Div,
                l: c(Int4(1)),
                r: c(Int4(0)),
            }),
            r: c(Int4(1)),
        };
        assert_eq!(
            Expr::And(c(Bool(false)), Box::new(boom.clone()))
                .eval(&[])
                .unwrap(),
            Bool(false)
        );
        assert!(Expr::And(c(Bool(true)), Box::new(boom)).eval(&[]).is_err());
        let inl = |a, list: Vec<Value>| {
            Expr::InList {
                arg: c(a),
                list: list.into_iter().map(Expr::Const).collect(),
                negated: false,
            }
            .eval(&[])
            .unwrap()
        };
        assert_eq!(inl(Int4(1), vec![Int4(2), Int4(1)]), Bool(true));
        assert_eq!(inl(Int4(1), vec![Int4(2), Null]), Null);
        assert_eq!(inl(Int4(1), vec![Int4(2)]), Bool(false));
        assert_eq!(inl(Null, vec![Int4(2)]), Null);
    }

    #[test]
    fn casts() {
        use Value::*;
        let x = |v, t| cast(v, t, CastMode::Explicit);
        assert_eq!(x(Float8(2.5), DataType::Int4).unwrap(), Int4(2));
        assert_eq!(x(Float8(3.5), DataType::Int4).unwrap(), Int4(4));
        assert_eq!(x(Float8(-2.5), DataType::Int8).unwrap(), Int8(-2));
        assert!(x(Float8(3e9), DataType::Int4).is_err());
        assert!(x(Int8(1 << 40), DataType::Int4).is_err());
        assert_eq!(x(Text(" 12 ".into()), DataType::Int4).unwrap(), Int4(12));
        assert_eq!(x(Float8(0.1), DataType::Text).unwrap(), Text("0.1".into()));
        assert_eq!(x(Bool(true), DataType::Text).unwrap(), Text("true".into()));
        assert_eq!(
            x(Text("abcdef".into()), DataType::Varchar(Some(3))).unwrap(),
            Text("abc".into())
        );
        let a = cast(
            Text("abcdef".into()),
            DataType::Varchar(Some(3)),
            CastMode::Assignment,
        );
        assert_eq!(a.unwrap_err().code, code::STRING_DATA_RIGHT_TRUNCATION);
        let a = cast(
            Text("ab   ".into()),
            DataType::Varchar(Some(3)),
            CastMode::Assignment,
        );
        assert_eq!(a.unwrap(), Text("ab ".into()));
        assert_eq!(x(Null, DataType::Int4).unwrap(), Null);
    }

    #[test]
    fn like_patterns() {
        let l = |s: &str, p: &str| {
            let s: Vec<char> = s.chars().collect();
            let p: Vec<char> = p.chars().collect();
            like(&s, &p)
        };
        assert!(l("hello", "h%o"));
        assert!(l("hello", "_ello"));
        assert!(!l("hello", "_llo"));
        assert!(l("", "%"));
        assert!(l("a%b", "a\\%b"));
        assert!(!l("axb", "a\\%b"));
        assert!(l("한글", "_글"));
    }
}
