//! Plain data for what an event must carry whose shape belongs to someone
//! else — a scheduled job's fields. Small, self-describing and ordered: maps
//! are `Vec`s, never hash maps, so equal values compare and print equal.
//!
//! A job is stored as `(kind, Value)` rather than as bytes because a domain's
//! events must stay `Clone + PartialEq + Debug` and readable in a test
//! failure, and because annals-sim has no encoder to depend on. A storage format
//! encodes a `Value` like any other field.

/// A self-describing value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    Int(i64),
    Bool(bool),
    Str(String),
    List(Vec<Value>),
    Map(Vec<(String, Value)>),
}

impl Value {
    /// A map from named fields, in the order given.
    pub fn map(fields: Vec<(&str, Value)>) -> Value {
        Value::Map(
            fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
    }

    /// A named field of a map.
    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Value::Map(fields) => fields.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    /// A named field of a map, decoded.
    pub fn field<T: Rec>(&self, key: &str) -> Option<T> {
        T::from_value(self.get(key)?)
    }
}

/// A type that can be written as a [`Value`] and read back.
pub trait Rec: Sized {
    fn to_value(&self) -> Value;
    fn from_value(v: &Value) -> Option<Self>;
}

impl Rec for bool {
    fn to_value(&self) -> Value {
        Value::Bool(*self)
    }
    fn from_value(v: &Value) -> Option<bool> {
        match v {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }
}

macro_rules! rec_int {
    ($($t:ty),*) => {$(
        impl Rec for $t {
            fn to_value(&self) -> Value {
                Value::Int(*self as i64)
            }
            fn from_value(v: &Value) -> Option<$t> {
                match v {
                    Value::Int(i) => <$t>::try_from(*i).ok(),
                    _ => None,
                }
            }
        }
    )*};
}

rec_int!(u8, u16, u32, i8, i16, i32, i64);

/// Seeds use the whole of a `u64`: stored bit for bit.
impl Rec for u64 {
    fn to_value(&self) -> Value {
        Value::Int(*self as i64)
    }
    fn from_value(v: &Value) -> Option<u64> {
        match v {
            Value::Int(i) => Some(*i as u64),
            _ => None,
        }
    }
}

impl Rec for String {
    fn to_value(&self) -> Value {
        Value::Str(self.clone())
    }
    fn from_value(v: &Value) -> Option<String> {
        match v {
            Value::Str(s) => Some(s.clone()),
            _ => None,
        }
    }
}

impl Rec for Value {
    fn to_value(&self) -> Value {
        self.clone()
    }
    fn from_value(v: &Value) -> Option<Value> {
        Some(v.clone())
    }
}

/// `None` is an empty list, `Some` a list of one.
impl<T: Rec> Rec for Option<T> {
    fn to_value(&self) -> Value {
        Value::List(self.iter().map(Rec::to_value).collect())
    }
    fn from_value(v: &Value) -> Option<Option<T>> {
        match v {
            Value::List(items) if items.is_empty() => Some(None),
            Value::List(items) if items.len() == 1 => T::from_value(&items[0]).map(Some),
            _ => None,
        }
    }
}

impl<T: Rec> Rec for Vec<T> {
    fn to_value(&self) -> Value {
        Value::List(self.iter().map(Rec::to_value).collect())
    }
    fn from_value(v: &Value) -> Option<Vec<T>> {
        match v {
            Value::List(items) => items.iter().map(T::from_value).collect(),
            _ => None,
        }
    }
}

impl<A: Rec, B: Rec> Rec for (A, B) {
    fn to_value(&self) -> Value {
        Value::List(vec![self.0.to_value(), self.1.to_value()])
    }
    fn from_value(v: &Value) -> Option<(A, B)> {
        match v {
            Value::List(items) if items.len() == 2 => {
                Some((A::from_value(&items[0])?, B::from_value(&items[1])?))
            }
            _ => None,
        }
    }
}

/// `Rec` for a fieldless enum, written as its variant's name:
/// `rec_enum!(Team: A, B);`
#[macro_export]
macro_rules! rec_enum {
    ($t:ident: $($v:ident),* $(,)?) => {
        impl $crate::Rec for $t {
            fn to_value(&self) -> $crate::Value {
                $crate::Value::Str(match self { $($t::$v => stringify!($v),)* }.to_string())
            }
            fn from_value(v: &$crate::Value) -> Option<$t> {
                match v {
                    $($crate::Value::Str(s) if s == stringify!($v) => Some($t::$v),)*
                    _ => None,
                }
            }
        }
    };
}

/// `Rec` for a one-field tuple struct, written as its field:
/// `rec_newtype!(EntityId(u32));`
#[macro_export]
macro_rules! rec_newtype {
    ($t:ident($inner:ty)) => {
        impl $crate::Rec for $t {
            fn to_value(&self) -> $crate::Value {
                $crate::Rec::to_value(&self.0)
            }
            fn from_value(v: &$crate::Value) -> Option<$t> {
                <$inner as $crate::Rec>::from_value(v).map($t)
            }
        }
    };
}

#[cfg(test)]
#[path = "record.test.rs"]
mod tests;
