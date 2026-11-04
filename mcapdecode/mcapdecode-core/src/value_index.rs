//! Indexing helpers for decoded values.
use crate::{Value, ValueAccessError};

/// An index accepted by [`Value::get`].
pub trait ValueIndex {
    fn get(self, value: &Value) -> Result<&Value, ValueAccessError>;
}
impl ValueIndex for usize {
    fn get(self, value: &Value) -> Result<&Value, ValueAccessError> {
        value
            .as_sequence()?
            .get(self)
            .ok_or(ValueAccessError::NotFound)
    }
}

/// Key types supported by [`Value::get_key`].
pub trait MapKey {
    fn matches(&self, value: &Value) -> bool;
}
impl MapKey for &str {
    fn matches(&self, value: &Value) -> bool {
        matches!(value, Value::String(s) if s.as_ref() == *self)
    }
}
impl MapKey for bool {
    fn matches(&self, value: &Value) -> bool {
        matches!(value, Value::Bool(v) if v == self)
    }
}
macro_rules! map_keys {
    ($($ty:ty),* $(,)?) => {$ (impl MapKey for $ty {
        fn matches(&self, value: &Value) -> bool {
            match value {
                Value::I8(v) => <$ty>::try_from(*v).ok() == Some(*self),
                Value::I16(v) => <$ty>::try_from(*v).ok() == Some(*self),
                Value::I32(v) => <$ty>::try_from(*v).ok() == Some(*self),
                Value::I64(v) => <$ty>::try_from(*v).ok() == Some(*self),
                Value::U8(v) => <$ty>::try_from(*v).ok() == Some(*self),
                Value::U16(v) => <$ty>::try_from(*v).ok() == Some(*self),
                Value::U32(v) => <$ty>::try_from(*v).ok() == Some(*self),
                Value::U64(v) => <$ty>::try_from(*v).ok() == Some(*self),
                _ => false,
            }
        }
    })*};
}
map_keys!(i8, i16, i32, i64, u8, u16, u32, u64);
