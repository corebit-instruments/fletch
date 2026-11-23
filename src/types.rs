use arrow::array::{
    ArrayRef, BooleanBuilder, Float32Builder, Float64Builder, Int32Builder, Int64Builder,
    StringBuilder, UInt8Builder, UInt16Builder, UInt32Builder, UInt64Builder,
};
use arrow::datatypes::DataType;
use std::sync::Arc;

/// A Rust type a row-stream column can hold, declared with
/// [`FletchRowStreamBuilder::column`](crate::FletchRowStreamBuilder::column).
pub trait FletchType: Sized + Send + Sync + 'static {
    fn data_type() -> DataType;
    fn new_builder(capacity: usize) -> ColumnBuilder;
}

macro_rules! fletch_type {
    ($ty:ty, $variant:ident, $builder:ident, $data_type:expr) => {
        impl FletchType for $ty {
            fn data_type() -> DataType {
                $data_type
            }

            fn new_builder(capacity: usize) -> ColumnBuilder {
                ColumnBuilder::$variant($builder::with_capacity(capacity))
            }
        }
    };
}

fletch_type!(bool, Bool, BooleanBuilder, DataType::Boolean);
fletch_type!(u8, U8, UInt8Builder, DataType::UInt8);
fletch_type!(u16, U16, UInt16Builder, DataType::UInt16);
fletch_type!(u32, U32, UInt32Builder, DataType::UInt32);
fletch_type!(u64, U64, UInt64Builder, DataType::UInt64);
fletch_type!(i32, I32, Int32Builder, DataType::Int32);
fletch_type!(i64, I64, Int64Builder, DataType::Int64);
fletch_type!(f32, F32, Float32Builder, DataType::Float32);
fletch_type!(f64, F64, Float64Builder, DataType::Float64);

impl FletchType for String {
    fn data_type() -> DataType {
        DataType::Utf8
    }

    fn new_builder(_capacity: usize) -> ColumnBuilder {
        ColumnBuilder::String(StringBuilder::new())
    }
}

/// The Arrow builder behind one row-stream column.
pub enum ColumnBuilder {
    Bool(BooleanBuilder),
    U8(UInt8Builder),
    U16(UInt16Builder),
    U32(UInt32Builder),
    U64(UInt64Builder),
    I32(Int32Builder),
    I64(Int64Builder),
    F32(Float32Builder),
    F64(Float64Builder),
    String(StringBuilder),
}

impl ColumnBuilder {
    fn append_null(&mut self) {
        match self {
            Self::Bool(builder) => builder.append_null(),
            Self::U8(builder) => builder.append_null(),
            Self::U16(builder) => builder.append_null(),
            Self::U32(builder) => builder.append_null(),
            Self::U64(builder) => builder.append_null(),
            Self::I32(builder) => builder.append_null(),
            Self::I64(builder) => builder.append_null(),
            Self::F32(builder) => builder.append_null(),
            Self::F64(builder) => builder.append_null(),
            Self::String(builder) => builder.append_null(),
        }
    }

    pub(crate) fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Bool(builder) => Arc::new(builder.finish()),
            Self::U8(builder) => Arc::new(builder.finish()),
            Self::U16(builder) => Arc::new(builder.finish()),
            Self::U32(builder) => Arc::new(builder.finish()),
            Self::U64(builder) => Arc::new(builder.finish()),
            Self::I32(builder) => Arc::new(builder.finish()),
            Self::I64(builder) => Arc::new(builder.finish()),
            Self::F32(builder) => Arc::new(builder.finish()),
            Self::F64(builder) => Arc::new(builder.finish()),
            Self::String(builder) => Arc::new(builder.finish()),
        }
    }
}

/// One cell of a row-stream row: a typed value or null.
///
/// Strings are borrowed, so building a row allocates nothing; the value is
/// copied into the column's Arrow builder when the row is appended.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RowValue<'a> {
    Null,
    Bool(bool),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Str(&'a str),
}

macro_rules! row_value_from {
    ($ty:ty, $variant:ident) => {
        impl From<$ty> for RowValue<'_> {
            fn from(value: $ty) -> Self {
                Self::$variant(value)
            }
        }
    };
}

row_value_from!(bool, Bool);
row_value_from!(u8, U8);
row_value_from!(u16, U16);
row_value_from!(u32, U32);
row_value_from!(u64, U64);
row_value_from!(i32, I32);
row_value_from!(i64, I64);
row_value_from!(f32, F32);
row_value_from!(f64, F64);

impl<'a> From<&'a str> for RowValue<'a> {
    fn from(value: &'a str) -> Self {
        Self::Str(value)
    }
}

impl<'a> From<&'a String> for RowValue<'a> {
    fn from(value: &'a String) -> Self {
        Self::Str(value)
    }
}

impl<'a, T: Into<RowValue<'a>>> From<Option<T>> for RowValue<'a> {
    fn from(value: Option<T>) -> Self {
        value.map_or(Self::Null, Into::into)
    }
}

impl ColumnBuilder {
    /// Whether `value` can be appended to this column: null always can,
    /// otherwise the value's type must be the column's type.
    pub(crate) fn accepts(&self, value: &RowValue<'_>) -> bool {
        matches!(
            (self, value),
            (_, RowValue::Null)
                | (Self::Bool(_), RowValue::Bool(_))
                | (Self::U8(_), RowValue::U8(_))
                | (Self::U16(_), RowValue::U16(_))
                | (Self::U32(_), RowValue::U32(_))
                | (Self::U64(_), RowValue::U64(_))
                | (Self::I32(_), RowValue::I32(_))
                | (Self::I64(_), RowValue::I64(_))
                | (Self::F32(_), RowValue::F32(_))
                | (Self::F64(_), RowValue::F64(_))
                | (Self::String(_), RowValue::Str(_))
        )
    }

    /// Appends a value that [`ColumnBuilder::accepts`] has already checked.
    pub(crate) fn append_row_value(&mut self, value: RowValue<'_>) {
        match (self, value) {
            (Self::Bool(builder), RowValue::Bool(value)) => builder.append_value(value),
            (Self::U8(builder), RowValue::U8(value)) => builder.append_value(value),
            (Self::U16(builder), RowValue::U16(value)) => builder.append_value(value),
            (Self::U32(builder), RowValue::U32(value)) => builder.append_value(value),
            (Self::U64(builder), RowValue::U64(value)) => builder.append_value(value),
            (Self::I32(builder), RowValue::I32(value)) => builder.append_value(value),
            (Self::I64(builder), RowValue::I64(value)) => builder.append_value(value),
            (Self::F32(builder), RowValue::F32(value)) => builder.append_value(value),
            (Self::F64(builder), RowValue::F64(value)) => builder.append_value(value),
            (Self::String(builder), RowValue::Str(value)) => builder.append_value(value),
            (builder, RowValue::Null) => builder.append_null(),
            _ => unreachable!("row value type is checked before it is appended"),
        }
    }
}
