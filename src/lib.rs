mod column;
mod column_order;
mod config;
mod enums;
mod rows;
mod schema;
mod sink;
#[cfg(feature = "view")]
mod typed_view;
mod types;
#[cfg(feature = "view")]
mod view;
mod workspace;

/// The result type of fletch's API and of derived code.
pub type Result<T> = anyhow::Result<T>;

pub use arrow::datatypes::DataType;
pub use column::{Column, ColumnValue, OrderedColumn};
pub use config::*;
pub use enums::FletchEnum;
pub use fletch_derive::{FletchEnum, FletchSchema};
pub use rows::*;
pub use schema::{FletchSchema, NamedSchema, Stream, StreamBuilder};
pub use sink::*;
#[cfg(feature = "view")]
pub use typed_view::*;
pub use types::{FletchField, FletchType, RowValue};
#[cfg(feature = "view")]
pub use view::*;
pub use workspace::*;

#[doc(hidden)]
pub use column::ColumnLiteral;
#[doc(hidden)]
pub use enums::enum_from_row_value;
#[doc(hidden)]
pub use schema::check_row_len;
#[doc(hidden)]
pub use types::ColumnBuilder;
