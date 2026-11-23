mod config;
mod rows;
mod schema;
mod sink;
mod types;
#[cfg(feature = "view")]
mod view;
mod workspace;

/// The result type of fletch's API and of `#[derive(FletchSchema)]` code.
pub type Result<T> = anyhow::Result<T>;

pub use config::*;
pub use fletch_derive::FletchSchema;
pub use rows::*;
pub use schema::*;
pub use sink::*;
pub use types::{FletchField, FletchType, RowValue};
#[cfg(feature = "view")]
pub use view::*;
pub use workspace::*;
