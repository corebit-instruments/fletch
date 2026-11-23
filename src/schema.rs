use anyhow::{Result, anyhow};
use arrow::datatypes::DataType;
use std::marker::PhantomData;

use crate::rows::{FletchRowStream, FletchRowStreamBuilder};
use crate::types::RowValue;
use crate::workspace::FletchWorkspace;

/// A Rust struct whose values are rows of a row stream, one column per field
/// in field order. Derive it with `#[derive(FletchSchema)]` rather than
/// implementing it by hand.
pub trait FletchSchema: Sized {
    /// Declares one column per field, in field order.
    fn declare_columns(builder: FletchRowStreamBuilder) -> Result<FletchRowStreamBuilder>;

    /// Appends this value to `stream` as one row.
    fn append_to(&self, stream: &mut FletchRowStream) -> Result<()>;

    /// Each field's column name and Arrow type, in field order.
    fn column_types() -> Vec<(&'static str, DataType)>;

    /// Builds a value from one row, one cell per field in field order. A
    /// null fills an `Option` field with `None` and is an error elsewhere.
    fn from_row(row: &[RowValue<'_>]) -> Result<Self>;
}

/// A [`FletchSchema`] whose stream name lives on the type, set with
/// `#[fletch(stream = "Name")]`.
#[diagnostic::on_unimplemented(
    message = "`{Self}` has no stream name",
    label = "no `#[fletch(stream = \"...\")]` on this type",
    note = "add `#[fletch(stream = \"Name\")]` to the struct, or pass the name with `builder_named`"
)]
pub trait NamedSchema: FletchSchema {
    const STREAM_NAME: &'static str;
}

/// Checks that a row passed to [`FletchSchema::from_row`] has one cell per
/// field. Used by derived code.
#[doc(hidden)]
pub fn check_row_len(row: &[RowValue<'_>], fields: usize) -> Result<()> {
    if row.len() != fields {
        return Err(anyhow!(
            "row has {} values, the schema has {} fields",
            row.len(),
            fields
        ));
    }
    Ok(())
}

/// A row stream typed by a [`FletchSchema`] struct: each
/// [`write`](Self::write) appends one struct value as one row.
pub struct Stream<T> {
    inner: FletchRowStream,
    _marker: PhantomData<fn(&T)>,
}

impl<T: NamedSchema> Stream<T> {
    /// Builds a stream named by the type's `#[fletch(stream = "Name")]`.
    pub fn builder(workspace: &FletchWorkspace) -> StreamBuilder<T> {
        Self::builder_named(workspace, T::STREAM_NAME)
    }
}

impl<T: FletchSchema> Stream<T> {
    /// Builds a stream with an explicit name, for types without
    /// `#[fletch(stream = "Name")]` or to write one type to several streams.
    pub fn builder_named(
        workspace: &FletchWorkspace,
        stream_name: impl Into<String>,
    ) -> StreamBuilder<T> {
        StreamBuilder {
            inner: FletchRowStreamBuilder::new(workspace, stream_name),
            _marker: PhantomData,
        }
    }

    /// Appends `value` as one row. The row is built from borrowed fields, so
    /// writing does not allocate.
    pub fn write(&mut self, value: &T) -> Result<()> {
        value.append_to(&mut self.inner)
    }

    /// The declared column names, in field order.
    pub fn columns(&self) -> impl Iterator<Item = &str> {
        self.inner.columns()
    }

    pub fn close(self) -> Result<()> {
        self.inner.close()
    }

    pub fn into_inner(self) -> FletchRowStream {
        self.inner
    }
}

/// Builds a [`Stream<T>`]; the columns come from `T`'s fields.
pub struct StreamBuilder<T> {
    inner: FletchRowStreamBuilder,
    _marker: PhantomData<fn(&T)>,
}

impl<T: FletchSchema> StreamBuilder<T> {
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.inner = self.inner.metadata(key, value);
        self
    }

    pub fn metadata_pairs<I, K, V>(mut self, metadata: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.inner = self.inner.metadata_pairs(metadata);
        self
    }

    pub fn batch_capacity(mut self, capacity: usize) -> Result<Self> {
        self.inner = self.inner.batch_capacity(capacity)?;
        Ok(self)
    }

    pub async fn build(self, run_id: &str) -> Result<Stream<T>> {
        let inner = T::declare_columns(self.inner)?.build(run_id).await?;
        Ok(Stream {
            inner,
            _marker: PhantomData,
        })
    }
}
