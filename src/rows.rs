use anyhow::{Result, anyhow};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::config::FletchConfig;
use crate::sink::BackgroundSink;
use crate::stream::{ChannelSpec, DEFAULT_BATCH_CAPACITY};
use crate::types::{ChannelBuilder, FletchType, RowValue};
use crate::workspace::FletchWorkspace;

/// Builds a long-format row stream: declared typed columns, one row per
/// [`FletchRowStream::append`], with no implicit `timestamp_ns` key.
pub struct FletchRowStreamBuilder {
    workspace: FletchWorkspace,
    stream_name: String,
    columns: Vec<ChannelSpec>,
    metadata: BTreeMap<String, String>,
    batch_capacity: usize,
}

impl FletchRowStreamBuilder {
    pub fn new(workspace: &FletchWorkspace, stream_name: impl Into<String>) -> Self {
        Self {
            workspace: workspace.clone(),
            stream_name: stream_name.into(),
            columns: Vec::new(),
            metadata: BTreeMap::new(),
            batch_capacity: DEFAULT_BATCH_CAPACITY,
        }
    }

    /// Declares the next column. Columns are written in declaration order and
    /// every column is nullable.
    pub fn column<T: FletchType>(mut self, name: impl Into<String>) -> Result<Self> {
        let name = name.into();
        if self.columns.iter().any(|column| column.name == name) {
            return Err(anyhow!("duplicate column `{}`", name));
        }
        self.columns.push(ChannelSpec {
            name,
            data_type: T::data_type(),
            builder_factory: T::new_builder,
        });
        Ok(self)
    }

    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    pub fn metadata_pairs<I, K, V>(mut self, metadata: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        for (key, value) in metadata {
            self.metadata.insert(key.into(), value.into());
        }
        self
    }

    pub fn batch_capacity(mut self, capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(anyhow!("batch capacity must be greater than zero"));
        }
        self.batch_capacity = capacity;
        Ok(self)
    }

    pub async fn build(self, run_id: &str) -> Result<FletchRowStream> {
        if self.columns.is_empty() {
            return Err(anyhow!("at least one column is required"));
        }

        let schema = Arc::new(Schema::new(
            self.columns
                .iter()
                .map(|column| Field::new(column.name.clone(), column.data_type.clone(), true))
                .collect::<Vec<_>>(),
        ));
        let config = FletchConfig::init(&self.workspace, &self.stream_name, run_id, self.metadata)?;
        let sink = BackgroundSink::spawn(config, schema.clone())?;
        let columns = self
            .columns
            .into_iter()
            .map(|spec| (spec.builder_factory)(self.batch_capacity))
            .collect();

        Ok(FletchRowStream {
            sink,
            schema,
            columns,
            rows: 0,
            batch_capacity: self.batch_capacity,
        })
    }
}

/// A long-format stream. Each [`append`](Self::append) writes exactly one row;
/// rows are written in append order and are never merged, overwritten or
/// sorted.
pub struct FletchRowStream {
    sink: BackgroundSink,
    schema: Arc<Schema>,
    columns: Vec<ChannelBuilder>,
    rows: usize,
    batch_capacity: usize,
}

impl FletchRowStream {
    /// Appends one row, one value per declared column in declaration order.
    /// Use [`RowValue::Null`] or `None` for a missing value.
    ///
    /// The whole row is checked before anything is written, so a rejected row
    /// leaves the stream unchanged.
    pub fn append(&mut self, row: &[RowValue<'_>]) -> Result<()> {
        if row.len() != self.columns.len() {
            return Err(anyhow!(
                "row has {} values, the stream has {} columns",
                row.len(),
                self.columns.len()
            ));
        }
        for (index, (column, value)) in self.columns.iter().zip(row).enumerate() {
            if !column.accepts(value) {
                let field = self.schema.field(index);
                return Err(anyhow!(
                    "column `{}` expects {:?}, got {:?}",
                    field.name(),
                    field.data_type(),
                    value
                ));
            }
        }

        for (column, value) in self.columns.iter_mut().zip(row) {
            column.append_row_value(*value);
        }
        self.rows += 1;

        if self.rows >= self.batch_capacity {
            self.flush_batch()?;
        }
        Ok(())
    }

    /// The declared column names, in order.
    pub fn columns(&self) -> impl Iterator<Item = &str> {
        self.schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
    }

    fn flush_batch(&mut self) -> Result<()> {
        if self.rows == 0 {
            return Ok(());
        }
        let columns = self
            .columns
            .iter_mut()
            .map(ChannelBuilder::finish)
            .collect();
        let batch = RecordBatch::try_new(self.schema.clone(), columns)?;
        self.rows = 0;
        self.sink.write_batch(batch)
    }

    pub fn close(mut self) -> Result<()> {
        self.flush_batch()?;
        self.sink.close()
    }
}
