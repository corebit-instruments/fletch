# Fletch

High-throughput telemetry logging for test engineering and HIL platforms.

Fletch writes typed telemetry row streams to local Apache Parquet files using
Apache Arrow builders. A caller provides a root folder, a run id, column
definitions, and optional string key/value metadata. The metadata is written into the Parquet
file footer so the data remains self-contained.

> Status: experimental. APIs are still expected to change while Modulo hardware
> telemetry workflows settle.

## Storage Layout

Fletch stores files under the workspace root:

```text
root/
  runs/
    run_001/
      DaqRecords/
        <uuid>.parquet
```

A stream's Parquet files have only the columns it declares, in declaration
order. Run metadata such as `fletch.run_id`, `fletch.stream_name`,
`fletch.created_at_ns`, and user-provided metadata is stored as Parquet
file-level key/value metadata.

## Row Streams

Fletch writes long-format row streams, declared at runtime with
`FletchRowStreamBuilder` or from a struct with `#[derive(FletchSchema)]`
(below). A row stream has the typed columns you
declare and no implicit timestamp, and each `append` writes exactly one row.
Rows are written in append order, across batches, and are never merged,
overwritten or sorted. Records that do not share one clock, such as records
from several modules each counting its own ticks, sit side by side in one
stream, each row carrying its own source and ticks.

```rust
use fletch::{FletchRowStreamBuilder, FletchWorkspace, RowValue};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let workspace = FletchWorkspace::builder()
        .root("C:/fletch_warehouse")
        .build()?;

    let mut records = FletchRowStreamBuilder::new(&workspace, "DaqRecords")
        .column::<u8>("source")?
        .column::<String>("channel")?
        .column::<u64>("ticks")?
        .column::<i64>("raw")?
        .column::<f64>("value")?
        .column::<u64>("sequence")?
        .metadata("modulo.test_id", "daq-v1")
        .build("run_001")
        .await?;

    // One value per declared column, in order; `None` or `RowValue::Null`
    // writes a null.
    let value: Option<f64> = None;
    records.append(&[
        1_u8.into(),
        "ain0".into(),
        48_000_u64.into(),
        1024_i64.into(),
        value.into(),
        7_u64.into(),
    ])?;
    records.close()?;

    Ok(())
}
```

Every column is nullable. `append` checks the whole row (its length and each
value's type) before writing anything, so a rejected row leaves the stream
unchanged. Values are borrowed, strings included, so appending a row does not
allocate. Supported column types are `bool`, `u8`, `u16`, `u32`, `u64`, `i32`,
`i64`, `f32`, `f64` and `String`.

## Derived Streams

When the columns are known at compile time, derive `FletchSchema` on a struct
and write it through `Stream<T>`. Each field is one column, in field order,
typed by the field; an `Option<T>` field writes null for `None`. There is no
implicit timestamp: a time column, such as `ticks` below, is a field like any
other. Each `write` appends one struct value as one row, borrowing its fields.

A fieldless enum with `#[derive(FletchEnum)]` is a field type too, stored as a
string column holding the variant's name: its identifier, a
`#[fletch(rename_all = "...")]` rule on the enum, or a per-variant
`#[fletch(rename = "...")]`. `#[fletch(stream = "Name")]` puts the stream name
on the struct; without it, name the stream with `Stream::<T>::builder_named`.

```rust
use fletch::{FletchEnum, FletchSchema, FletchWorkspace, Stream};

#[derive(Clone, Copy, Debug, PartialEq, FletchEnum)]
#[fletch(rename_all = "SCREAMING_SNAKE_CASE")]
enum Channel {
    AnIn1, // stored as "AN_IN1"
    #[fletch(rename = "DIG_3")]
    DigIn3,
}

#[derive(Debug, FletchSchema)]
#[fletch(stream = "DaqRecords")]
struct DaqRecord {
    source: u8,
    channel: Channel,
    ticks: u64,
    raw: i64,
    value: Option<f64>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let workspace = FletchWorkspace::builder()
        .root("C:/fletch_warehouse")
        .build()?;

    let mut records = Stream::<DaqRecord>::builder(&workspace)
        .metadata("modulo.test_id", "daq-v1")
        .build("run_001")
        .await?;
    records.write(&DaqRecord {
        source: 1,
        channel: Channel::AnIn1,
        ticks: 48_000,
        raw: 1024,
        value: None,
    })?;
    records.close()?;

    Ok(())
}
```

Field types are the column types above, a `FletchEnum`, or an `Option` of one;
any other field type, and an enum variant that carries data, is a compile
error. A derived stream is a row stream underneath, with the same layout,
metadata and append-order rules. An enum is also a dynamic column type:
`.column::<Channel>("channel")` and `Channel::AnIn1.into()`.

## Views

The optional `view` feature reads one row stream back through Polars. A view
never joins or aligns streams: row streams share no clock. It filters and
orders by the stream's declared columns, and adds a `run_id` column from the
storage layout. Filters are Polars expressions, so the calling crate depends on
`polars` too.

```rust
use fletch::{FletchViewBuilder, FletchWorkspace};
use polars::prelude::{col, lit};

fn main() -> anyhow::Result<()> {
    let workspace = FletchWorkspace::builder()
        .root("C:/fletch_warehouse")
        .build()?;

    let df = FletchViewBuilder::new(&workspace, "DaqRecords")
        .run_id("run_001")
        .filter(col("source").eq(lit(1_u8)))
        .order_by(&["source", "ticks"])
        .columns(&["source", "ticks", "raw"])
        .build()?
        .collect()?;
    println!("{df}");

    Ok(())
}
```

Without `run_id` a view reads every run. Without `order_by` rows keep their
stored order: run by run, file by file, and append order within a file. Rows
that tie in `order_by` keep that stored order. A view can be collected into a
`DataFrame`, taken as a `LazyFrame`, or written to CSV or Parquet. Until
[fletch#11](https://github.com/corebit-instruments/fletch/issues/11) lands, a
view, typed or not, cannot read the `f32` or `f64` columns Parquet 60 writes.

## Typed Views

For a `FletchSchema` struct, `View<T>` reads a stream back as `T` values. The
derive generates one typed column handle per field, `DaqRecord::TICKS` being a
`Column<u64>`, whose `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `between`, `is_in`,
`is_null` and `is_not_null` accept only that column's type: comparing
`DaqRecord::SOURCE` with a string, or a misspelled column, does not compile.
Enum columns support `eq`, `ne` and `is_in` with enum values. Predicates
combine with `and`, `or` and `!`; `asc()` and `desc()` give ordering keys.

```rust
use fletch::{FletchWorkspace, View};

fn main() -> anyhow::Result<()> {
    let workspace = FletchWorkspace::builder()
        .root("C:/fletch_warehouse")
        .build()?;

    let records: Vec<DaqRecord> = View::<DaqRecord>::builder(&workspace)
        .run_id("run_001")
        .filter(DaqRecord::CHANNEL.eq(Channel::AnIn1))
        .filter(DaqRecord::TICKS.between(1_000, 50_000).and(!DaqRecord::VALUE.is_null()))
        .order_by([DaqRecord::SOURCE.asc(), DaqRecord::TICKS.asc()])
        .build()?
        .rows()?;
    println!("{records:?}");

    Ok(())
}
```

`build` checks once that the stream's columns and types match the struct's
fields exactly, and names the column that does not. `rows` reads nulls as
`None`; a null in a field that is not an `Option`, or a string that names no
enum variant, is an error naming the column. `filter_expr` takes a raw Polars
expression for what typed predicates cannot say, and `View::<T>::builder_named`
reads a type without `#[fletch(stream = "...")]`. A typed predicate also works
in a string-based view through `.into()`. String-based views stay for tools
that read streams they did not define.

## Querying with DuckDB

DuckDB can query the Parquet files directly:

```sql
SELECT *
FROM read_parquet('C:/fletch_warehouse/runs/*/DaqRecords/*.parquet');
```

File-level metadata is preserved in the Parquet footer. Use columns for
telemetry values that need normal SQL filtering, and metadata for contextual
run/test/device values that should travel with the file without requiring a
sidecar database.
