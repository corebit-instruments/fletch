# Fletch Project Architecture

## Crate

- Root `Cargo.toml` defines the `fletch` Rust crate.
- The crate uses Rust 2024 and exposes a library API for telemetry ingestion, local Parquet storage, and optional analytical views.
- The default feature set keeps Polars out of the dependency graph.
- The `view` feature enables Polars-backed exploration through `FletchViewBuilder` and `FletchView`.

## Key Areas

- `src/lib.rs`: public exports and feature-gated view module.
- `src/workspace.rs`: `FletchWorkspace`, workspace builders, and local root path handling.
- `src/config.rs`: storage config, output Parquet path generation, and file-level metadata assembly.
- `src/rows.rs`: `FletchRowStreamBuilder` and long-format `FletchRowStream`, the only write API.
- `src/types.rs`: `FletchType` trait, borrowed `RowValue` cells, and the supported Rust-to-Arrow column builders.
- `src/sink.rs`: background Arrow-to-Parquet writer and Parquet file metadata writing.
- `src/view.rs`: optional Polars LazyFrame scans of one row stream, run filtering, column filters, ordering and selection, and CSV/Parquet exports.
- `tests/`: integration tests for row-stream append order, nulls, column types, rejected rows, empty streams, file metadata, and feature-gated views.
- `examples/`: a runnable DAQ row-stream example with optional view construction.

## Data Model

- A workspace is a local root folder.
- A run is identified by a caller-provided `run_id`.
- A stream maps to one stream name and a list of declared columns.
- Row-stream Arrow schemas hold only their declared nullable columns; there is no implicit timestamp.
- Parquet files are written under `runs/{run_id}/{stream_name}/{uuid}.parquet`.
- Run and user metadata is stored as Parquet file-level key/value metadata.

## Ingestion Model

- Callers create row streams with `FletchRowStreamBuilder`, append one row per call with one `RowValue` per declared column, and call `close`.
- Row streams flush full batches in append order without merging or sorting, and reject a mismatched row before writing any of it.
- `BackgroundSink` writes batches on a worker thread and writes file-level metadata into the Parquet footer.
- Empty streams close without creating Parquet files.

## View Model

- `FletchViewBuilder` is available only with the `view` feature.
- A view reads one row stream's Parquet files from the local root, for one run or every run.
- It materializes `run_id` from the storage layout as its first column, followed by the requested columns or every declared column.
- It applies optional filters (Polars expressions over declared columns), then an optional stable ascending order by declared columns, for DAQ source then ticks. Without an order, rows keep stored order: run, file, then append order.
- Views never join or align streams on a shared timestamp.
- Views can be collected into a DataFrame or exported to CSV/Parquet.

## Modulo Workspace Role

- Fletch owns telemetry-library behavior and local analytical data storage.
- It should remain decoupled from `modulo-sdk` protocol definitions and `modulo-firmware` hardware execution unless a task explicitly connects hardware telemetry to Fletch ingestion.
- Bitshift or other UI/data consumers should treat Fletch outputs as plain Parquet/data-view artifacts rather than duplicating Fletch storage rules.
