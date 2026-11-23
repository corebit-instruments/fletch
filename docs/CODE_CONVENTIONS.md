# Fletch Code Conventions

## Rust Style

- Use `cargo fmt` formatting.
- Keep the public API builder-oriented and ergonomic for test-engineering workflows.
- Prefer explicit domain names for workspaces, runs, streams, storage layout, timestamps, file metadata, and analytical views.
- Use `anyhow::Result` consistently for the current library surface unless a typed error boundary is introduced across the crate.
- Keep module boundaries narrow: workspace setup, storage config, dynamic stream ingestion, background sink, file metadata, and optional views should stay separately owned.
- Add concise comments only for non-obvious Arrow, Parquet, or Polars behavior.

## Data And Storage Practices

- A row stream's schema is its declared columns, in declaration order, all nullable. Fletch adds no implicit columns.
- Preserve stream names from builder names unless a deliberate migration path is added. Derived streams take their name from `#[fletch(stream = "Name")]` or `builder_named`, never from the struct identifier.
- Keep local workspace roots represented as filesystem paths at public boundaries.
- Preserve the default layout semantics: run, stream, and unique Parquet file names.
- Sanitize path components before rendering storage layout values.
- Store user metadata as Parquet file-level string key/value pairs, but keep `fletch.*` metadata owned by Fletch.
- Avoid adding sidecar database/catalog dependencies unless the storage contract is intentionally expanded.
- For schema or storage layout changes, include a migration path that preserves existing local workspaces where practical.

## Ingestion And View Practices

- Keep row-stream columns typed through `FletchType` and row cells through `RowValue` instead of ad hoc values.
- Keep `Stream<T>` a typed facade over `FletchRowStream`, not a second write path: a derived struct declares one column per field in field order and writes one row per value, borrowing its fields. Supported field types are the `FletchType`s, `FletchEnum`s and `Option` of one (`FletchField`); anything else must fail to compile.
- Store enums as strings holding the variant's name, so files stay readable without the Rust type and reordering variants cannot change meaning. Data-carrying variants are rejected at compile time.
- Keep typed view predicates type-checked: a `Column<T>` compares only with `T` (or `&str` for `String`), ordering comparisons apply only to `OrderedColumn` types, and enums compare only for equality.
- Keep row streams in append order: one row per append, no merging, overwriting or sorting, and no partial row written when an append is rejected.
- Ensure empty streams can close cleanly without writing Parquet files.
- Keep the Polars-backed view layer behind the `view` feature.
- Preserve run filtering by `run_id` so views do not accidentally cross runs.
- Materialize `run_id` in views from the storage layout.
- A view reads one row stream. Keep string-based views alongside typed ones, for tools reading streams they did not define. A typed view checks the stream's columns and types against `T` exactly at build, and a typed read never turns a null into a default value. Never join or align streams on a shared timestamp; filter and order by declared columns (for DAQ: source, then ticks), with ties keeping stored order.

## Verification

- Run `cargo fmt`.
- Run `cargo test`.
- Run `cargo check --features view` when changing view behavior or public examples that use `FletchViewBuilder`.
- Run `cargo clippy --all-targets --all-features` when practical.
- Use temporary directories in tests for filesystem behavior.
