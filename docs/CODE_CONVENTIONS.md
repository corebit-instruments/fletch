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
- Preserve stream names from builder names unless a deliberate migration path is added. Derived streams take their name from `Stream::<T>::builder`, not from the struct.
- Keep local workspace roots represented as filesystem paths at public boundaries.
- Preserve the default layout semantics: run, stream, and unique Parquet file names.
- Sanitize path components before rendering storage layout values.
- Store user metadata as Parquet file-level string key/value pairs, but keep `fletch.*` metadata owned by Fletch.
- Avoid adding sidecar database/catalog dependencies unless the storage contract is intentionally expanded.
- For schema or storage layout changes, include a migration path that preserves existing local workspaces where practical.

## Ingestion And View Practices

- Keep row-stream columns typed through `FletchType` and row cells through `RowValue` instead of ad hoc values.
- Keep `Stream<T>` a typed facade over `FletchRowStream`, not a second write path: a derived struct declares one column per field in field order and writes one row per value, borrowing its fields. Supported field types are the `FletchType`s and `Option` of one (`FletchField`); anything else must fail to compile.
- Keep row streams in append order: one row per append, no merging, overwriting or sorting, and no partial row written when an append is rejected.
- Ensure empty streams can close cleanly without writing Parquet files.
- Keep the Polars-backed view layer behind the `view` feature.
- Preserve run filtering by `run_id` so views do not accidentally cross runs.
- Materialize `run_id` in views from the storage layout.
- A view reads one row stream. Never join or align streams on a shared timestamp; filter and order by declared columns (for DAQ: source, then ticks), with ties keeping stored order.

## Verification

- Run `cargo fmt`.
- Run `cargo test`.
- Run `cargo check --features view` when changing view behavior or public examples that use `FletchViewBuilder`.
- Run `cargo clippy --all-targets --all-features` when practical.
- Use temporary directories in tests for filesystem behavior.
