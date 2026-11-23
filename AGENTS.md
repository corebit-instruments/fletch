# Agent Instructions

## Shared Instructions

Shared workflow policy comes from the `agentic` plugin, maintained in
[`corebit-instruments/agentic`](https://github.com/corebit-instruments/agentic).
Use `agentic:engineering-tasks` for Epic/ET work and
`agentic:verification-workflow` for validation. If the plugin is unavailable
and a sibling Agentic checkout exists, read
`plugins/agentic/skills/workflows/engineering-tasks/SKILL.md` and
`plugins/agentic/skills/workflows/verification-workflow/SKILL.md` within that checkout.
Use the companion `corebit` plugin for Corebit company and Modulo context.
Always run the installed `agentic` executable directly for local workflow
operations and authorized GitHub writes.

Repository configuration lives in [agentic.toml](agentic.toml). Installation
requirements and validation facts are in
[fletch-validation.md](docs/agents/fletch-validation.md); tracker ownership is in
[issue-tracker.md](docs/agents/issue-tracker.md). Local task briefs belong at
`.agent/task-brief.json`, which is ignored by Git.

Local instructions in this file override shared and workspace instructions.

## Repository Role

`fletch` is the Rust telemetry logging and Parquet storage library for HIL and
test-engineering data, with optional Polars-backed views.

- Fletch writes long-format row streams only, backed by Apache Arrow builders: declared columns, one row per append, no timestamp key.
- `FletchRowStreamBuilder` declares a row stream's columns at runtime; `FletchRowStream::append` writes one row of `RowValue`s.
- `#[derive(FletchSchema)]` (from `fletch-derive`) declares one column per struct field, generates a typed `Column<T>` handle per field, and `Stream<T>::write` appends one struct value as one row on top of `FletchRowStream`. `#[fletch(stream = "Name")]` puts the stream name on the type.
- `#[derive(FletchEnum)]` makes a fieldless enum a column type, stored as a string column of variant names (`rename`, `rename_all`).
- `FletchWorkspace` owns the local root folder and storage layout.
- Parquet file-level key/value metadata stores run and user-provided metadata.
- The optional `view` feature enables Polars-backed views of one row stream, filtered and ordered by its declared columns: string-based `FletchViewBuilder` for any stream, and typed `View<T>` with typed predicates that reads rows back as `T`.

## Data Rules

- Keep stream names, storage paths, and file-level metadata aligned with the written Parquet files.
- Keep Fletch-owned `fletch.*` metadata authoritative over user-provided metadata.
- Row streams (`FletchRowStream`) have only their declared columns: each append is one row, kept in append order, never merged, overwritten or sorted.
- Views never join or align streams on a shared timestamp; they filter and order by declared columns.
- Enums are stored as strings holding the variant's name, never as integers.
- A typed view checks the stream's schema against `T` once at build; typed reads fail on a null in a non-`Option` field or an unknown enum name, naming the column.
- Keep local workspace roots as filesystem paths at public boundaries.
- Keep the Polars view layer behind the `view` feature.

## Commands

Run commands from the repository root unless a task is scoped to a specific file or example.
`agentic local validate` runs the checks declared in `agentic.toml`.

- Format: `cargo fmt`
- Test: `cargo test`
- Test views: `cargo test --features view`
- Lint: `cargo clippy --all-targets --all-features`
- Run examples as needed, for example `cargo run --example daq_records --features view`

## Branching and Pull Requests

- Use `agentic local branch`, `agentic local commit`, and `agentic remote pr`.
- `agentic.toml` sets `main` as the branch for new work and the default PR base,
  matching GitHub's default branch. Use `--closes` for completed ETs so GitHub
  links them to the PR and closes them when it merges into `main`.

## Docs

When you need to understand an area, or doubt how or why it works, look
for its doc here first. Update a doc your change contradicts in the same
change.

- [`docs/CODE_CONVENTIONS.md`](docs/CODE_CONVENTIONS.md): Rust style, data and
  storage practices, ingestion and view rules, and verification expectations.
- [`docs/PROJECT_ARCHITECTURE.md`](docs/PROJECT_ARCHITECTURE.md): the crate's
  modules, data model, row-stream ingestion (dynamic and derived, enums
  included), the view model (string-based and typed) and Fletch's role in the
  Modulo workspace.
- [`docs/agents/fletch-validation.md`](docs/agents/fletch-validation.md): agent
  tooling setup, the declared validation checks and their toolchain
  prerequisites.
- [`docs/agents/issue-tracker.md`](docs/agents/issue-tracker.md): where
  Engineering Tasks live and which Project tracks them.
