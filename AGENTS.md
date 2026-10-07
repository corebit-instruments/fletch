# Agent Instructions

## Shared Instructions

Shared workflow policy comes from the `corebit` plugin, maintained in
[`corebit-instruments/agentic`](https://github.com/corebit-instruments/agentic).
Use `corebit:engineering-tasks` for Epic/ET work and
`corebit:verification-workflow` for validation. If the plugin is unavailable
and a sibling Agentic checkout exists, read
`../agentic/plugin/skills/corebit/engineering-tasks/SKILL.md` and
`../agentic/plugin/skills/corebit/verification-workflow/SKILL.md`.
Always run the installed `corebit` executable directly for local workflow
operations and authorized GitHub writes.

Repository configuration lives in [corebit.toml](corebit.toml). Installation
requirements and validation facts are in
[fletch-validation.md](docs/agents/fletch-validation.md); tracker ownership is in
[issue-tracker.md](docs/agents/issue-tracker.md). Local task briefs belong at
`.agent/task-brief.json`, which is ignored by Git.

Local instructions in this file override shared and workspace instructions.

## Repository Role

`fletch` is the Rust telemetry logging, Parquet storage, and sensor-fusion
library for HIL and test-engineering data.

- `FletchStreamBuilder` builds dynamic ingestion streams backed by Apache Arrow builders.
- `Stream<T>` and `#[derive(FletchSchema)]` provide a typed facade for static schemas.
- `FletchWorkspace` owns the local root folder and storage layout.
- Parquet file-level key/value metadata stores run and user-provided metadata.
- The optional `view` feature enables Polars-backed analytical views and time-aligned joins.

## Data Rules

- Keep `timestamp_ns` as the stable leading column in generated telemetry schemas.
- Keep stream names, storage paths, and file-level metadata aligned with the written Parquet files.
- Keep Fletch-owned `fletch.*` metadata authoritative over user-provided metadata.
- Preserve sparse-row semantics: same-timestamp field writes coalesce into one row, missing fields remain null, and duplicate field writes keep the latest value.
- Keep local workspace roots as filesystem paths at public boundaries.
- Keep the Polars view layer behind the `view` feature.

## Commands

Run commands from the repository root unless a task is scoped to a specific file or example.
`corebit local validate` runs the checks declared in `corebit.toml`.

- Format: `cargo fmt`
- Test: `cargo test`
- Test views: `cargo test --features view`
- Lint: `cargo clippy --all-targets --all-features`
- Run examples as needed, for example `cargo run --example accelerometer --features view`

## Branching and Pull Requests

- Use `corebit local branch`, `corebit local commit`, and `corebit remote pr`.
- `corebit.toml` sets `main` as the branch for new work and the default PR base,
  matching GitHub's default branch. Use `--closes` for completed ETs so GitHub
  links them to the PR and closes them when it merges into `main`.

## Knowledge Base

- Code conventions: `.steering/CODE_CONVENTIONS.md`
- Project architecture: `.steering/PROJECT_ARCHITECTURE.md`
