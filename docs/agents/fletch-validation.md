# Fletch validation

## Toolkit and discovery

Shared workflow policy comes from the
[Corebit plugin](https://github.com/corebit-instruments/agentic/tree/main/plugin/skills).
Follow Agentic's [installation guide](https://github.com/corebit-instruments/agentic#install)
and [CLI guidance](https://github.com/corebit-instruments/agentic/blob/main/docs/design/cli.md).
The committed Codex, Claude Code, and OpenCode settings enable the plugin;
machine installation and Codex project trust remain prerequisites. Start the
harness at the repository root; `CLAUDE.md` points to `AGENTS.md`.

Fletch adoption uses Agentic commit
`cfa8ce0ee79d82b00039e36e970025b5d74e9ac9`, the same source pin as firmware
#118. Its package version is still `0.1.0`; the revision identifies the tested
implementation. The original `v0.1.0` release lacks root `corebit.toml` support.
Until a release includes it, use a checkout at the exact pin and run from the
Fletch root:

```text
uv run --isolated --project <agentic-checkout>/tools --python 3.13 corebit show preflight --repo . --project https://github.com/orgs/corebit-instruments/projects/1 --owner corebit-instruments/fletch --pr-base main --json
uv run --isolated --project <agentic-checkout>/tools --python 3.13 corebit local validate --json
```

`corebit.toml` owns repository facts. New work branches and PRs use `main`,
matching GitHub's live default branch. Use `remote pr --closes 6` for this ET's
PR so GitHub links the issue and closes it when the PR merges into `main`.
Branch policy is independent of toolkit installation.

Validation results and limitations belong on the
[Fletch ET](https://github.com/corebit-instruments/fletch/issues/6) and its PR.
Revalidate affected checks when the toolkit pin changes. Local briefs and logs
live under ignored `.agent/`.
The canonical CLI guidance owns the handoff format and resume procedure.

## Rust checks

The configured checks run without a shell, in this order, stopping on failure:

```text
cargo fmt --all -- --check
cargo test --locked
cargo check --features view --locked
cargo test --features view --locked
cargo clippy --all-targets --all-features --locked
```

These are the repository's existing format, default-test, optional-view, and
lint commands. `--locked` preserves dependency resolution; formatting checks
do not rewrite files. The default feature set keeps Polars disabled; the view
checks exercise the optional Polars layer. Clippy includes the view-dependent
accelerometer example. Tests use temporary filesystem workspaces.

Prerequisites are a Rust toolchain supporting edition 2024, rustfmt, Clippy,
the native host linker/SDK, and access to the crates resolved by `Cargo.lock`.
The Rust/Cargo `1.97.1` attempt on Windows/MSVC fails in the locked view
dependency `ethnum 1.5.2` with E0512 (`TryFromIntError` changed size). An
installed Rust `1.92` toolchain provides the alternate validation route:

```text
rustup run 1.92 uv run --isolated --project <agentic-checkout>/tools --python 3.13 corebit local validate --json
```

No repository toolchain override, dependency update or registry patch is part
of this adoption.
Fletch declares no embedded target or device runner. These commands exercise
a local library and do not require a connected Modulo board. They preserve
telemetry schema, sparse-row behavior, storage paths, metadata ownership and
feature boundaries.

Fresh live harness invocation/resume, clean installation/update on every
supported OS, and closure-sensitive GitHub scenarios remain part of Agentic's
human-owned [final E2E #38](https://github.com/corebit-instruments/agentic/issues/38).
