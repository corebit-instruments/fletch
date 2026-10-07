# Fletch #6 adoption evidence

Observed 2026-10-06 on Windows/PowerShell in an isolated Fletch worktree,
on branch `chore/6-agentic-workflows`. This records repository adoption;
full rollout acceptance remains in
[Agentic #38](https://github.com/corebit-instruments/agentic/issues/38).

## Revisions and references

- Fletch implementation baseline: `4a6a483915c4d3c532bac1b47d29790271cc4f6a`
  (current `main`). Intended PR base: `staging`, whose observed revision is
  `e6f82534afa5b4b9ed0776aef93c6eea3a03a4a4`. The difference is one obsolete
  workflow-pointer removal; no library behavior differs between those bases.
- Agentic source pin: `cfa8ce0ee79d82b00039e36e970025b5d74e9ac9`.
  Package version `0.1.0` alone does not identify the tested toolkit.
- Reference adoption: firmware HEAD
  `3a1a50ca5db117120405f02ed94f48fef42e18e5`, `corebit.toml`, native harness
  settings, and `docs/agents/firmware-validation.md` plus its #118 evidence.
  SDK merged PR #126, commit `0edde6cf42b16a460768cf68b6baf7bdc85138a0`,
  supplies the root check/tracker configuration and thin tracker pointer.
- Bitwatch HEAD `097ffdf3a508ce26e7fa118d6e49fe9aca38be79`: inspected
  `CONTEXT.md`, `.steering/TESTING.md`, and existing Claude settings for
  repository-specific discovery and command ownership. Its Agentic #186
  adoption is still open; its legacy installed-helper paths are not a current
  adoption model.
- Rust/Cargo `1.97.1`, native target `x86_64-pc-windows-msvc`; Python
  `3.13.15`, uv `0.12.21`, Git `2.45.1.windows.1`, GitHub CLI `2.92.0`.
  The alternate Rust/Cargo toolchain is installed `1.92.0`
  (`ded5c06cf`, 2025-12-08), also with MSVC, rustfmt and Clippy.
- Agentic prerequisite ETs #29, #30, #34, and #50 were closed when checked.
  The pin contains the merged root `corebit.toml` contract. No unmerged
  prerequisite or further prerequisite merge order is required for this pin.
  Changing the pin requires revalidating affected evidence. #38 remains open.

The source-owned sibling Agentic checkout was verified clean at the pin.
No installed toolkit copies or global harness settings were changed. Machine
artifacts are under ignored `.agent/fletch-6/`; the durable local brief is
`.agent/task-brief.json` and carries relative source/evidence paths and
canonical GitHub URLs.

## Commands and results

`<cli>` below means this command, run from the Fletch worktree root:

```text
uv run --isolated --project <agentic-checkout>/tools --python 3.13 corebit
```

| Scenario / command | Result | Evidence and limits |
| --- | --- | --- |
| `<cli> local validate --json` with Rust 1.97.1 | Failed | `rust-format` and all six default integration tests passed. `rust-view-check` exits 101: locked Polars dependency `ethnum 1.5.2` emits E0512 because `transmute(())` targets the now 8-bit `TryFromIntError`. The runner stops; view tests and all-feature lint are Not run in this attempt. The unchanged lockfile/source and diagnostic identify a baseline toolchain/dependency failure. |
| Same declared validation with installed Rust 1.92 | Passed | All five checks passed: format, six default integration tests, optional-view check, eight view-enabled integration tests and all-target/all-feature Clippy. Existing unused-field warnings appear in derived test/example structs. No toolchain override file, dependency update or registry patch was introduced. |
| `<cli> show preflight --repo . --project https://github.com/orgs/corebit-instruments/projects/1 --owner corebit-instruments/fletch --pr-base staging --json` | Passed | Correct root, branch and HEAD; discovers `AGENTS.md`, `CLAUDE.md` and both steering files. GitHub read access resolves current Fletch identity and live default `main`, separately from intended base `staging`. |
| Same preflight from `src/` | Passed | Resolves the same worktree root and instruction index. |
| Parsed manifest and native plugin settings | Passed | Five non-shell argument arrays, Project 1, Fletch repository ownership, default new-ET Status `Todo`, intended base `staging`; all three plugin settings parse and exact fallback skill paths exist. Configuration verification is not live invocation. |
| `<cli> remote et-start 6 --json` (dry run) | Passed | Resolves the actual ET through `[tasks]`, reuses `chore/6-agentic-workflows` in this worktree, and validates the live Project's `In Progress` destination. Both mutation flags are false; the ET remains open and its existing board status is preserved. |
| Repeated `local brief`, copied brief and fresh preflight | Passed | Two temporary Git repositories with spaces/apostrophes in their paths; ownership, authorization, relative sources/evidence and next action survive relocation. Fresh preflight resolves the destination, rather than reusing the saved absolute root. |
| Failing validation fixture | Passed | First check exits 7; CLI exits 1 and identifies the failed check; its second check never executes. |
| Missing-worktree preflight fixture | Passed | Reports `blocked`, `repository directory unavailable`, and exit 1. |
| Targeted pinned-toolkit adapter tests | Passed, one Not run | 145 tests considered, 144 passed; one symlink case skipped because Windows symlink privilege was unavailable. Coverage is toolkit fixtures, not live harness or GitHub writes. |
| `<cli> doctor --json` | Failed | Isolated CLI location fails the permanent PATH check; this worktree has no Codex project trust entry. Git, uv, authentication, toolkit access and installed harness plugin listings pass. Stable release discovery is skipped because no stable release is reported. |
| Repository and Project identity | Passed | Former `Ruben1729/fletch`, `modulo-org/fletch` and current URLs resolve repository ID `1164463026`; origin and Cargo package metadata now use the canonical Fletch address. Project 1 is the organization's Modulo Project; #6 is already on it. Its six ET Status options are present. No Project mutation was performed. |
| Preservation | Passed | All 15 tracked library manifests, lockfile, sources, derive sources, tests, examples and steering files match the implementation baseline apart from the Cargo package repository URL. Original checkout stays on `refactor/parquet-dynamic-streams` with its pre-existing `AGENTS.md` edit intact. New ignore entries cover local briefs, scratch and worktrees. |

Temporary adoption scenarios ran with:

```text
uv run --isolated --project <agentic-checkout>/tools --python 3.13 python .agent/fletch-6/verify_adoption.py
```

Results are in `adoption-scenarios.json`; root/nested and relocated preflight
snapshots, the fail-fast fixture and `doctor.json` are in the same ignored
directory. Rust output is `rust-validation.log` for 1.97.1 and
`rust-1.92-validation.log` for the alternate toolchain. Validation reused the
original Fletch `target/` cache through `CARGO_TARGET_DIR`, with
`CARGO_BUILD_JOBS=2`; the alternate attempt additionally set
`RUSTUP_TOOLCHAIN=1.92` for that process only. Targeted toolkit tests use:

```text
uv run --isolated --project .. --python 3.13 python -m unittest test_preflight test_brief test_manifest test_runner test_process test_harnesses test_pulls test_tasks
```

Run that command from the pinned toolkit's `tools/tests` directory. It checks
path/quoting, timeout and exit handling, handoff/discovery, manifest parsing,
mocked harness installation/preservation and ET/PR branch/closing behavior.

## Epic acceptance mapping and pending checks

| Criterion | Fletch evidence | Pending / Not run |
| --- | --- | --- |
| A16: discovery and resume | Root/nested discovery and relocated handoff fixtures passed on Windows. Generic policy remains in Agentic, with only repository facts and pointers here. | Fresh interactive task invocation/resume in Codex, Claude Code and OpenCode; macOS/Linux execution. |
| A17: capability reporting and preservation | Preflight verifies read access and current identity; all declared Rust checks pass on 1.92; baseline files and existing checkout edits are preserved. The 1.97.1 dependency failure and machine doctor findings remain separate. | Resolve permanent machine installation/PATH and Codex project trust through canonical setup; current dependencies do not pass the view check on 1.97.1. Successful preflight alone does not establish runtime readiness. |
| A18: deterministic portable mechanics | Windows paths with spaces/apostrophes, repeated handoff, relocation and fail-fast scenarios passed using temporary fixtures; 144 targeted toolkit adapter tests passed. | Unavailable Windows symlink privilege, macOS/Linux execution and live GitHub pagination, partial writes or closure scenarios; no external write or closure test was run. |
| A19: current identity and active references | Current repository/Project mapping and renamed origin verified; shared skills and enablement point to Agentic/Corebit. Fletch `staging` policy is preserved independently from GitHub default `main`. | Clean installation and existing-installation upgrade with the pin across all three harnesses/OSes; historical board/view migration was not exercised here. |

These pending combinations belong to the shared rollout/E2E boundary. No
unrun combination counts as Passed, and neither merging this adoption nor
passing library checks supplies the human acceptance required by Agentic #38.
