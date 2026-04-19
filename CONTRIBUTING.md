# Contributing to RustDB

Thank you for your interest in RustDB. The project **welcomes contributions**: bug fixes, tests, documentation, benchmarks, and architecture discussion. Below are guidelines for **issues** and **pull requests**, plus a description of each **CI job**.

---

## Expectations and tone

- Be respectful; constructive criticism is welcome.
- The project is under **active development**: not everything is documented perfectly; if something is unclear, open an issue rather than guessing.
- **Do not advertise** RustDB as a production-ready database without caveats — see the **Status** section in `README.md`.

---

## Issues (when and how)

### When to open an issue

- **Bug**: reproducible incorrect behavior, crash, or hang.
- **Design proposal** (discuss before a large PR).
- **Architecture / build question** — if the answer is not in `README.md` or `docs/`.

### How to write it

1. **Title**: short summary of what is wrong or what you are asking about.
2. **Environment**: OS (prefer **Linux** for server-related paths), Rust version (`rustc --version`), commit or tag.
3. **Reproduction steps**: commands from a clean state, minimal SQL/config example.
4. **Actual vs expected** behavior.
5. **Logs** (trimmed to what matters), and if needed a trace snippet.

### When an issue may be closed without a code change

- Duplicate of an existing issue.
- Question fully answered by documentation.
- Requests outside project goals (e.g. “full Oracle compatibility”) — may be closed or redirected with an explanation.

### Security

A public issue is **not** the right place for undisclosed vulnerabilities with production exploitation details. If you find a serious security issue, describe it **without a step-by-step exploit**, or contact maintainers through repository contacts if listed. Ask for a dedicated policy if the project adds one.

---

## Pull requests

### Before you submit

1. Read `README.md` and relevant files under `docs/`.
2. For non-trivial changes, **align on direction** in an issue first so the PR is not rejected for scope.
3. Locally run:
   - `cargo fmt --all`
   - `cargo clippy --all-targets -- -D warnings`
   - `cargo test`, and for affected paths `cargo test --test integration_tests`
4. One PR — **one logical change** (easier review). Avoid mixing drive-by refactors with a bug fix.

### PR description

- **Title**: what changed (imperative or short summary).
- **Body**: why, how you tested, link to issue (`Fixes #123` / `Refs #123`).
- **Breaking changes** — list them explicitly.

### Branches

- Target branches: **`main`** and/or **`develop`** — follow whatever this repo uses; if unsure, check open PRs or ask in an issue.

### Review

- Maintainers may request changes; large PRs may be split when possible.
- **CI should be green** before merge (see job notes below for exceptions).

---

## CI/CD: what each job does

Workflow: [`.github/workflows/ci-cd.yml`](.github/workflows/ci-cd.yml). Pushes and pull requests to `main` and `develop` run the pipeline when code changes. Commits that touch **only** `**/*.md`, `docs/**`, `LICENSE*`, `architecture.puml`, etc. (see `paths-ignore`) **do not** start the workflow — convenient for doc-only edits, but CI will not run automatically for those.

| Job (name in GitHub Actions) | Purpose |
|------------------------------|---------|
| **Test Suite** (`test`) | **Ubuntu** matrix: `stable` and `beta`. `cargo test`, integration tests, doc tests. On **stable** additionally: `cargo audit`, **cargo-deny** (licenses, duplicates, advisories). |
| **Coverage** | `cargo llvm-cov`, upload to **Codecov**, **≥85%** line threshold with `ignore-filename-regex` for heavy/internal modules. |
| **Format Check** | `cargo fmt --all -- --check`. Depends on successful `test`. |
| **Clippy Check** | `cargo clippy --all-targets -- -D warnings`. Depends on `test`. |
| **MSRV** | `cargo check` on **Rust 1.90.0** (minimum supported Rust). Depends on `test`. |
| **Build Release** | `--release` build, **linux x64** binary artifact. Depends on format, clippy, msrv, coverage. |
| **Docker Build** | Build image, push to **GHCR**, output tag for downstream jobs. |
| **Docker stateful SQL smoke** | Image from GHCR + `scripts/sql_stateful_smoke.sh` (checks with a persistent volume). |
| **Docker QUIC SQL smoke** | Image + `scripts/sql_quic_smoke.sh` (QUIC client from the host). |
| **Benchmark SQLite vs RustDB (charts)** | **Push to `main` only**: benchmark RustDB (Docker) vs SQLite vs Postgres; charts and reports in artifact `sqlite-vs-rustdb-bench`. Heavy job. |
| **Loom concurrent SQL tests** | **Push to `main` or `workflow_dispatch`**: runs `cargo test --release` with `RUSTFLAGS='--cfg loom'` for `engine_concurrent_inserts_only_one_wins_same_pk` and `engine_alter_fk_many_inserts_under_contention` ([tokio-rs/loom](https://github.com/tokio-rs/loom)); artifact `loom-sql-engine-tests` (`loom-tests.log`). Optional `LOOM_MAX_PREEMPTIONS` via workflow input `loom_max_preemptions` (default `4`). |
| **Trace profile (QUIC select_table @128)** | **workflow_dispatch** only: Chrome trace and load metadata; artifact `trace-profile-quic-select-table`. Concurrency/queries come from the manual workflow run form. |
| **Flame Graph (rustdb_load)** | **workflow_dispatch** only: **perf** + **cargo flamegraph**, SVG in artifact `flamegraph-rustdb-load`. |
| **Documentation** | `cargo doc` with `-D warnings`; on push to `main`, deploy to **GitHub Pages** (if enabled). |
| **Deploy to GitHub Releases** | **Push to `main` only**: prerelease with source and Linux binary archives; depends on the **bench** job among others (see workflow). |
| **Notify** | Simple notification based on deploy result. |

**Note:** some jobs (benchmark, deploy) are gated on **main** and/or manual dispatch. On a **pull request**, the full set may differ because of `if:` conditions on jobs.

---

## License for contributions

By submitting a PR, you agree your contribution is licensed under the **repository license** (MIT), unless you agree otherwise in writing with the maintainers.

---

Thank you for helping improve RustDB.
