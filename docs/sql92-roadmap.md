# SQL-92 implementation roadmap (RustDB)

This document is a practical, file-by-file plan for incrementally expanding RustDB toward a SQL‑92-style feature set. It is **not** a claim of full SQL‑92 compliance; it is a structured TODO list.

## Guiding principles

- Prefer **small PRs**: one feature family per PR with focused tests.
- Build in layers: **parser/AST → analyzer → planner → executor → catalog/storage enforcement**.
- Add tests at each layer:
  - Parser tests for syntax shape
  - Analyzer/type tests for name+type rules
  - Planner tests for plan shape
  - Executor/e2e tests for result correctness

## Phase 0 — Baseline and tracking

- **Create and maintain a checklist**
  - Add/keep this doc as the source of truth (tick items as implemented).
  - Optionally add a short “SQL coverage” section in `README.md` pointing here.

## Phase 1 — Parser/AST: syntax coverage first

### Files/modules

- **AST definitions**
  - `src/parser/ast.rs`
- **Parser implementation**
  - `src/parser/parser.rs`
- **Lexer/tokens (only if needed)**
  - `src/parser/token.rs`
  - `src/parser/lexer_readers.rs`
- **Parser tests**
  - `src/parser/tests/select_clauses_tests.rs`
  - `src/parser/tests/parser_tests.rs`
  - `src/parser/tests/ast_tests.rs`
  - `src/parser/tests/token_coverage_tests.rs`

### TODO (Parser/AST)

- **Predicates / expressions**
  - [x] `IS NULL` / `IS NOT NULL`
  - [x] `LIKE` (and later `ESCAPE`)
  - [x] `BETWEEN`
  - [x] `IN (value, ...)`
  - [x] `IN (subquery)`
  - [x] `EXISTS (subquery)`
  - [x] `CASE ... WHEN ... THEN ... ELSE ... END`
- **Subqueries**
  - [x] Subquery in `FROM`: `FROM (SELECT ...) AS alias`
  - [ ] Correlated subquery placeholders in the AST (scoping will be analyzer work)
- **JOIN**
  - [x] `JOIN ... USING(col, ...)` (syntax + AST representation)
- **Set operations**
  - [x] `UNION [ALL]`
  - [x] `INTERSECT`
  - [x] `EXCEPT`
- **CREATE TABLE constraints (syntax)**
  - [x] Column constraints: `NOT NULL`, `DEFAULT`, `CHECK`, `UNIQUE`, `PRIMARY KEY`, `REFERENCES`
  - [x] Table constraints: `PRIMARY KEY(...)`, `UNIQUE(...)`, `FOREIGN KEY(...) REFERENCES ...`, `CHECK(...)`

## Phase 2 — Analyzer: names, types, and NULL semantics

### Files/modules

- `src/analyzer/semantic_analyzer.rs` (+ `src/analyzer/tests/semantic_analyzer_tests.rs`)
- `src/analyzer/type_checker.rs` (+ `src/analyzer/tests/type_checker_tests.rs`)
- `src/analyzer/access_checker.rs` (+ `src/analyzer/tests/access_checker_tests.rs`)
- `src/analyzer/metadata_cache.rs` (if schema/catalog lookup needs caching)

### TODO (Analyzer)

- **Name resolution**
  - [x] Table aliases, column qualification (`t.col`)
  - [x] Scope rules for subqueries (including correlation)
- **Type checking**
  - [x] `CASE` result type unification
  - [x] Predicate typing for `IN/EXISTS/LIKE/BETWEEN/IS NULL`
  - [x] Implicit/explicit casts strategy (even if minimal)
- **SQL NULL behavior**
  - [x] 3-valued logic (TRUE/FALSE/UNKNOWN) model and rules for predicates
  - [x] Consistent handling of `NULL` in comparisons

## Phase 3 — Planner: plan nodes and rewrites

### Files/modules

- `src/planner/planner.rs` (+ `src/planner/tests/planner_tests.rs`)
- `src/planner/optimizer.rs` (+ `src/planner/tests/optimizer_tests.rs`)
- `src/planner/advanced_optimizer.rs` (+ `src/planner/tests/advanced_optimizer_tests.rs`)

### TODO (Planner)

- **New logical operators**
  - [x] `Distinct` (AST + parser baseline: `SELECT DISTINCT ...`)
    - Planner/executor operator is still TODO (see Phase 4: `Distinct`)
  - [x] Set operators: `Union`, `Intersect`, `Except` (planner baseline)
    - `SqlStatement::SetOperation` plans into `PlanNode::SetOp` (executor support is Phase 4)
  - [x] Subquery planning: initial naive approach first (nested evaluation), then rewrites
    - Baseline: EXISTS subquery can be rewritten into a semi-join plan node
- **Rewrites / optimizations (after correctness)**
  - [x] `EXISTS/IN` → semi-join / anti-join where possible (baseline)
    - Implemented `EXISTS(...)` → `SemiJoin` rewrite when a simple FROM table exists in subquery
  - [x] Predicate pushdown improvements
    - Keeps correctness: never drops a `Filter` over `Join` unless fully pushed down
    - Fixes e2e join+where behavior while retaining existing executor semantics

## Phase 4 — Executor: correctness for new operators + expression evaluation

### Files/modules

- `src/executor/executor.rs`
- `src/executor/operators.rs`
- `src/executor/result.rs`
- Tests:
  - `src/executor/tests/plan_execution_tests.rs`
  - `src/executor/tests/operators_extra_tests.rs`
  - `src/executor/tests/join_operator_tests.rs`
  - `src/network/tests/sql_full_query_tests.rs` (end-to-end SQL path, if applicable)

### TODO (Executor)

- **Expression evaluation**
  - [x] `IS NULL`, `LIKE`, `BETWEEN`, `IN` (values), `CASE`
  - [x] 3-valued logic semantics wired through filter evaluation
- **Operators**
  - [x] `Distinct` (hash-based)
  - [x] `Union/Intersect/Except` (baseline)
  - [x] `EXISTS/IN` execution strategy (baseline: uncorrelated `EXISTS` via semi-join + short-circuit)

## Phase 5 — DDL + catalog + constraint enforcement (where SQL feels “real”)

### Files/modules

- Catalog/tests to orient yourself:
  - `src/catalog/tests/schema_tests.rs`
  - `src/catalog/tests/access_tests.rs`
- Likely implementation areas (depends on current wiring):
  - `src/catalog/**`
  - `src/storage/**`
  - `src/core/**` (transactions/locking interactions)

### TODO (Constraints)

- **Catalog representation**
  - [x] Store constraint metadata (names, kind, columns, referenced targets)
    - Implemented minimal in `SchemaManager`: per-table columns + CHECK constraints (name + AST)
- **Enforcement on write paths**
  - [x] `NOT NULL` and `DEFAULT` on INSERT/UPDATE
  - [x] `CHECK` evaluation on rows
  - [x] `UNIQUE` / `PRIMARY KEY` (in-memory key maps + conflict detection; `src/network/sql_constraints.rs`)
  - [x] `FOREIGN KEY` checks (referential integrity + delete parent blocked when children reference)
- **ALTER TABLE**
  - [x] `ALTER TABLE ... ADD CONSTRAINT ...` (PRIMARY KEY / UNIQUE / FOREIGN KEY / CHECK)
  - [x] `ALTER TABLE ... DROP CONSTRAINT ...`
- **DROP semantics**
  - [x] `RESTRICT` (default): refuse drop when other tables have FKs to this table; `CASCADE`: drop dependents first (`src/network/sql_engine.rs`)

## Phase 6 — Transactions and concurrency (to keep invariants true)

### Files/modules

- `src/core/tests/acid_tests.rs`
- `src/core/tests/transaction_tests.rs`
- `src/core/tests/lock_tests.rs`

### TODO (Concurrency)

- [x] Ensure constraints remain correct under concurrent INSERT/UPDATE (`SqlEngineState.storage_access` `RwLock` + per-statement write lock around DML/DDL; see `src/network/sql_engine.rs`)
- [x] Minimal isolation baseline: **read committed at statement level** (`SessionContext.transaction` + undo log; `SqlIsolationLevel::ReadCommitted` in `src/network/engine.rs`)

## Recommended PR slicing (low-risk order)

1. **Parser-only PRs**: `IS NULL`, `LIKE`, `BETWEEN`, `IN (list)`, `CASE` with parser tests.
2. **Analyzer PRs**: name resolution + types for the above.
3. **Executor PRs**: expression evaluation + filter semantics (including NULL logic).
4. **Subqueries**: `EXISTS` then `IN (subquery)` (correctness first, then semi-join rewrites).
5. **Set ops**: `UNION [ALL]` baseline, then `INTERSECT/EXCEPT`.
6. **Constraints**: start with `NOT NULL` + `DEFAULT`, then `UNIQUE/PK`, then `CHECK`, then `FK`.

