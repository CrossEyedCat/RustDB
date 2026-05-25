# EXPLAIN

RustDB supports PostgreSQL-style plan inspection via SQL:

```sql
EXPLAIN SELECT ...
EXPLAIN INSERT INTO ...
EXPLAIN ANALYZE UPDATE ...
EXPLAIN VERBOSE DELETE FROM ...
```

Keywords `ANALYZE` and `VERBOSE` may appear in any order after `EXPLAIN`.

## Output

Results are returned as a single-column result set named `QUERY PLAN` (one plan line per row). This works from:

- `rustdb query 'EXPLAIN ...'`
- QUIC `sql.query` frames (same `ResultSet` payload as ordinary SELECT)

## Supported statements

| Statement | `EXPLAIN` | `EXPLAIN ANALYZE` |
|-----------|-----------|-------------------|
| `SELECT`, set ops (`UNION` / …) | Yes | Yes (runs query) |
| `INSERT` / `UPDATE` / `DELETE` | Yes | Yes (runs DML) |
| DDL, transactions, `PREPARE` / `EXECUTE` | No | No |

Nested `EXPLAIN EXPLAIN ...` is rejected at parse time.

## `EXPLAIN ANALYZE` and side effects

Like PostgreSQL, **`EXPLAIN ANALYZE` executes the inner statement**. Inserts, updates, and deletes modify data and commit according to the current session (implicit auto-commit or open transaction).

The plan output appends:

- `Execution Time: <ms>`
- `Rows: <n>` for queries returning a result set
- `Rows Affected: <n>` for DML without a result set

Per-operator timings are not available yet (only total wall time).

## Logical vs physical plan (DML)

The displayed tree is the **logical plan** from the planner/optimizer (`Insert`, `Update`, `Delete`, `Table Scan`, …). DML execution in the engine uses specialized heap paths that may differ from executing `PlanNode::Update` in the generic executor. `EXPLAIN ANALYZE` still measures the real engine path.

## Examples

```bash
rustdb query "EXPLAIN SELECT w_id FROM warehouse WHERE w_id = 1"
rustdb query "EXPLAIN INSERT INTO t (a) VALUES (1)"
rustdb query "EXPLAIN ANALYZE UPDATE t SET a = 2 WHERE a = 1"
```

With `VERBOSE`, optimizer messages (index selection, predicate pushdown, etc.) are included when the optimizer emits them.
