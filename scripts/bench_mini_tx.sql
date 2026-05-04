-- One transaction: read + update + insert (same for RustDB and PostgreSQL).
-- {ix} = transaction index; {logpk} = unique PK for mini_log (rustdb_load: ix*4096+worker_id; SQLite/PG: ix).
BEGIN TRANSACTION
SELECT v FROM mini_main WHERE k = 1
UPDATE mini_main SET v = {ix} WHERE k = 1
INSERT INTO mini_log (i, ref) VALUES ({logpk}, 1)
COMMIT
