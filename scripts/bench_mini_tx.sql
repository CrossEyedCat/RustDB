-- One transaction: read + update + insert (same for RustDB and PostgreSQL).
-- {ix} is replaced with a unique integer per iteration (load generator).
BEGIN TRANSACTION
SELECT v FROM mini_main WHERE k = 1
UPDATE mini_main SET v = v + 1 WHERE k = 1
INSERT INTO mini_log (i, ref) VALUES ({ix}, 1)
COMMIT
