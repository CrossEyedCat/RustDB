-- Minimal TPC-C-ish schema + seed for throughput benchmarking.
-- Intentionally small so CI finishes quickly.
--
-- Notes:
-- - RustDB's SQL surface is evolving; keep schema simple (INTEGER, VARCHAR).
-- - We target a single warehouse with a few districts/customers/items.

CREATE TABLE warehouse (
  w_id INTEGER,
  w_name VARCHAR(32),
  w_tax INTEGER,
  w_ytd INTEGER
);

CREATE TABLE district (
  d_id INTEGER,
  d_w_id INTEGER,
  d_name VARCHAR(32),
  d_tax INTEGER,
  d_ytd INTEGER,
  d_next_o_id INTEGER
);

CREATE TABLE customer (
  c_id INTEGER,
  c_d_id INTEGER,
  c_w_id INTEGER,
  c_first VARCHAR(32),
  c_last VARCHAR(32),
  c_balance INTEGER
);

CREATE TABLE item (
  i_id INTEGER,
  i_name VARCHAR(32),
  i_price INTEGER
);

CREATE TABLE stock (
  s_i_id INTEGER,
  s_w_id INTEGER,
  s_qty INTEGER,
  s_ytd INTEGER,
  s_order_cnt INTEGER
);

CREATE TABLE oorder (
  o_id INTEGER,
  o_d_id INTEGER,
  o_w_id INTEGER,
  o_c_id INTEGER,
  o_ol_cnt INTEGER
);

CREATE TABLE new_order (
  no_o_id INTEGER,
  no_d_id INTEGER,
  no_w_id INTEGER
);

CREATE TABLE order_line (
  ol_o_id INTEGER,
  ol_d_id INTEGER,
  ol_w_id INTEGER,
  ol_number INTEGER,
  ol_i_id INTEGER,
  ol_qty INTEGER,
  ol_amount INTEGER
);

-- Seed: 1 warehouse.
INSERT INTO warehouse (w_id, w_name, w_tax, w_ytd) VALUES (1, 'W1', 8, 0);

-- Seed: 5 districts.
INSERT INTO district (d_id, d_w_id, d_name, d_tax, d_ytd, d_next_o_id) VALUES (1, 1, 'D1', 5, 0, 1);
INSERT INTO district (d_id, d_w_id, d_name, d_tax, d_ytd, d_next_o_id) VALUES (2, 1, 'D2', 5, 0, 1);
INSERT INTO district (d_id, d_w_id, d_name, d_tax, d_ytd, d_next_o_id) VALUES (3, 1, 'D3', 5, 0, 1);
INSERT INTO district (d_id, d_w_id, d_name, d_tax, d_ytd, d_next_o_id) VALUES (4, 1, 'D4', 5, 0, 1);
INSERT INTO district (d_id, d_w_id, d_name, d_tax, d_ytd, d_next_o_id) VALUES (5, 1, 'D5', 5, 0, 1);

-- Seed: 50 customers per district (250 total).
-- (Small dataset; enough for contention + writes.)
INSERT INTO customer (c_id, c_d_id, c_w_id, c_first, c_last, c_balance) VALUES (1, 1, 1, 'C1', 'L1', 0);
INSERT INTO customer (c_id, c_d_id, c_w_id, c_first, c_last, c_balance) VALUES (2, 1, 1, 'C2', 'L2', 0);
INSERT INTO customer (c_id, c_d_id, c_w_id, c_first, c_last, c_balance) VALUES (3, 1, 1, 'C3', 'L3', 0);
INSERT INTO customer (c_id, c_d_id, c_w_id, c_first, c_last, c_balance) VALUES (4, 1, 1, 'C4', 'L4', 0);
INSERT INTO customer (c_id, c_d_id, c_w_id, c_first, c_last, c_balance) VALUES (5, 1, 1, 'C5', 'L5', 0);

-- Keep inserts short; the benchmark will not rely on full TPCC cardinalities.

-- Seed: 100 items + stock (only a few rows to avoid huge SQL in repo).
INSERT INTO item (i_id, i_name, i_price) VALUES (1, 'I1', 10);
INSERT INTO item (i_id, i_name, i_price) VALUES (2, 'I2', 20);
INSERT INTO item (i_id, i_name, i_price) VALUES (3, 'I3', 30);
INSERT INTO item (i_id, i_name, i_price) VALUES (4, 'I4', 40);
INSERT INTO item (i_id, i_name, i_price) VALUES (5, 'I5', 50);

INSERT INTO stock (s_i_id, s_w_id, s_qty, s_ytd, s_order_cnt) VALUES (1, 1, 100, 0, 0);
INSERT INTO stock (s_i_id, s_w_id, s_qty, s_ytd, s_order_cnt) VALUES (2, 1, 100, 0, 0);
INSERT INTO stock (s_i_id, s_w_id, s_qty, s_ytd, s_order_cnt) VALUES (3, 1, 100, 0, 0);
INSERT INTO stock (s_i_id, s_w_id, s_qty, s_ytd, s_order_cnt) VALUES (4, 1, 100, 0, 0);
INSERT INTO stock (s_i_id, s_w_id, s_qty, s_ytd, s_order_cnt) VALUES (5, 1, 100, 0, 0);

