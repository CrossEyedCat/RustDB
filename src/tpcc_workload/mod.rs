//! Shared TPC-C-ish workload definitions and driver loop for `rustdb_tpcc` / `postgres_tpcc`.
//!
//! Not a full TPC-C implementation — mirrors the minimal schema in `scripts/tpcc_seed.sql`.

use async_trait::async_trait;
use serde::Serialize;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;

pub const MAX_LATENCY_SAMPLES: usize = 200_000;
pub const TXN_LOG_MAX_LINES: usize = 2_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnKind {
    NewOrder,
    Payment,
    OrderStatus,
    Delivery,
    StockLevel,
}

#[derive(Clone, Debug)]
pub struct Mix {
    cumulative: Vec<(TxnKind, f64)>,
}

impl Mix {
    pub fn parse(s: &str) -> Result<Self, String> {
        let mut w = Vec::new();
        for part in s.split(',').map(|p| p.trim()).filter(|p| !p.is_empty()) {
            let (k, v) = part
                .split_once('=')
                .ok_or_else(|| format!("bad mix item: {part}"))?;
            let val: f64 = v.parse().map_err(|_| format!("bad weight: {part}"))?;
            let kind = match k.trim() {
                "new_order" => TxnKind::NewOrder,
                "payment" => TxnKind::Payment,
                "order_status" => TxnKind::OrderStatus,
                "delivery" => TxnKind::Delivery,
                "stock_level" => TxnKind::StockLevel,
                other => return Err(format!("unknown kind: {other}")),
            };
            if val < 0.0 {
                return Err(format!("negative weight: {part}"));
            }
            w.push((kind, val));
        }
        if w.is_empty() {
            return Err("empty mix".to_string());
        }
        let sum: f64 = w.iter().map(|(_, x)| *x).sum();
        if sum <= 0.0 {
            return Err("mix sum must be > 0".to_string());
        }
        let mut cum = 0.0;
        let mut cumulative = Vec::with_capacity(w.len());
        for (k, x) in w {
            cum += x / sum;
            cumulative.push((k, cum.min(1.0)));
        }
        Ok(Self { cumulative })
    }

    pub fn pick(&self, u: f64) -> TxnKind {
        for (k, c) in &self.cumulative {
            if u <= *c {
                return *k;
            }
        }
        self.cumulative
            .last()
            .map(|x| x.0)
            .unwrap_or(TxnKind::NewOrder)
    }
}

pub fn lcg_next(state: &mut u64) -> u64 {
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
    *state
}

pub fn rand_f64_0_1(state: &mut u64) -> f64 {
    let x = lcg_next(state);
    let v = x >> 11;
    (v as f64) / ((1u64 << 53) as f64)
}

pub fn reservoir_sample_push(samples: &mut Vec<u128>, seen: &mut u64, rng: &mut u64, value: u128) {
    *seen = seen.wrapping_add(1);
    if samples.len() < MAX_LATENCY_SAMPLES {
        samples.push(value);
        return;
    }
    let j = (lcg_next(rng) % (*seen).max(1)) as usize;
    if j < samples.len() {
        samples[j] = value;
    }
}

pub fn txn_params(seed: u64, global_txn_id: u64) -> TpccTxnParams {
    let mut st = seed ^ (global_txn_id.wrapping_mul(0x9E3779B97F4A7C15));
    TpccTxnParams {
        w_id: 1,
        d_id: (lcg_next(&mut st) % 5 + 1) as i32,
        c_id: (lcg_next(&mut st) % 5 + 1) as i32,
        i_id: (lcg_next(&mut st) % 5 + 1) as i32,
        qty: (lcg_next(&mut st) % 5 + 1) as i32,
        o_id: global_txn_id as i64,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TpccTxnParams {
    pub w_id: i32,
    pub d_id: i32,
    pub c_id: i32,
    pub i_id: i32,
    pub qty: i32,
    pub o_id: i64,
}

pub fn txn_sql(kind: TxnKind, seed: u64, global_txn_id: u64) -> Vec<String> {
    let p = txn_params(seed, global_txn_id);
    let w_id = p.w_id;
    let d_id = p.d_id;
    let c_id = p.c_id;
    let i_id = p.i_id;
    let qty = p.qty;
    let o_id = p.o_id;

    match kind {
        TxnKind::NewOrder => vec![
            "BEGIN TRANSACTION".to_string(),
            format!(
                "UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id = {w_id} AND d_id = {d_id}"
            ),
            format!(
                "INSERT INTO oorder (o_id, o_d_id, o_w_id, o_c_id, o_ol_cnt) VALUES ({o_id}, {d_id}, {w_id}, {c_id}, 1)"
            ),
            format!(
                "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) VALUES ({o_id}, {d_id}, {w_id})"
            ),
            format!(
                "UPDATE stock SET s_qty = s_qty - {qty}, s_ytd = s_ytd + {qty}, s_order_cnt = s_order_cnt + 1 WHERE s_w_id = {w_id} AND s_i_id = {i_id}"
            ),
            format!(
                "INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_qty, ol_amount) VALUES ({o_id}, {d_id}, {w_id}, 1, {i_id}, {qty}, {qty}*10)"
            ),
            "COMMIT".to_string(),
        ],
        TxnKind::Payment => vec![
            "BEGIN TRANSACTION".to_string(),
            format!("UPDATE warehouse SET w_ytd = w_ytd + 1 WHERE w_id = {w_id}"),
            format!(
                "UPDATE district SET d_ytd = d_ytd + 1 WHERE d_w_id = {w_id} AND d_id = {d_id}"
            ),
            format!(
                "UPDATE customer SET c_balance = c_balance - 1 WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id}"
            ),
            "COMMIT".to_string(),
        ],
        TxnKind::OrderStatus => vec![
            "BEGIN TRANSACTION".to_string(),
            format!(
                "SELECT * FROM oorder WHERE o_w_id = {w_id} AND o_d_id = {d_id} AND o_c_id = {c_id}"
            ),
            "COMMIT".to_string(),
        ],
        TxnKind::Delivery => vec![
            "BEGIN TRANSACTION".to_string(),
            format!("DELETE FROM new_order WHERE no_w_id = {w_id} AND no_d_id = {d_id}"),
            "COMMIT".to_string(),
        ],
        // Stock-level uses a range predicate (`s_qty < 20`); the engine keeps the table read lock
        // and scans index prefix on `s_w_id` (see `idx_stock_ws` in tpcc_seed.sql).
        TxnKind::StockLevel => vec![
            "BEGIN TRANSACTION".to_string(),
            format!("SELECT * FROM stock WHERE s_w_id = {w_id} AND s_qty < 20"),
            "COMMIT".to_string(),
        ],
    }
}

pub fn quantile_ms(sorted_us: &[u128], q: f64) -> f64 {
    if sorted_us.is_empty() {
        return 0.0;
    }
    let q = q.clamp(0.0, 1.0);
    let idx = ((sorted_us.len() - 1) as f64 * q).round() as usize;
    sorted_us[idx] as f64 / 1000.0
}

fn skip_false(b: &bool) -> bool {
    !*b
}

#[derive(Serialize)]
pub struct OverallLatencyMs {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
}

#[derive(Serialize)]
pub struct TpccReport {
    #[serde(rename = "txn_attempts")]
    pub txn_attempts: u64,
    #[serde(rename = "txn_successes")]
    pub txn_successes: u64,
    pub transactions: usize,
    pub concurrency: usize,
    pub elapsed_s: f64,
    pub txns_per_s: f64,
    pub attempts_per_s: f64,
    pub success_rate_pct: f64,
    pub new_orders: u64,
    #[serde(rename = "tpmC")]
    pub tpm_c: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub overall_latency_ms: OverallLatencyMs,
    pub err: u64,
    pub mix: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txn_log_path: Option<String>,
    #[serde(default, skip_serializing_if = "skip_false")]
    pub txn_log_truncated: bool,
}

/// Wire / native API discriminant for [`TxnKind`].
pub fn txn_kind_as_u8(k: TxnKind) -> u8 {
    match k {
        TxnKind::NewOrder => 0,
        TxnKind::Payment => 1,
        TxnKind::OrderStatus => 2,
        TxnKind::Delivery => 3,
        TxnKind::StockLevel => 4,
    }
}

pub fn txn_kind_from_u8(v: u8) -> Option<TxnKind> {
    match v {
        0 => Some(TxnKind::NewOrder),
        1 => Some(TxnKind::Payment),
        2 => Some(TxnKind::OrderStatus),
        3 => Some(TxnKind::Delivery),
        4 => Some(TxnKind::StockLevel),
        _ => None,
    }
}

pub fn txn_kind_tag(k: TxnKind) -> &'static str {
    match k {
        TxnKind::NewOrder => "new_order",
        TxnKind::Payment => "payment",
        TxnKind::OrderStatus => "order_status",
        TxnKind::Delivery => "delivery",
        TxnKind::StockLevel => "stock_level",
    }
}

pub fn csv_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    let need_quote = s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r');
    if need_quote {
        out.push('"');
        for ch in s.chars() {
            if ch == '"' {
                out.push_str("\"\"");
            } else {
                out.push(ch);
            }
        }
        out.push('"');
        out
    } else {
        s.to_string()
    }
}

#[async_trait]
pub trait TpccExec: Send + Sync + 'static {
    async fn run_sql_batch(
        &self,
        sqls: &[String],
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Run one TPC-C txn via the same SQL statements as [`txn_sql`] (override for prepared PG, etc.).
    async fn run_kind(
        &self,
        kind: TxnKind,
        seed: u64,
        global_txn_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.run_sql_batch(&txn_sql(kind, seed, global_txn_id))
            .await
    }

    fn native_tpcc_enabled(&self) -> bool {
        false
    }

    async fn run_native_tpcc(
        &self,
        kind: TxnKind,
        seed: u64,
        global_txn_id: u64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let _ = (kind, seed, global_txn_id);
        Err("native TPC-C not supported".into())
    }
}

pub struct TpccRunConfig {
    pub concurrency: usize,
    pub transactions: usize,
    pub duration: Option<Duration>,
    pub mix: Mix,
    pub mix_string: String,
    pub txn_log: Option<PathBuf>,
    /// Use server `ExecuteTpcc` wire path when the executor supports it.
    pub use_native_tpcc: bool,
}

pub async fn run_tpcc<E: TpccExec>(
    workers: Vec<Arc<E>>,
    config: TpccRunConfig,
) -> Result<TpccReport, Box<dyn std::error::Error + Send + Sync>> {
    let concurrency = config.concurrency.max(1);
    if workers.len() != concurrency {
        return Err(format!(
            "worker count {} != concurrency {}",
            workers.len(),
            concurrency
        )
        .into());
    }

    let sem = Arc::new(Semaphore::new(concurrency));
    let tx_total = config.transactions.max(1);
    let deadline = config
        .duration
        .map(|d| Instant::now() + d.max(Duration::from_secs(1)));
    let global_txn_counter = Arc::new(AtomicU64::new(0));
    let use_native_tpcc = config.use_native_tpcc;

    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);

    for worker_id in 0..concurrency {
        let permit = sem.clone().acquire_owned().await?;
        let exec = workers[worker_id].clone();
        let mix = config.mix.clone();
        let global_txn_counter = global_txn_counter.clone();
        let mix_str = config.mix_string.clone();
        let want_log = config.txn_log.is_some();
        let worker_deadline = deadline;

        let base = tx_total / concurrency;
        let extra = (worker_id < (tx_total % concurrency)) as usize;
        let my_tx = base + extra;
        let start_index = worker_id * base + worker_id.min(tx_total % concurrency);

        handles.push(tokio::spawn(async move {
            let _permit = permit;
            let mut lat_ok: Vec<u128> = Vec::with_capacity(my_tx);
            let seed = 0xC0FFEE_u64 ^ (worker_id as u64).wrapping_mul(0xA5A5A5A5A5A5A5A5);
            let mut seen: u64 = 0;
            let mut rng = seed ^ 0xD6E8FEB86659FD93;
            let mut attempts: u64 = 0;
            let mut successes: u64 = 0;
            let mut new_orders_ok: u64 = 0;
            let mut log_lines: Vec<String> = Vec::new();

            if let Some(dl) = worker_deadline {
                while Instant::now() < dl {
                    let global_tx = global_txn_counter.fetch_add(1, Ordering::Relaxed);
                    let mut st = seed ^ global_tx.wrapping_mul(0xD1B54A32D192ED03);
                    let u = rand_f64_0_1(&mut st);
                    let kind = mix.pick(u);
                    let t0 = Instant::now();
                    let res = if use_native_tpcc && exec.native_tpcc_enabled() {
                        exec.run_native_tpcc(kind, seed, global_tx).await
                    } else {
                        exec.run_kind(kind, seed, global_tx).await
                    };
                    let dt = t0.elapsed();
                    let us = dt.as_micros();
                    attempts = attempts.wrapping_add(1);
                    let ok = res.is_ok();
                    if ok {
                        successes = successes.wrapping_add(1);
                        if kind == TxnKind::NewOrder {
                            new_orders_ok = new_orders_ok.wrapping_add(1);
                        }
                        reservoir_sample_push(&mut lat_ok, &mut seen, &mut rng, us);
                    }
                    if want_log && log_lines.len() < TXN_LOG_MAX_LINES {
                        let err = res.err().map(|e| e.to_string()).unwrap_or_default();
                        log_lines.push(format!(
                            "{},{},{},{},{},{}",
                            worker_id,
                            global_tx,
                            txn_kind_tag(kind),
                            if ok { 1 } else { 0 },
                            us,
                            csv_escape(&err)
                        ));
                    }
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                    lat_ok,
                    mix_str,
                    attempts,
                    successes,
                    new_orders_ok,
                    log_lines,
                ))
            } else {
                for j in 0..my_tx {
                    let global_tx = (start_index + j) as u64;
                    let mut st = seed ^ global_tx.wrapping_mul(0xD1B54A32D192ED03);
                    let u = rand_f64_0_1(&mut st);
                    let kind = mix.pick(u);
                    let t0 = Instant::now();
                    let res = if use_native_tpcc && exec.native_tpcc_enabled() {
                        exec.run_native_tpcc(kind, seed, global_tx).await
                    } else {
                        exec.run_kind(kind, seed, global_tx).await
                    };
                    let dt = t0.elapsed();
                    let us = dt.as_micros();
                    attempts += 1;
                    let ok = res.is_ok();
                    if ok {
                        successes += 1;
                        if kind == TxnKind::NewOrder {
                            new_orders_ok += 1;
                        }
                        reservoir_sample_push(&mut lat_ok, &mut seen, &mut rng, us);
                    }
                    if want_log && log_lines.len() < TXN_LOG_MAX_LINES {
                        let err = res.err().map(|e| e.to_string()).unwrap_or_default();
                        log_lines.push(format!(
                            "{},{},{},{},{},{}",
                            worker_id,
                            global_tx,
                            txn_kind_tag(kind),
                            if ok { 1 } else { 0 },
                            us,
                            csv_escape(&err)
                        ));
                    }
                }
                Ok::<_, Box<dyn std::error::Error + Send + Sync>>((
                    lat_ok,
                    mix_str,
                    attempts,
                    successes,
                    new_orders_ok,
                    log_lines,
                ))
            }
        }));
    }

    let mut all_lat: Vec<u128> = Vec::with_capacity(tx_total);
    let mut mix_str = config.mix_string.clone();
    let mut total_attempts: u64 = 0;
    let mut total_successes: u64 = 0;
    let mut total_new_orders_ok: u64 = 0;
    let mut merged_log: Vec<String> = Vec::new();
    let mut log_truncated = false;

    for h in handles {
        let (mut lat, mx, att, succ, no_ok, lines) = h.await??;
        all_lat.append(&mut lat);
        mix_str = mx;
        total_attempts = total_attempts.wrapping_add(att);
        total_successes = total_successes.wrapping_add(succ);
        total_new_orders_ok = total_new_orders_ok.wrapping_add(no_ok);
        if config.txn_log.is_some() {
            let cap = TXN_LOG_MAX_LINES.saturating_sub(merged_log.len());
            if cap > 0 {
                merged_log.extend(lines.into_iter().take(cap));
            }
            if merged_log.len() >= TXN_LOG_MAX_LINES {
                log_truncated = true;
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64().max(1e-9);
    all_lat.sort_unstable();

    let txn_attempts = total_attempts;
    let txn_successes = total_successes;
    let err = txn_attempts.saturating_sub(txn_successes);
    let success_rate_pct = if txn_attempts > 0 {
        100.0 * (txn_successes as f64) / (txn_attempts as f64)
    } else {
        0.0
    };
    let txns_per_s = (txn_successes as f64) / elapsed;
    let attempts_per_s = (txn_attempts as f64) / elapsed;
    let tpmc = (total_new_orders_ok as f64) / (elapsed / 60.0);

    let p50 = quantile_ms(&all_lat, 0.50);
    let p95 = quantile_ms(&all_lat, 0.95);
    let p99 = quantile_ms(&all_lat, 0.99);

    if let Some(ref path) = config.txn_log {
        let header = "worker_id,global_attempt_id,kind,ok,elapsed_us,error\n";
        let mut out = header.to_string();
        for line in &merged_log {
            out.push_str(line);
            out.push('\n');
        }
        if log_truncated {
            out.push_str("# truncated: exceeded TXN_LOG_MAX_LINES\n");
        }
        std::fs::write(path, out)?;
    }

    Ok(TpccReport {
        txn_attempts,
        txn_successes,
        transactions: txn_attempts.try_into().unwrap_or(usize::MAX),
        concurrency,
        elapsed_s: elapsed,
        txns_per_s,
        attempts_per_s,
        success_rate_pct,
        new_orders: total_new_orders_ok,
        tpm_c: tpmc,
        p50_ms: p50,
        p95_ms: p95,
        p99_ms: p99,
        overall_latency_ms: OverallLatencyMs { p50, p95, p99 },
        err,
        mix: mix_str,
        txn_log_path: config.txn_log.as_ref().map(|p| p.display().to_string()),
        txn_log_truncated: log_truncated,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use std::sync::atomic::{AtomicU64, Ordering};
    use tempfile::tempdir;

    #[test]
    fn mix_parse_trims_and_normalizes_weights() {
        let m = Mix::parse(" new_order=2 , payment=2 ").unwrap();
        assert_eq!(m.cumulative.len(), 2);
        assert_eq!(m.pick(0.0), TxnKind::NewOrder);
        assert_eq!(m.pick(0.5), TxnKind::NewOrder);
        assert_eq!(m.pick(0.51), TxnKind::Payment);
        assert_eq!(m.pick(1.0), TxnKind::Payment);
    }

    #[test]
    fn mix_parse_errors() {
        assert!(Mix::parse("").unwrap_err().contains("empty"));
        assert!(Mix::parse("   ").unwrap_err().contains("empty"));
        assert!(Mix::parse("nope").unwrap_err().contains("bad mix"));
        assert!(Mix::parse("new_order=x")
            .unwrap_err()
            .contains("bad weight"));
        assert!(Mix::parse("unknown=1").unwrap_err().contains("unknown"));
        assert!(Mix::parse("new_order=-1").unwrap_err().contains("negative"));
        assert!(Mix::parse("new_order=0,payment=0")
            .unwrap_err()
            .contains("mix sum"));
        assert!(Mix::parse("new_order=0").unwrap_err().contains("mix sum"));
    }

    #[test]
    fn mix_pick_fallback_past_last_bucket() {
        let m = Mix::parse("new_order=1").unwrap();
        assert_eq!(m.pick(1.0000000001), TxnKind::NewOrder);
    }

    #[test]
    fn lcg_and_rand_and_quantile() {
        let mut s = 1_u64;
        let a = lcg_next(&mut s);
        let b = lcg_next(&mut s);
        assert_ne!(a, b);
        let mut r = 0xABC_u64;
        let x = rand_f64_0_1(&mut r);
        assert!((0.0..=1.0).contains(&x));
        assert_eq!(quantile_ms(&[], 0.5), 0.0);
        let one = vec![5000_u128];
        assert_eq!(quantile_ms(&one, 0.0), 5.0);
        assert_eq!(quantile_ms(&one, 1.0), 5.0);
        let mut v = vec![1000, 2000, 3000, 4000];
        v.sort_unstable();
        assert!(quantile_ms(&v, 0.5) > 0.0);
    }

    #[test]
    fn reservoir_under_cap_and_at_cap() {
        let mut samples = Vec::new();
        let mut seen = 0_u64;
        let mut rng = 0xFEED_u64;
        for i in 0..10_u128 {
            reservoir_sample_push(&mut samples, &mut seen, &mut rng, i);
        }
        assert_eq!(samples.len(), 10);
        assert_eq!(seen, 10);

        let mut samples: Vec<u128> = (0..MAX_LATENCY_SAMPLES as u128).collect();
        let mut seen = MAX_LATENCY_SAMPLES as u64;
        let extra = 50_usize;
        for i in 0..extra {
            reservoir_sample_push(&mut samples, &mut seen, &mut rng, 1000 + i as u128);
        }
        assert_eq!(samples.len(), MAX_LATENCY_SAMPLES);
        assert_eq!(seen, MAX_LATENCY_SAMPLES as u64 + extra as u64);
    }

    #[test]
    fn txn_params_matches_txn_sql_placeholders() {
        let p = txn_params(99, 42);
        let no = txn_sql(TxnKind::NewOrder, 99, 42);
        assert!(no[1].contains(&p.d_id.to_string()));
        assert!(no[2].contains(&p.o_id.to_string()));
        assert!(no[4].contains(&p.qty.to_string()));
    }

    #[test]
    fn txn_sql_each_kind_contains_expected_keywords() {
        for k in [
            TxnKind::NewOrder,
            TxnKind::Payment,
            TxnKind::OrderStatus,
            TxnKind::Delivery,
            TxnKind::StockLevel,
        ] {
            let sqls = txn_sql(k, 99, 42);
            assert!(!sqls.is_empty());
            assert!(sqls
                .iter()
                .any(|s| s.contains("BEGIN") || s.contains("BEGIN TRANSACTION")));
        }
        let no = txn_sql(TxnKind::NewOrder, 1, 7);
        assert!(no.iter().any(|s| s.contains("oorder")));
        let pay = txn_sql(TxnKind::Payment, 1, 7);
        assert!(pay.iter().any(|s| s.contains("warehouse")));
        let st = txn_sql(TxnKind::StockLevel, 3, 9);
        assert!(st.iter().any(|s| s.contains("stock")));
    }

    #[test]
    fn txn_kind_tag_roundtrip_names() {
        assert_eq!(txn_kind_tag(TxnKind::NewOrder), "new_order");
        assert_eq!(txn_kind_tag(TxnKind::Payment), "payment");
        assert_eq!(txn_kind_tag(TxnKind::OrderStatus), "order_status");
        assert_eq!(txn_kind_tag(TxnKind::Delivery), "delivery");
        assert_eq!(txn_kind_tag(TxnKind::StockLevel), "stock_level");
    }

    #[test]
    fn csv_escape_cases() {
        assert_eq!(csv_escape("ok"), "ok");
        assert_eq!(csv_escape("a,b"), "\"a,b\"");
        assert_eq!(csv_escape("say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(csv_escape("x\ny"), "\"x\ny\"");
        assert_eq!(csv_escape("r\rz"), "\"r\rz\"");
    }

    #[derive(Clone)]
    struct CountingExec {
        calls: Arc<AtomicU64>,
        fail_if: Arc<dyn Fn(u64) -> bool + Send + Sync>,
    }

    impl CountingExec {
        fn new(fail_if: impl Fn(u64) -> bool + Send + Sync + 'static) -> Self {
            Self {
                calls: Arc::new(AtomicU64::new(0)),
                fail_if: Arc::new(fail_if),
            }
        }
    }

    #[async_trait]
    impl TpccExec for CountingExec {
        async fn run_sql_batch(
            &self,
            sqls: &[String],
        ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
            let n = self.calls.fetch_add(1, Ordering::Relaxed);
            if !sqls.is_empty() && (self.fail_if)(n) {
                Err("injected".into())
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn run_tpcc_rejects_worker_mismatch() {
        let exec = Arc::new(CountingExec::new(|_| false));
        let mix = Mix::parse("new_order=1").unwrap();
        let err = run_tpcc(
            vec![exec],
            TpccRunConfig {
                concurrency: 2,
                transactions: 4,
                duration: None,
                mix,
                mix_string: "new_order=1".into(),
                txn_log: None,
                use_native_tpcc: false,
            },
        )
        .await
        .err()
        .expect("expected worker mismatch error");
        assert!(err.to_string().contains("worker count"));
    }

    #[tokio::test]
    async fn run_tpcc_fixed_count_ok_and_errors() {
        let mix = Mix::parse("new_order=1").unwrap();
        let exec_ok = Arc::new(CountingExec::new(|_| false));
        let workers: Vec<_> = (0..2).map(|_| exec_ok.clone()).collect();
        let rep = run_tpcc(
            workers,
            TpccRunConfig {
                concurrency: 2,
                transactions: 6,
                duration: None,
                mix: mix.clone(),
                mix_string: "new_order=1".into(),
                txn_log: None,
                use_native_tpcc: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(rep.txn_attempts, 6);
        assert_eq!(rep.txn_successes, 6);
        assert_eq!(rep.err, 0);
        assert!(rep.new_orders >= 1);

        let exec_flaky = Arc::new(CountingExec::new(|n| n % 2 == 1));
        let workers: Vec<_> = (0..2).map(|_| exec_flaky.clone()).collect();
        let rep2 = run_tpcc(
            workers,
            TpccRunConfig {
                concurrency: 2,
                transactions: 8,
                duration: None,
                mix,
                mix_string: "new_order=1".into(),
                txn_log: None,
                use_native_tpcc: false,
            },
        )
        .await
        .unwrap();
        assert_eq!(rep2.txn_attempts, 8);
        assert!(rep2.txn_successes < rep2.txn_attempts);
        assert!(rep2.err > 0);
    }

    #[tokio::test]
    async fn run_tpcc_duration_mode() {
        let mix = Mix::parse("payment=1").unwrap();
        let exec = Arc::new(CountingExec::new(|_| false));
        let workers: Vec<_> = (0..2).map(|_| exec.clone()).collect();
        let rep = run_tpcc(
            workers,
            TpccRunConfig {
                concurrency: 2,
                transactions: 1,
                duration: Some(Duration::from_millis(80)),
                mix,
                mix_string: "payment=1".into(),
                txn_log: None,
                use_native_tpcc: false,
            },
        )
        .await
        .unwrap();
        assert!(rep.txn_attempts >= 2);
        assert_eq!(rep.txn_successes, rep.txn_attempts);
    }

    #[tokio::test]
    async fn run_tpcc_txn_log_written() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("t.csv");
        let mix = Mix::parse("new_order=1").unwrap();
        let exec = Arc::new(CountingExec::new(|n| n == 2));
        let workers: Vec<_> = (0..2).map(|_| exec.clone()).collect();
        let rep = run_tpcc(
            workers,
            TpccRunConfig {
                concurrency: 2,
                transactions: 5,
                duration: None,
                mix,
                mix_string: "new_order=1".into(),
                txn_log: Some(path.clone()),
                use_native_tpcc: false,
            },
        )
        .await
        .unwrap();
        let body = std::fs::read_to_string(&path).unwrap();
        assert!(body.starts_with("worker_id,global_attempt_id,kind,ok,elapsed_us,error\n"));
        assert!(body.contains("new_order"));
        assert!(body.contains("injected") || body.contains("0") || body.contains("1"));
        assert_eq!(rep.txn_log_path.as_deref(), Some(path.to_str().unwrap()));
        assert!(!rep.txn_log_truncated);
    }

    #[test]
    fn tpcc_report_json_shape() {
        let r = TpccReport {
            txn_attempts: 10,
            txn_successes: 9,
            transactions: 10,
            concurrency: 2,
            elapsed_s: 1.0,
            txns_per_s: 9.0,
            attempts_per_s: 10.0,
            success_rate_pct: 90.0,
            new_orders: 3,
            tpm_c: 180.0,
            p50_ms: 1.0,
            p95_ms: 2.0,
            p99_ms: 3.0,
            overall_latency_ms: OverallLatencyMs {
                p50: 1.0,
                p95: 2.0,
                p99: 3.0,
            },
            err: 1,
            mix: "new_order=1".into(),
            txn_log_path: None,
            txn_log_truncated: false,
        };
        let s = serde_json::to_string(&r).unwrap();
        assert!(s.contains("\"txn_attempts\":10"));
        assert!(s.contains("\"tpmC\":180"));
        assert!(!s.contains("txn_log_truncated"));

        let r2 = TpccReport {
            txn_log_truncated: true,
            ..r
        };
        let s2 = serde_json::to_string(&r2).unwrap();
        assert!(s2.contains("txn_log_truncated"));
    }
}
