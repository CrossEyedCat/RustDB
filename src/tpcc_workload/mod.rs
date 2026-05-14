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

pub fn txn_sql(kind: TxnKind, seed: u64, global_txn_id: u64) -> Vec<String> {
    let mut st = seed ^ (global_txn_id.wrapping_mul(0x9E3779B97F4A7C15));
    let w_id = 1;
    let d_id = lcg_next(&mut st) % 5 + 1;
    let c_id = lcg_next(&mut st) % 5 + 1;
    let i_id = lcg_next(&mut st) % 5 + 1;
    let qty = lcg_next(&mut st) % 5 + 1;
    let o_id = global_txn_id;

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
}

pub struct TpccRunConfig {
    pub concurrency: usize,
    pub transactions: usize,
    pub duration: Option<Duration>,
    pub mix: Mix,
    pub mix_string: String,
    pub txn_log: Option<PathBuf>,
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

    let start = Instant::now();
    let mut handles = Vec::with_capacity(concurrency);

    for worker_id in 0..concurrency {
        let permit = sem.clone().acquire_owned().await?;
        let exec = workers[worker_id].clone();
        let mix = config.mix.clone();
        let global_txn_counter = global_txn_counter.clone();
        let mix_str = config.mix_string.clone();
        let want_log = config.txn_log.is_some();

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

            if let Some(dl) = deadline {
                while Instant::now() < dl {
                    let global_tx = global_txn_counter.fetch_add(1, Ordering::Relaxed);
                    let mut st = seed ^ global_tx.wrapping_mul(0xD1B54A32D192ED03);
                    let u = rand_f64_0_1(&mut st);
                    let kind = mix.pick(u);
                    let sqls = txn_sql(kind, seed, global_tx);
                    let t0 = Instant::now();
                    let res = exec.run_sql_batch(&sqls).await;
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
                    let sqls = txn_sql(kind, seed, global_tx);
                    let t0 = Instant::now();
                    let res = exec.run_sql_batch(&sqls).await;
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
