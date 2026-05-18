//! Native TPC-C transaction dispatch (one wire round-trip per txn).
//!
//! Bypasses per-statement SQL parse/plan/execute; uses index-backed row locks and page latches.

use super::{
    delete_rows_by_equalities, equalities_map_i32, insert_row_tuple_tpcc_deferred,
    int_column_value, sql_phase_log_enabled, tpcc_order_status_row_count, tpcc_run_in_transaction,
    tpcc_stock_level_row_count, tuple_i32_field, update_rows_by_equalities, SqlEngineState,
};
use crate::network::engine::{EngineError, EngineOutput, SessionContext};
use crate::storage::tuple::Tuple;
use crate::tpcc_workload::{txn_kind_from_u8, TxnKind};
use std::time::Instant;
use tracing::info;

fn txn_kind_from_wire(kind: u8) -> Result<TxnKind, EngineError> {
    txn_kind_from_u8(kind).ok_or_else(|| {
        EngineError::new(
            crate::network::engine::engine_error_code::PROTOCOL,
            format!("unknown TPC-C kind {kind}"),
        )
    })
}

fn tpcc_params(seed: u64, global_txn_id: u64) -> (u64, i32, i32, i32, i32, i32, u64) {
    let mut st = seed ^ global_txn_id.wrapping_mul(0x9E3779B97F4A7C15);
    let w_id = 1i32;
    let d_id = (lcg_next(&mut st) % 5 + 1) as i32;
    let c_id = (lcg_next(&mut st) % 5 + 1) as i32;
    let i_id = (lcg_next(&mut st) % 5 + 1) as i32;
    let qty = (lcg_next(&mut st) % 5 + 1) as i32;
    let o_id = global_txn_id;
    (st, w_id, d_id, c_id, i_id, qty, o_id)
}

fn lcg_next(state: &mut u64) -> u64 {
    *state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
    *state
}

fn new_tuple_with_columns(id: u64, cols: &[(&str, i32)]) -> Tuple {
    let mut tuple = Tuple::new(id);
    for (name, val) in cols {
        tuple.set_value(name, int_column_value(*val));
    }
    tuple
}

fn new_order_line_tuple(id: u64, o_id: i32, d_id: i32, w_id: i32, i_id: i32, qty: i32) -> Tuple {
    new_tuple_with_columns(
        id,
        &[
            ("ol_o_id", o_id),
            ("ol_d_id", d_id),
            ("ol_w_id", w_id),
            ("ol_number", 1),
            ("ol_i_id", i_id),
            ("ol_qty", qty),
            ("ol_amount", qty * 10),
        ],
    )
}

fn phase_elapsed_us(t0: Instant) -> u64 {
    t0.elapsed().as_micros() as u64
}

fn run_new_order_native(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    w_id: i32,
    d_id: i32,
    c_id: i32,
    i_id: i32,
    qty: i32,
    o_id: u64,
) -> Result<u64, EngineError> {
    let profile = sql_phase_log_enabled();
    let mut district_us = 0u64;
    let mut district_update_us = 0u64;
    let mut insert_oorder_us = 0u64;
    let mut insert_new_order_us = 0u64;
    let mut insert_order_line_us = 0u64;
    let mut stock_us = 0u64;
    let mut wal_insert_us = 0u64;
    let mut index_sync_us = 0u64;

    let mut rows = 0u64;
    let mut district_phases = super::RowUpdatePhaseUs::default();
    rows += update_rows_by_equalities(
        state,
        ctx,
        "district",
        &equalities_map_i32(&[("d_w_id", w_id), ("d_id", d_id)]),
        |tuple| {
            let next = tuple_i32_field(tuple, "d_next_o_id")? + 1;
            tuple.set_value("d_next_o_id", int_column_value(next));
            Ok(())
        },
        if profile {
            Some(&mut district_phases)
        } else {
            None
        },
    )?;
    if profile {
        district_us = district_phases.lock_us;
        district_update_us = district_phases.update_us;
    }

    let t0 = profile.then(Instant::now);
    let id = state
        .next_tuple_id
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let ins = insert_row_tuple_tpcc_deferred(
        state,
        ctx,
        "oorder",
        new_tuple_with_columns(
            id,
            &[
                ("o_id", o_id as i32),
                ("o_d_id", d_id),
                ("o_w_id", w_id),
                ("o_c_id", c_id),
                ("o_ol_cnt", 1),
            ],
        ),
    )?;
    wal_insert_us += ins.wal_us;
    index_sync_us += ins.index_us;
    rows += 1;
    if let Some(t0) = t0 {
        insert_oorder_us = phase_elapsed_us(t0);
    }

    let t0 = profile.then(Instant::now);
    let id = state
        .next_tuple_id
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let ins = insert_row_tuple_tpcc_deferred(
        state,
        ctx,
        "new_order",
        new_tuple_with_columns(
            id,
            &[
                ("no_o_id", o_id as i32),
                ("no_d_id", d_id),
                ("no_w_id", w_id),
            ],
        ),
    )?;
    wal_insert_us += ins.wal_us;
    index_sync_us += ins.index_us;
    rows += 1;
    if let Some(t0) = t0 {
        insert_new_order_us = phase_elapsed_us(t0);
    }

    let t0 = profile.then(Instant::now);
    rows += update_rows_by_equalities(
        state,
        ctx,
        "stock",
        &equalities_map_i32(&[("s_w_id", w_id), ("s_i_id", i_id)]),
        |tuple| {
            let s_qty = tuple_i32_field(tuple, "s_qty")? - qty;
            let s_ytd = tuple_i32_field(tuple, "s_ytd")? + qty;
            let s_order_cnt = tuple_i32_field(tuple, "s_order_cnt")? + 1;
            tuple.set_value("s_qty", int_column_value(s_qty));
            tuple.set_value("s_ytd", int_column_value(s_ytd));
            tuple.set_value("s_order_cnt", int_column_value(s_order_cnt));
            Ok(())
        },
        None,
    )?;
    if let Some(t0) = t0 {
        stock_us = phase_elapsed_us(t0);
    }

    let t0 = profile.then(Instant::now);
    let id = state
        .next_tuple_id
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let ins = insert_row_tuple_tpcc_deferred(
        state,
        ctx,
        "order_line",
        new_order_line_tuple(id, o_id as i32, d_id, w_id, i_id, qty),
    )?;
    wal_insert_us += ins.wal_us;
    index_sync_us += ins.index_us;
    rows += 1;
    if let Some(t0) = t0 {
        insert_order_line_us = phase_elapsed_us(t0);
    }

    ctx.tpcc_dml_done_at = Some(Instant::now());

    if profile {
        info!(
            target: "rustdb::sql_phases",
            district_us,
            district_update_us,
            insert_oorder_us,
            insert_new_order_us,
            insert_order_line_us,
            stock_us,
            wal_insert_us,
            index_sync_us,
            "sql.execute_tpcc.new_order"
        );
    }
    Ok(rows)
}

fn run_payment(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    w_id: i32,
    d_id: i32,
    c_id: i32,
) -> Result<u64, EngineError> {
    let mut rows = 0u64;
    rows += update_rows_by_equalities(
        state,
        ctx,
        "warehouse",
        &equalities_map_i32(&[("w_id", w_id)]),
        |tuple| {
            let ytd = tuple_i32_field(tuple, "w_ytd")? + 1;
            tuple.set_value("w_ytd", int_column_value(ytd));
            Ok(())
        },
        None,
    )?;
    rows += update_rows_by_equalities(
        state,
        ctx,
        "district",
        &equalities_map_i32(&[("d_w_id", w_id), ("d_id", d_id)]),
        |tuple| {
            let ytd = tuple_i32_field(tuple, "d_ytd")? + 1;
            tuple.set_value("d_ytd", int_column_value(ytd));
            Ok(())
        },
        None,
    )?;
    rows += update_rows_by_equalities(
        state,
        ctx,
        "customer",
        &equalities_map_i32(&[("c_w_id", w_id), ("c_d_id", d_id), ("c_id", c_id)]),
        |tuple| {
            let bal = tuple_i32_field(tuple, "c_balance")? - 1;
            tuple.set_value("c_balance", int_column_value(bal));
            Ok(())
        },
        None,
    )?;
    Ok(rows)
}

fn run_delivery(
    state: &SqlEngineState,
    ctx: &mut SessionContext,
    w_id: i32,
    d_id: i32,
) -> Result<u64, EngineError> {
    delete_rows_by_equalities(
        state,
        ctx,
        "new_order",
        &equalities_map_i32(&[("no_w_id", w_id), ("no_d_id", d_id)]),
    )
}

pub(crate) fn execute_tpcc(
    state: &SqlEngineState,
    kind: u8,
    seed: u64,
    global_txn_id: u64,
    ctx: &mut SessionContext,
) -> Result<EngineOutput, EngineError> {
    let txn_kind = txn_kind_from_wire(kind)?;
    let (_st, w_id, d_id, c_id, i_id, qty, o_id) = tpcc_params(seed, global_txn_id);
    tpcc_run_in_transaction(state, ctx, kind, |state, ctx| {
        let rows = match txn_kind {
            TxnKind::NewOrder => {
                run_new_order_native(state, ctx, w_id, d_id, c_id, i_id, qty, o_id)?
            }
            TxnKind::Payment => run_payment(state, ctx, w_id, d_id, c_id)?,
            TxnKind::OrderStatus => tpcc_order_status_row_count(state, w_id, d_id, c_id)?,
            TxnKind::Delivery => run_delivery(state, ctx, w_id, d_id)?,
            TxnKind::StockLevel => tpcc_stock_level_row_count(state, w_id, 20)?,
        };
        Ok(rows)
    })
}
