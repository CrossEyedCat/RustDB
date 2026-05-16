//! Native TPC-C transaction dispatch (one wire round-trip per txn).

use super::{acquire_table_storage_write_lock, table_storage_lock_arc, SqlEngine, SqlEngineState};
use crate::network::engine::{EngineError, EngineOutput, SessionContext};
use crate::tpcc_workload::{txn_sql, TxnKind};
use std::sync::{Arc, RwLockWriteGuard};

fn txn_kind_from_u8(kind: u8) -> Result<TxnKind, EngineError> {
    match kind {
        0 => Ok(TxnKind::NewOrder),
        1 => Ok(TxnKind::Payment),
        2 => Ok(TxnKind::OrderStatus),
        3 => Ok(TxnKind::Delivery),
        4 => Ok(TxnKind::StockLevel),
        _ => Err(EngineError::new(
            crate::network::engine::engine_error_code::PROTOCOL,
            format!("unknown TPC-C kind {kind}"),
        )),
    }
}

fn tables_for_kind(kind: TxnKind) -> &'static [&'static str] {
    match kind {
        TxnKind::NewOrder => &["district", "oorder", "new_order", "stock", "order_line"],
        TxnKind::Payment => &["warehouse", "district", "customer"],
        TxnKind::OrderStatus => &["oorder"],
        TxnKind::Delivery => &["new_order"],
        TxnKind::StockLevel => &["stock"],
    }
}

pub(crate) fn execute_tpcc(
    state: &SqlEngineState,
    kind: u8,
    seed: u64,
    global_txn_id: u64,
    ctx: &mut SessionContext,
) -> Result<EngineOutput, EngineError> {
    let txn_kind = txn_kind_from_u8(kind)?;
    let sqls = txn_sql(txn_kind, seed, global_txn_id);
    let mut table_names: Vec<&str> = tables_for_kind(txn_kind).to_vec();
    table_names.sort_unstable();
    table_names.dedup();

    let mut table_locks: Vec<(String, Arc<std::sync::RwLock<()>>)> =
        Vec::with_capacity(table_names.len());
    for table in table_names {
        table_locks.push((table.to_string(), table_storage_lock_arc(state, table)?));
    }
    let mut guards: Vec<RwLockWriteGuard<'_, ()>> = Vec::with_capacity(table_locks.len());
    for (table, lock) in &table_locks {
        guards.push(acquire_table_storage_write_lock(lock, table)?);
    }

    let prev_skip = ctx.skip_dml_storage_lock;
    ctx.skip_dml_storage_lock = true;
    let mut rows_affected = 0u64;
    let run = (|| -> Result<EngineOutput, EngineError> {
        for sql in &sqls {
            let out = SqlEngine::execute_sql_inner(state, sql, ctx)?;
            if let EngineOutput::ExecutionOk { rows_affected: n } = out {
                rows_affected = rows_affected.saturating_add(n);
            }
        }
        Ok(EngineOutput::ExecutionOk { rows_affected })
    })();
    ctx.skip_dml_storage_lock = prev_skip;
    run
}
