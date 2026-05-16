//! Native TPC-C transaction dispatch (one wire round-trip per txn).
//!
//! Each statement uses the normal [`SqlEngine::execute_sql_inner`] path (per-table or per-row
//! locks acquired inside the engine). No upfront multi-table write locks.

use super::{SqlEngine, SqlEngineState};
use crate::network::engine::{EngineError, EngineOutput, SessionContext};
use crate::tpcc_workload::{txn_sql, TxnKind};

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

pub(crate) fn execute_tpcc(
    state: &SqlEngineState,
    kind: u8,
    seed: u64,
    global_txn_id: u64,
    ctx: &mut SessionContext,
) -> Result<EngineOutput, EngineError> {
    let txn_kind = txn_kind_from_u8(kind)?;
    let sqls = txn_sql(txn_kind, seed, global_txn_id);
    let mut rows_affected = 0u64;
    for sql in &sqls {
        let out = SqlEngine::execute_sql_inner(state, sql, ctx)?;
        if let EngineOutput::ExecutionOk { rows_affected: n } = out {
            rows_affected = rows_affected.saturating_add(n);
        }
    }
    Ok(EngineOutput::ExecutionOk { rows_affected })
}
