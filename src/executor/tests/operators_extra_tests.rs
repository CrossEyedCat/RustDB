//! Дополнительное покрытие фабрики сканов и операторов.

use super::common;
use crate::executor::operators::{
    AggregationSortOperatorFactory, IndexCondition, IndexOperator, Operator, OperatorStatistics,
    ScanOperatorFactory,
};
use crate::common::Result;
use crate::common::types::{ColumnValue, DataType, Row};
use std::sync::{Arc, Mutex};

/// Возвращает несколько строк и завершается (без длинного table scan).
struct FewRows {
    idx: u8,
    versions: [u64; 3],
}

impl Operator for FewRows {
    fn next(&mut self) -> Result<Option<Row>> {
        if (self.idx as usize) >= self.versions.len() {
            return Ok(None);
        }
        let mut row = Row::new();
        row.version = self.versions[self.idx as usize];
        row.set_value(
            "a",
            ColumnValue::new(DataType::Varchar("a_val".into())),
        );
        self.idx += 1;
        Ok(Some(row))
    }

    fn reset(&mut self) -> Result<()> {
        self.idx = 0;
        Ok(())
    }

    fn get_schema(&self) -> Result<Vec<String>> {
        Ok(vec!["a".into()])
    }

    fn get_statistics(&self) -> OperatorStatistics {
        OperatorStatistics::default()
    }
}

#[test]
fn test_scan_factory_range_and_conditional() -> Result<()> {
    let (_tmp, pm) = common::create_test_page_manager();
    let factory = ScanOperatorFactory::new(pm.clone());
    let base = Box::new(FewRows {
        idx: 0,
        versions: [1, 5, 9],
    });
    let range = factory.create_range_scan(base, Some("1".to_string()), Some("9".to_string()))?;
    let mut r = range;
    let mut n = 0;
    while r.next()?.is_some() {
        n += 1;
        if n > 20 {
            panic!("range scan did not terminate");
        }
    }
    let base2 = Box::new(FewRows {
        idx: 0,
        versions: [1, 1, 1],
    });
    let cond = factory.create_conditional_scan(base2, "a".to_string())?;
    let mut c = cond;
    n = 0;
    while c.next()?.is_some() {
        n += 1;
        if n > 20 {
            panic!("conditional scan did not terminate");
        }
    }
    Ok(())
}

#[test]
fn test_aggregation_sort_factory_both_modes() -> Result<()> {
    use crate::executor::operators::{AggregateFunction, Operator as _};
    struct OneRow {
        done: bool,
    }
    impl Operator for OneRow {
        fn next(&mut self) -> Result<Option<crate::Row>> {
            if self.done {
                return Ok(None);
            }
            self.done = true;
            let mut row = crate::Row::new();
            row.set_value(
                "c0",
                crate::common::types::ColumnValue::new(crate::common::types::DataType::Integer(1)),
            );
            row.set_value(
                "c1",
                crate::common::types::ColumnValue::new(crate::common::types::DataType::Integer(2)),
            );
            Ok(Some(row))
        }
        fn reset(&mut self) -> Result<()> {
            self.done = false;
            Ok(())
        }
        fn get_schema(&self) -> Result<Vec<String>> {
            Ok(vec!["c0".into(), "c1".into()])
        }
        fn get_statistics(&self) -> crate::executor::operators::OperatorStatistics {
            crate::executor::operators::OperatorStatistics::default()
        }
    }
    let input = Box::new(OneRow { done: false });
    let mut h = AggregationSortOperatorFactory::create_group_by(
        input,
        vec![0],
        vec![(AggregateFunction::Count, 0)],
        vec!["g".into(), "cnt".into()],
        true,
    )?;
    let _ = h.next()?;
    let input2 = Box::new(OneRow { done: false });
    let mut s = AggregationSortOperatorFactory::create_group_by(
        input2,
        vec![0],
        vec![(AggregateFunction::Sum, 1)],
        vec!["g".into(), "s".into()],
        false,
    )?;
    let _ = s.next()?;
    Ok(())
}

#[test]
fn test_index_scan_operator_variants() -> Result<()> {
    let (_tmp, pm) = common::create_test_page_manager();
    let idx = Arc::new(Mutex::new(crate::storage::index::BPlusTree::new(3)));
    let schema = vec!["k".to_string(), "v".to_string()];
    for (iop, val) in [
        (IndexOperator::LessThan, "5"),
        (IndexOperator::LessThanOrEqual, "5"),
        (IndexOperator::GreaterThan, "1"),
        (IndexOperator::GreaterThanOrEqual, "1"),
        (IndexOperator::Between, "3"),
        (IndexOperator::In, "2"),
    ] {
        let conds = vec![IndexCondition {
            column: "k".to_string(),
            operator: iop,
            value: val.to_string(),
        }];
        let mut op_scan = crate::executor::operators::IndexScanOperator::new(
            "t".to_string(),
            "ix".to_string(),
            idx.clone(),
            pm.clone(),
            conds,
            schema.clone(),
        )?;
        let _ = op_scan.next()?;
    }
    Ok(())
}
