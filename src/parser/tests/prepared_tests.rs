//! Tests for PreparedStatementCache

use crate::common::Result;
use crate::parser::ast::{
    ExecuteStatement, Expression, FromClause, PrepareStatement, SelectItem, SelectStatement,
    SqlStatement, TableReference,
};
use crate::parser::prepared::PreparedStatementCache;

fn minimal_select_stmt() -> SqlStatement {
    SqlStatement::Select(SelectStatement {
        distinct: false,
        select_list: vec![SelectItem::Wildcard],
        from: Some(FromClause {
            table: TableReference::Table {
                name: "t".to_string(),
                alias: None,
            },
            joins: vec![],
        }),
        where_clause: None,
        group_by: vec![],
        having: None,
        order_by: vec![],
        limit: None,
        offset: None,
    })
}

#[test]
fn test_prepared_cache_new_and_empty() {
    let c = PreparedStatementCache::new();
    assert!(c.is_empty());
    assert_eq!(c.len(), 0);
    let d: PreparedStatementCache = PreparedStatementCache::default();
    assert!(d.is_empty());
}

#[test]
fn test_prepared_prepare_execute_deallocate() -> Result<()> {
    let c = PreparedStatementCache::new();
    let stmt = minimal_select_stmt();
    c.prepare(PrepareStatement {
        name: "q1".to_string(),
        statement: Box::new(stmt),
    })?;
    assert_eq!(c.len(), 1);
    let out = c.execute(ExecuteStatement {
        name: "q1".to_string(),
        params: vec![],
    })?;
    assert!(matches!(out, SqlStatement::Select(_)));
    c.deallocate("q1")?;
    assert!(c.is_empty());
    Ok(())
}

#[test]
fn test_prepared_execute_missing() {
    let c = PreparedStatementCache::new();
    let r = c.execute(ExecuteStatement {
        name: "none".to_string(),
        params: vec![],
    });
    assert!(r.is_err());
}

#[test]
fn test_prepared_param_mismatch() -> Result<()> {
    let c = PreparedStatementCache::new();
    c.prepare(PrepareStatement {
        name: "q".to_string(),
        statement: Box::new(minimal_select_stmt()),
    })?;
    let r = c.execute(ExecuteStatement {
        name: "q".to_string(),
        params: vec![Expression::Literal(crate::parser::ast::Literal::Integer(1))],
    });
    assert!(r.is_err());
    Ok(())
}
