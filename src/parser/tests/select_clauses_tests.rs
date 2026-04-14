//! TDD-style parser coverage for SELECT with WHERE, ORDER BY, LIMIT, OFFSET.

use crate::parser::ast::{
    BinaryOperator, Expression, Literal, OrderDirection, SelectStatement, SqlStatement,
};
use crate::parser::SqlParser;

fn assert_select(sql: &str) -> SelectStatement {
    let mut p = SqlParser::new(sql).expect("parser");
    let stmt = p.parse().expect("parse");
    match stmt {
        SqlStatement::Select(s) => s,
        _ => panic!("expected SELECT"),
    }
}

#[test]
fn select_parses_where_greater_than() {
    let s = assert_select("SELECT a FROM t WHERE a > 0");
    assert!(!s.distinct);
    assert_eq!(
        s.where_clause,
        Some(Expression::BinaryOp {
            left: Box::new(Expression::Identifier("a".into())),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(Literal::Integer(0))),
        })
    );
}

#[test]
fn select_parses_where_and_or_not_parentheses() {
    let s = assert_select("SELECT a FROM t WHERE a = 2 AND NOT (b = 0)");
    assert!(s.where_clause.is_some());
}

#[test]
fn select_parses_where_equality() {
    let s = assert_select("SELECT a FROM t WHERE a = 1");
    assert!(s.from.is_some());
    assert_eq!(
        s.where_clause,
        Some(Expression::BinaryOp {
            left: Box::new(Expression::Identifier("a".into())),
            op: BinaryOperator::Equal,
            right: Box::new(Expression::Literal(Literal::Integer(1))),
        })
    );
}

#[test]
fn select_parses_order_by_asc_desc() {
    let s = assert_select("SELECT x FROM t ORDER BY x ASC, y DESC");
    assert_eq!(s.order_by.len(), 2);
    assert_eq!(s.order_by[0].direction, OrderDirection::Asc);
    assert_eq!(s.order_by[1].direction, OrderDirection::Desc);
}

#[test]
fn select_parses_limit_offset() {
    let s = assert_select("SELECT * FROM t LIMIT 10 OFFSET 3");
    assert_eq!(s.limit, Some(10));
    assert_eq!(s.offset, Some(3));
}

#[test]
fn select_parses_distinct() {
    let s = assert_select("SELECT DISTINCT a FROM t");
    assert!(s.distinct);
}

#[test]
fn select_full_clause_chain() {
    let s = assert_select("SELECT id FROM items WHERE id = 42 ORDER BY id DESC LIMIT 5 OFFSET 1");
    assert_eq!(s.select_list.len(), 1);
    match &s.select_list[0] {
        crate::parser::ast::SelectItem::Expression { expr, alias } => {
            assert!(alias.is_none());
            assert!(matches!(expr, Expression::Identifier(n) if n == "id"));
        }
        _ => panic!("expected expression select item"),
    }
    assert!(s.where_clause.is_some());
    assert_eq!(s.order_by.len(), 1);
    assert_eq!(s.order_by[0].direction, OrderDirection::Desc);
    assert_eq!(s.limit, Some(5));
    assert_eq!(s.offset, Some(1));
}

#[test]
fn select_parses_inner_join_on() {
    let s = assert_select("SELECT a FROM t1 INNER JOIN t2 ON t1.a = t2.a");
    let from = s.from.expect("from");
    assert_eq!(from.joins.len(), 1);
}
