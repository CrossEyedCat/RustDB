//! Tests for AST builder and types

use crate::common::Result;
use crate::parser::ast::{
    AstBuilder, BinaryOperator, CreateTableStatement, DataType, Expression, InsertStatement,
    Literal, SqlStatement, TableReference,
};

#[test]
fn test_ast_builder_new_and_metadata() -> Result<()> {
    let mut b = AstBuilder::new()?;
    b.add_metadata("k".to_string(), "v".to_string());
    assert_eq!(b.get_metadata("k"), Some(&"v".to_string()));
    assert!(b.get_metadata("missing").is_none());
    Ok(())
}

#[test]
fn test_build_simple_select_star() -> Result<()> {
    let b = AstBuilder::new()?;
    let stmt = b.build_simple_select(vec!["*".to_string()], "users".to_string())?;
    assert!(matches!(stmt, SqlStatement::Select(_)));
    Ok(())
}

#[test]
fn test_build_simple_select_columns() -> Result<()> {
    let b = AstBuilder::new()?;
    let stmt =
        b.build_simple_select(vec!["id".to_string(), "name".to_string()], "t".to_string())?;
    assert!(matches!(stmt, SqlStatement::Select(_)));
    Ok(())
}

#[test]
fn test_build_simple_insert() -> Result<()> {
    let b = AstBuilder::new()?;
    let stmt = b.build_simple_insert(
        "users".to_string(),
        vec!["a".to_string()],
        vec![vec!["1".to_string()]],
    )?;
    assert!(matches!(stmt, SqlStatement::Insert(_)));
    Ok(())
}

#[test]
fn test_build_simple_insert_no_columns() -> Result<()> {
    let b = AstBuilder::new()?;
    let stmt = b.build_simple_insert("t".to_string(), vec![], vec![vec!["x".to_string()]])?;
    if let SqlStatement::Insert(InsertStatement { columns, .. }) = stmt {
        assert!(columns.is_none());
    } else {
        panic!("expected insert");
    }
    Ok(())
}

#[test]
fn test_sql_statement_variants_construct() {
    let sel = crate::parser::ast::SelectStatement {
        select_list: vec![crate::parser::ast::SelectItem::Wildcard],
        from: Some(crate::parser::ast::FromClause {
            table: TableReference::Table {
                name: "x".to_string(),
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
    };
    let _ = SqlStatement::Select(sel.clone());
    let _ = SqlStatement::BeginTransaction;
    let _ = SqlStatement::CommitTransaction;
    let _ = SqlStatement::RollbackTransaction;
    let _ = SqlStatement::CreateTable(CreateTableStatement {
        table_name: "t".to_string(),
        columns: vec![],
        constraints: vec![],
        if_not_exists: true,
    });
    let _ = Expression::BinaryOp {
        left: Box::new(Expression::Literal(Literal::Integer(1))),
        op: BinaryOperator::Add,
        right: Box::new(Expression::Literal(Literal::Integer(2))),
    };
    let _ = DataType::Decimal {
        precision: Some(10),
        scale: Some(2),
    };
}
