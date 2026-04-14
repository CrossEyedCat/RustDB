//! Phase 1 (parser/AST) coverage: SQL-92-ish syntax extensions.

use crate::common::Result;
use crate::parser::ast::{
    ColumnConstraint, Expression, InList, JoinType, Literal, SelectItem, SetOperator, SqlStatement,
    TableConstraint, TableReference,
};
use crate::parser::SqlParser;

#[test]
fn where_parses_is_null_and_is_not_null() -> Result<()> {
    let mut p = SqlParser::new("SELECT a FROM t WHERE a IS NULL")?;
    let stmt = p.parse()?;
    let SqlStatement::Select(s) = stmt else {
        panic!("expected SELECT");
    };
    assert_eq!(
        s.where_clause,
        Some(Expression::IsNull {
            expr: Box::new(Expression::Identifier("a".into())),
            negated: false
        })
    );

    let mut p = SqlParser::new("SELECT a FROM t WHERE a IS NOT NULL")?;
    let stmt = p.parse()?;
    let SqlStatement::Select(s) = stmt else {
        panic!("expected SELECT");
    };
    assert_eq!(
        s.where_clause,
        Some(Expression::IsNull {
            expr: Box::new(Expression::Identifier("a".into())),
            negated: true
        })
    );
    Ok(())
}

#[test]
fn where_parses_like_and_not_like() -> Result<()> {
    let mut p = SqlParser::new("SELECT a FROM t WHERE a LIKE 'x%'")?;
    let stmt = p.parse()?;
    let SqlStatement::Select(s) = stmt else {
        panic!("expected SELECT");
    };
    assert_eq!(
        s.where_clause,
        Some(Expression::Like {
            expr: Box::new(Expression::Identifier("a".into())),
            // Current lexer keeps quotes in StringLiteral token values.
            pattern: Box::new(Expression::Literal(Literal::String("'x%'".into()))),
            negated: false
        })
    );

    let mut p = SqlParser::new("SELECT a FROM t WHERE a NOT LIKE 'x%'")?;
    let stmt = p.parse()?;
    let SqlStatement::Select(s) = stmt else {
        panic!("expected SELECT");
    };
    assert!(matches!(
        s.where_clause,
        Some(Expression::Like { negated: true, .. })
    ));
    Ok(())
}

#[test]
fn where_parses_between_and_in_list() -> Result<()> {
    let mut p = SqlParser::new("SELECT a FROM t WHERE a BETWEEN 1 AND 3")?;
    let stmt = p.parse()?;
    let SqlStatement::Select(s) = stmt else {
        panic!("expected SELECT");
    };
    assert!(matches!(s.where_clause, Some(Expression::Between { .. })));

    let mut p = SqlParser::new("SELECT a FROM t WHERE a IN (1, 2, 3)")?;
    let stmt = p.parse()?;
    let SqlStatement::Select(s) = stmt else {
        panic!("expected SELECT");
    };
    assert_eq!(
        s.where_clause,
        Some(Expression::In {
            expr: Box::new(Expression::Identifier("a".into())),
            list: InList::Values(vec![
                Expression::Literal(Literal::Integer(1)),
                Expression::Literal(Literal::Integer(2)),
                Expression::Literal(Literal::Integer(3)),
            ])
        })
    );
    Ok(())
}

#[test]
fn where_parses_exists_subquery() -> Result<()> {
    let mut p = SqlParser::new("SELECT 1 WHERE EXISTS (SELECT 1)")?;
    let stmt = p.parse()?;
    let SqlStatement::Select(s) = stmt else {
        panic!("expected SELECT");
    };
    assert!(matches!(s.where_clause, Some(Expression::Exists(_))));
    Ok(())
}

#[test]
fn select_parses_case_expression() -> Result<()> {
    let mut p = SqlParser::new("SELECT CASE WHEN 1 = 1 THEN 2 ELSE 3 END")?;
    let stmt = p.parse()?;
    let SqlStatement::Select(s) = stmt else {
        panic!("expected SELECT");
    };
    assert_eq!(s.select_list.len(), 1);
    match &s.select_list[0] {
        SelectItem::Expression { expr, .. } => {
            assert!(matches!(expr, Expression::Case { .. }));
        }
        _ => panic!("expected expression select item"),
    }
    Ok(())
}

#[test]
fn from_parses_subquery_and_join_using() -> Result<()> {
    let mut p = SqlParser::new("SELECT * FROM (SELECT 1) AS x")?;
    let stmt = p.parse()?;
    let SqlStatement::Select(s) = stmt else {
        panic!("expected SELECT");
    };
    let from = s.from.expect("from");
    assert!(matches!(from.table, TableReference::Subquery { .. }));

    let mut p = SqlParser::new("SELECT a FROM t1 JOIN t2 USING (a)")?;
    let stmt = p.parse()?;
    let SqlStatement::Select(s) = stmt else {
        panic!("expected SELECT");
    };
    let from = s.from.expect("from");
    assert_eq!(from.joins.len(), 1);
    assert_eq!(from.joins[0].join_type, JoinType::Inner);
    assert_eq!(from.joins[0].using_columns, Some(vec!["a".into()]));
    Ok(())
}

#[test]
fn create_table_parses_constraints() -> Result<()> {
    let sql = "CREATE TABLE t (\
        id INT PRIMARY KEY, \
        name TEXT NOT NULL, \
        x INT DEFAULT 1, \
        y INT REFERENCES other(id), \
        CHECK (1 = 1), \
        PRIMARY KEY (id)\
    )";
    let mut p = SqlParser::new(sql)?;
    let stmt = p.parse()?;
    let SqlStatement::CreateTable(ct) = stmt else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(ct.columns.len(), 4);
    assert!(ct.columns[0]
        .constraints
        .contains(&ColumnConstraint::PrimaryKey));
    assert!(ct.columns[1]
        .constraints
        .contains(&ColumnConstraint::NotNull));
    assert!(ct
        .constraints
        .iter()
        .any(|c| matches!(c, TableConstraint::Check(_))));
    assert!(ct.constraints.iter().any(
        |c| matches!(c, TableConstraint::PrimaryKey(cols) if cols == &vec![String::from("id")])
    ));
    Ok(())
}

#[test]
fn parses_union_all_set_operation() -> Result<()> {
    let mut p = SqlParser::new("SELECT 1 UNION ALL SELECT 2")?;
    let stmt = p.parse()?;
    let SqlStatement::SetOperation(set) = stmt else {
        panic!("expected set operation");
    };
    assert_eq!(set.op, SetOperator::Union);
    assert!(set.all);
    Ok(())
}
