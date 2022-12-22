use std::sync::Arc;

use sqlparser::{ast, dialect, parser};
use polars::lazy::dsl;
use polars::prelude::LiteralValue;

use super::MyError;

#[derive(Debug)]
struct MyDialect {
    g: dialect::GenericDialect,
}

impl dialect::Dialect for MyDialect {
    fn is_identifier_start(&self, ch: char) -> bool {
        self.g.is_identifier_start(ch)
    }

    fn is_identifier_part(&self, ch: char) -> bool {
        if self.g.is_identifier_part(ch) {
            true
        } else {
            ":/?=_-".contains(ch)
        }
    }

    fn is_delimited_identifier_start(&self, ch: char) -> bool {
        if self.g.is_delimited_identifier_start(ch) {
            true
        } else {
            "`".contains(ch)
        }
    }
}

impl Default for MyDialect {
    fn default() -> Self {
        MyDialect::new()
    }
}

impl MyDialect {
    pub fn new() -> MyDialect {
        MyDialect {
            g: dialect::GenericDialect::default(),
        }
    }
}

// AST for sql query
#[derive(Debug)]
pub struct Query {
    pub(crate) projections: Vec<dsl::Expr>,
    pub(crate) source: Option<String>,
    pub(crate) condition: Option<dsl::Expr>,
    pub(crate) limit: Option<dsl::Expr>,
}

#[derive(Debug)]
struct SqlExpression<'a>(&'a ast::Expr);
#[derive(Debug)]
struct SqlSelectItem<'a>(&'a ast::SelectItem);
#[derive(Debug)]
struct SqlSelect<'a>(&'a ast::Query);
#[derive(Debug)]
struct SqlBinOp<'a>(&'a ast::BinaryOperator);
#[derive(Debug)]
struct SqlValue<'a>(&'a ast::Value);

impl<'a> TryFrom<SqlValue<'a>> for LiteralValue {
    type Error = MyError;

    fn try_from(value: SqlValue<'a>) -> Result<Self, Self::Error> {
        use ast::Value::*;
        match value.0 {
            Number(s, _b) => Ok(LiteralValue::Float64(s.parse()?)),
            Boolean(b) => Ok(LiteralValue::Boolean(*b)),
            SingleQuotedString(s) => Ok(LiteralValue::Utf8(s.clone())),
            _ => todo!("SqlValue: {} not supported", value.0),
        }
    }
}

impl<'a> TryFrom<SqlBinOp<'a>> for dsl::Operator {
    type Error = MyError;

    fn try_from(value: SqlBinOp<'a>) -> Result<Self, Self::Error> {
        use ast::BinaryOperator as op;
        match value.0 {
            op::Gt => Ok(Self::Gt),
            op::Plus => Ok(Self::Plus),
            op::Minus => Ok(Self::Minus),
            op::Multiply => Ok(Self::Multiply),
            op::Divide => Ok(Self::Divide),
            op::Modulo => Ok(Self::Modulus),
            op::Lt => Ok(Self::Lt),
            op::GtEq => Ok(Self::GtEq),
            op::LtEq => Ok(Self::LtEq),
            op::Eq => Ok(Self::Eq),
            op::NotEq => Ok(Self::NotEq),
            op::And => Ok(Self::And),
            op::Or => Ok(Self::Or),
            op::Xor => Ok(Self::Xor),
            _ => Err(MyError::AstError(format!("SqlBinOp {} is not supported", value.0))),
        }
    }
}

impl<'a> TryFrom<SqlExpression<'a>> for dsl::Expr {
    type Error = MyError;

    fn try_from(value: SqlExpression<'a>) -> Result<Self, Self::Error> {
        match value.0 {
            ast::Expr::BinaryOp { left, op, right } => Ok(Self::BinaryExpr { 
                left: Box::new(SqlExpression(left).try_into()?),
                op: SqlBinOp(op).try_into()?,
                right: Box::new(SqlExpression(right).try_into()?), 
            }),
            ast::Expr::Identifier(id) => Ok(Self::Column(Arc::from(id.value.as_str()))),
            ast::Expr::Value(v) => Ok(Self::Literal(SqlValue(v).try_into()?)),

            _ => Err(MyError::AstError(format!("SqlExpression: {} not supported", value.0))),
        }
    }
}

impl<'a> TryFrom<SqlSelectItem<'a>> for dsl::Expr {
    type Error = MyError;

    fn try_from(value: SqlSelectItem<'a>) -> Result<Self, Self::Error> {
        use ast::SelectItem::*;
        match value.0 {
            Wildcard => Ok(Self::Wildcard),
            ExprWithAlias { expr: ast::Expr::Identifier(id), alias } => {
                Ok(Self::Column(Arc::from(id.value.as_str())).alias(alias.value.as_str()))
            },
            UnnamedExpr(ast::Expr::Identifier(id)) => 
                Ok(Self::Column(Arc::from(id.value.as_str()))),
            UnnamedExpr(ast::Expr::Value(value)) => 
                Ok(Self::Literal(SqlValue(value).try_into()?)),
            _ => todo!("SelectItem: {} is not supported", value.0)
        }
    }
}

impl<'a> TryFrom<SqlSelect<'a>> for Query {
    type Error = MyError;

    fn try_from(value: SqlSelect<'a>) -> Result<Self, Self::Error> {
        let query = value.0;
        println!("{:#?}", query);

        if let ast::SetExpr::Select(ref select) = query.body.as_ref() {
            let source = if let Some(ast::TableWithJoins {relation: ast::TableFactor::Table { name, .. }, .. }) = &select.from.first() {
                Some(if name.0.len() > 1 {
                    name.to_string()
                } else {
                    name.0.iter().map(|id|id.value.as_str()).collect::<Vec<_>>().join("")
                })
            } else {
                None
            };

            let mut projections = vec![];
            for sel in select.projection.iter() {
                projections.push(SqlSelectItem(sel).try_into()?);
            }
            let condition = match select.selection {
                Some(ref c) => Some(SqlExpression(c).try_into()?),
                None => None,
            };

            let limit = match query.limit {
                Some(ref e) => Some(SqlExpression(e).try_into()?),
                None => None,
            };

            Ok(Query {
                projections,
                source,
                condition,
                limit,
            })
        } else {
            todo!("not implemented")
        }
    }
}


pub fn parse<S: AsRef<str>>(sql: S) -> Result<Query, MyError> {
    let dialect = MyDialect::new();
    match parser::Parser::parse_sql(&dialect, sql.as_ref())?[0] {
        ast::Statement::Query(ref query) => {
            SqlSelect(query).try_into()
        }
        _ => Err(sqlparser::parser::ParserError::ParserError("sql not supported".to_owned()).into()),
    }
}
