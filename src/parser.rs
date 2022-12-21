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
    pub(crate) source: String,
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
            op::Gt => Ok(dsl::Operator::Gt),
            op::Plus => Ok(dsl::Operator::Plus),
            op::Minus => Ok(dsl::Operator::Minus),
            op::Multiply => Ok(dsl::Operator::Multiply),
            op::Divide => Ok(dsl::Operator::Divide),
            op::Modulo => Ok(dsl::Operator::Modulus),
            op::Lt => Ok(dsl::Operator::Lt),
            op::GtEq => Ok(dsl::Operator::GtEq),
            op::LtEq => Ok(dsl::Operator::LtEq),
            op::Eq => Ok(dsl::Operator::Eq),
            op::NotEq => Ok(dsl::Operator::NotEq),
            op::And => Ok(dsl::Operator::And),
            op::Or => Ok(dsl::Operator::Or),
            op::Xor => Ok(dsl::Operator::Xor),
            _ => Err(MyError::AstError(format!("BinaryOperator {} not supported", value.0))),
        }
    }
}

impl<'a> TryFrom<SqlExpression<'a>> for dsl::Expr {
    type Error = MyError;

    fn try_from(value: SqlExpression<'a>) -> Result<Self, Self::Error> {
        match value.0 {
            ast::Expr::BinaryOp { left, op, right } => Ok(dsl::Expr::BinaryExpr { 
                left: Box::new(SqlExpression(left).try_into()?),
                op: SqlBinOp(op).try_into()?,
                right: Box::new(SqlExpression(right).try_into()?), 
            }),
            ast::Expr::Identifier(id) => Ok(dsl::Expr::Column(Arc::from(id.value.as_str()))),
            ast::Expr::Value(v) => Ok(dsl::Expr::Literal(SqlValue(v).try_into()?)),

            _ => Err(MyError::AstError(format!("SqlExpression: {} not supported", value.0))),
        }
    }
}

impl<'a> TryFrom<SqlSelectItem<'a>> for dsl::Expr {
    type Error = MyError;

    fn try_from(value: SqlSelectItem<'a>) -> Result<Self, Self::Error> {
        use ast::SelectItem::*;
        match value.0 {
            Wildcard => Ok(dsl::Expr::Wildcard),
            ExprWithAlias { expr: ast::Expr::Identifier(id), alias } => {
                Ok(dsl::Expr::Column(Arc::from(id.value.as_str())).alias(alias.value.as_str()))
            },
            //QualifiedWildcard(name) => Ok(dsl::Expr::Wildcard),
            UnnamedExpr(ast::Expr::Identifier(id)) => 
                Ok(dsl::Expr::Column(Arc::from(id.value.as_str()))),
            _ => todo!("unsupported")
        }
    }
}

impl<'a> TryFrom<SqlSelect<'a>> for Query {
    type Error = MyError;

    fn try_from(value: SqlSelect<'a>) -> Result<Self, Self::Error> {
        let query = value.0;
        println!("{:#?}", query);

        if let ast::SetExpr::Select(ref select) = query.body.as_ref() {
            let source = if let ast::TableFactor::Table { name, .. } = &select.from[0].relation {
                name.to_string()
            } else {
                "default".to_owned()
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
