use std::sync::Arc;

use sqlparser::{ast, dialect, parser};
use polars::lazy::dsl;
use polars::prelude::LiteralValue;

use super::SqError;

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
    pub(crate) limit: Option<usize>,
    pub(crate) offset: Option<i64>,
    pub(crate) order_by: Vec<(dsl::Expr, bool)>,
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
#[derive(Debug)]
struct SqlLimit<'a>(&'a ast::Expr);
#[derive(Debug)]
struct SqlOffset<'a>(&'a ast::Offset);
#[derive(Debug)]
struct SqlOrderBy<'a>(&'a ast::OrderByExpr);

impl<'a> TryFrom<SqlValue<'a>> for LiteralValue {
    type Error = SqError;

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
    type Error = SqError;

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
            _ => Err(SqError::AstError(format!("SqlBinOp {} is not supported", value.0))),
        }
    }
}

impl<'a> TryFrom<SqlOffset<'a>> for i64 {
    type Error = SqError;

    fn try_from(value: SqlOffset<'a>) -> Result<Self, Self::Error> {
        match value.0 {
            ast::Offset {value: ast::Expr::Value(ast::Value::Number(s, _b)), ..} => Ok(s.parse()?),
            _ => Err(SqError::AstError(format!("SqlOffset {} is invalid", value.0))),
        }
    }
}

impl<'a> TryFrom<SqlLimit<'a>> for usize {
    type Error = SqError;

    fn try_from(value: SqlLimit<'a>) -> Result<Self, Self::Error> {
        match value.0 {
            ast::Expr::Value(ast::Value::Number(s, _b)) => Ok(s.parse()?),
            _ => Err(SqError::AstError(format!("SqlLimit {} invalid", value.0))),
        }
    }
}

impl<'a> TryFrom<SqlOrderBy<'a>> for (dsl::Expr, bool) {
    type Error = SqError;

    fn try_from(value: SqlOrderBy<'a>) -> Result<Self, Self::Error> {
        let ast::OrderByExpr { expr, asc, .. } = value.0;
        Ok((SqlExpression(expr).try_into()?, !asc.unwrap_or(true)))
    }
}

impl<'a> TryFrom<SqlExpression<'a>> for dsl::Expr {
    type Error = SqError;

    fn try_from(value: SqlExpression<'a>) -> Result<Self, Self::Error> {
        match value.0 {
            ast::Expr::BinaryOp { left, op, right } => Ok(Self::BinaryExpr { 
                left: Box::new(SqlExpression(left).try_into()?),
                op: SqlBinOp(op).try_into()?,
                right: Box::new(SqlExpression(right).try_into()?), 
            }),
            ast::Expr::Identifier(id) => Ok(Self::Column(Arc::from(id.value.as_str()))),
            ast::Expr::Value(v) => Ok(Self::Literal(SqlValue(v).try_into()?)),

            _ => Err(SqError::AstError(format!("SqlExpression: {} not supported", value.0))),
        }
    }
}

impl<'a> TryFrom<SqlSelectItem<'a>> for dsl::Expr {
    type Error = SqError;

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
    type Error = SqError;

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
                Some(ref e) => Some(SqlLimit(e).try_into()?),
                None => None,
            };
            let offset = match query.offset {
                Some(ref e) => Some(SqlOffset(e).try_into()?),
                None => None,
            };
            let mut order_by = vec![];
            for e in query.order_by.iter() {
                order_by.push(SqlOrderBy(e).try_into()?);
            }


            Ok(Query {
                projections,
                source,
                condition,
                limit,
                offset,
                order_by,
            })
        } else {
            todo!("not implemented")
        }
    }
}


pub fn parse<S: AsRef<str>>(sql: S) -> Result<Query, SqError> {
    let dialect = MyDialect::new();
    match parser::Parser::parse_sql(&dialect, sql.as_ref())?[0] {
        ast::Statement::Query(ref query) => {
            SqlSelect(query).try_into()
        }
        _ => Err(sqlparser::parser::ParserError::ParserError("sql not supported".to_owned()).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::lazy::dsl;
    use polars::prelude::*;

    #[test]
    fn test_parse_1() {
        let sql = r#" select 'welcome', 'to', 'sq' "#;
        let res = parse(sql);
        assert!(res.is_ok());

        let q = res.unwrap();
        assert_eq!(q.source, None); 
        assert_eq!(q.limit, None);
        assert_eq!(q.condition, None);
        assert_eq!(q.projections.len(), 3);
        assert_eq!(q.projections[0], dsl::Expr::Literal(LiteralValue::Utf8("welcome".to_owned())));
        assert_eq!(q.projections[1], dsl::Expr::Literal(LiteralValue::Utf8("to".to_owned())));
        assert_eq!(q.projections[2], dsl::Expr::Literal(LiteralValue::Utf8("sq".to_owned())));
    }

    #[test]
    fn test_parse_2() {
        let url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv";
        let sql = format!(r#"
            select continent, "location", "total_cases", "new_cases", "total_deaths"
            from {}
            where total_cases > 200000.0 and continent = 'Africa'
            limit 10
            offset 5
            "#, url);
        let res = parse(sql);
        assert!(res.is_ok());

        let q = res.unwrap();
        assert_eq!(q.source, Some(url.to_owned())); 
        assert_eq!(q.limit, Some(10));
        match q.condition {
            Some(dsl::Expr::BinaryExpr { left, op: dsl::Operator::And, right }) => {
                match left.as_ref() {
                    dsl::Expr::BinaryExpr { op: dsl::Operator::Gt, .. } => assert!(true),
                    _ => assert!(false, "left condition is wrong"),
                }
                match right.as_ref() {
                    dsl::Expr::BinaryExpr { op: dsl::Operator::Eq, .. } => assert!(true),
                    _ => assert!(false, "right condition is wrong"),
                }
                assert!(true)
            },
            _ => assert!(false),
        }
        assert_eq!(q.projections.len(), 5);
        for (i,nm) in ["continent", "location", "total_cases", "new_cases", "total_deaths"].iter().enumerate() {
            assert_eq!(q.projections[i], col(nm));
        }
    }

}
