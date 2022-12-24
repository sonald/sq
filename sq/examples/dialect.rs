use sqlparser::*;

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

fn main() {
    let dialect = MyDialect::new();
    let sql = r"
        select c1,c2 from `http://sonald.me`
        ";
    let stmts = parser::Parser::parse_sql(&dialect, sql).unwrap();
    if let ast::Statement::Query(ref query) = stmts[0] {
        if let ast::SetExpr::Select(ref select) = query.body.as_ref() {
            println!("{:#?}", select);
            if let ast::TableFactor::Table { name, .. } = &select.from[0].relation {
                let nm = format!("{}", name);
                println!("{}", nm);
            }
        }
    }
}
