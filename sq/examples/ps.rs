// dump ps data

use std::ops::Deref;

use sq::{execute, SqError};

#[tokio::main]
async fn main() -> std::result::Result<(), SqError> {
    let sql = r#"
        select * from cmd://ps?au
        order by STARTED
        "#;

    std::env::set_var("POLARS_FMT_MAX_ROWS", "-1"); // -1 force to height
    std::env::set_var("POLARS_FMT_TABLE_HIDE_COLUMN_DATA_TYPES", "1");
    match execute(sql).await {
        Ok(ds) => {
            println!("{}", ds.deref());
            Ok(())
        }
        Err(e) => {
            println!("{}", e);
            Err(e)
        }
    }
}
