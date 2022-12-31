use std::ops::Deref;

use sq::execute;

#[tokio::main]
async fn main() -> Result<(), sq::SqError> {
    let sql = if let Some(sql) = std::env::args().nth(1) {
        sql
    } else {
        r#" select 'welcome', 'to', 'sq' "#.to_owned()
    };

    std::env::set_var("POLARS_FMT_MAX_ROWS", "-1"); // -1 force to height
    std::env::set_var("POLARS_FMT_TABLE_HIDE_COLUMN_DATA_TYPES", "1");
    match execute(&sql).await {
        Ok(ds) => {
            println!("{}", ds.deref());
        }
        Err(e) => println!("{}", e),
    }

    Ok(())
}
