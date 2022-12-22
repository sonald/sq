use std::ops::Deref;

use sq::execute;

#[tokio::main]
async fn main() -> Result<(), sq::MyError> {
    let sql = if let Some(sql) = std::env::args().nth(1) {
        sql
    } else {
        r#" select 'welcome', 'to', 'sq' "# .to_owned()
    };

    match execute(&sql).await {
        Ok(ds) => {
            println!("{}", ds.deref());
        }
        Err(e) => println!("{}", e),
    }

    Ok(())
}
