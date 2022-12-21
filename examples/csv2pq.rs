// csv 2 parquet

use std::{fs::OpenOptions, ops::Deref};

use polars::prelude::ParquetWriter;
use sq::{execute, MyError};

#[tokio::main]
async fn main() -> std::result::Result<(), MyError> {
    let sql = r#"
        select continent, "location", "total_cases", "new_cases", "total_deaths"
        from https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv
        where total_cases > 200000.0 and continent = 'Africa'
        limit 10
        "#;
    match execute(sql).await {
        Ok(mut ds) => {
            println!("{}", ds.deref());
            ParquetWriter::new(OpenOptions::new().truncate(true).write(true).create(true).open("covid.parquet")?)
                .finish(&mut ds)?;
            }
        Err(e) => println!("{}", e),
    }


    Ok(())
}
