// csv 2 parquet

use std::{fs::OpenOptions, ops::Deref};

use polars::prelude::ParquetWriter;
use sq::{execute, SqError};

#[tokio::main]
async fn main() -> std::result::Result<(), SqError> {
    let sql = r#"
        select iso_code, continent, "location", "total_cases", "new_cases", "total_deaths", last_updated_date
        from https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv
        where total_cases > 200000.0
        order by new_cases, continent and total_deaths > 0
        limit 100
        offset 2
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
