use std::ops::Deref;

use sq::execute;

#[tokio::main]
async fn main() -> Result<(), sq::MyError> {
    let sql = r#"
        select iso_code, continent, total_cases 
        from file:///home/sonald/work/sq/covid.parquet
        where total_cases > 200000.0 and continent = 'Africa'
        limit 8
        "#;
    let sql2 = r#"
        select iso_code, continent, total_cases 
        from https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/latest/owid-covid-latest.csv
        where total_cases > 200000.0 and continent = 'Africa'
        limit 5
        "#;
    match execute(sql).await {
        Ok(ds) => {
            println!("{}", ds.deref());
        }
        Err(e) => println!("{}", e),
    }

    Ok(())
}
