use polars::prelude::*;

pub fn main() {
    let db = std::env::args().nth(1).unwrap_or("./*.parquet".to_owned());
    if let Ok(lf1) = LazyFrame::scan_parquet(&db, ScanArgsParquet::default()) {
        let df = lf1.select(&[col("*")]).collect().unwrap();
        println!("size: {}", df.height());
        println!("{}", df.head(Some(4)));
    }
}
