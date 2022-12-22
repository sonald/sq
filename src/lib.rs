use polars::prelude::*;
use std::io::Cursor;
use std::ops::{Deref, DerefMut};

pub mod fetch;
pub mod parser;

#[derive(Debug, thiserror::Error)]
pub enum MyError {
    #[error("parse: {0}")]
    ParseError(#[from] sqlparser::parser::ParserError),
    #[error("request: {0}")]
    ReqError(#[from] reqwest::Error),
    #[error("io: {0}")]
    IoError(#[from] std::io::Error),
    #[error("polars: {0}")]
    PolarsError(#[from] PolarsError),
    #[error("load: {0}")]
    LoadError(String),
    #[error("ast: {0}")]
    AstError(String),
    #[error("convert: {0}")]
    ConvertError(#[from] std::num::ParseIntError),
    #[error("convert: {0}")]
    ConvertError2(#[from] std::num::ParseFloatError),
}

use fetch::*;
use parser::*;

#[derive(Debug)]
pub struct DataSet(DataFrame);

impl Deref for DataSet {
    type Target = DataFrame;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for DataSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

pub trait Loader {
    type Error;
    fn load(&self) -> Result<DataSet, Self::Error>;
}

#[derive(Debug)]
struct CsvLoader<'a>(&'a Vec<u8>);

impl<'a> Loader for CsvLoader<'a> {
    type Error = MyError;

    fn load(&self) -> Result<DataSet, Self::Error> {
        let df = CsvReader::new(Cursor::new(self.0))
            .infer_schema(Some(10))
            .finish()?;
        Ok(DataSet(df))
    }
}

#[derive(Debug)]
struct ParquetLoader<'a>(&'a Vec<u8>);

impl<'a> Loader for ParquetLoader<'a> {
    type Error = MyError;

    fn load(&self) -> Result<DataSet, Self::Error> {
        let df = ParquetReader::new(Cursor::new(self.0))
            .read_parallel(ParallelStrategy::Columns)
            .finish()?;
        Ok(DataSet(df))
    }
}

#[derive(Debug)]
struct GuessLoader<'a>(&'a Vec<u8>);

impl<'a> Loader for GuessLoader<'a> {
    type Error = MyError;

    fn load(&self) -> Result<DataSet, Self::Error> {
        Err(MyError::LoadError("Guess content failed".to_owned()))
    }
}

fn load(data: &FetchData) -> Result<DataSet, MyError> {
    match data.hint.as_ref().map(|s| s.as_ref()).unwrap_or("") {
        "csv" => CsvLoader(&data.data).load(),
        "parquet" => ParquetLoader(&data.data).load(),
        _ => GuessLoader(&data.data).load(),
    }
}

pub async fn execute<S: AsRef<str>>(sql: S) -> Result<DataSet, MyError> {
    let Query {
        projections,
        source,
        condition,
        limit,
    } = parse(sql)?;

    match source {
        Some(source) => {
            println!("source: [{}]", source);
            let data = fetch(&source).await?;
            let ds = load(&data)?;

            let ds = {
                let ds = ds.0.lazy().select(projections);
                let ds = if condition.is_some() {
                    ds.filter(condition.unwrap())
                } else {
                    ds
                };
                if let Some(Expr::Literal(LiteralValue::Float64(f))) = limit {
                    ds.limit(f as u32)
                } else {
                    ds
                }
            };

            Ok(DataSet(ds.collect()?))
        }
        _ => {
            println!("no source");
            let df = projections
                .into_iter()
                .fold(DataFrame::default().lazy(), |lf, e| {
                    let nm = e.to_string();
                    lf.with_column(e.alias(&nm))
                })
                .collect()?;

            Ok(DataSet(df))
        }
    }
}
