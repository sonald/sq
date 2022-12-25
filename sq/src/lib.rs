use polars::prelude::*;
use std::io::Cursor;
use std::ops::{Deref, DerefMut};
use lazy_static::lazy_static;

pub mod fetch;
pub mod parser;

#[derive(Debug, thiserror::Error)]
pub enum SqError {
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
    #[error("from_utf8: {0}")]
    FromUtf8Error(#[from] std::string::FromUtf8Error),
    #[error("utf8: {0}")]
    Utf8Error(#[from] std::str::Utf8Error),
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

impl DataSet {
    pub fn to_csv(&mut self) -> Result<String, SqError> {
        let mut buf = vec![];
        CsvWriter::new(&mut buf).finish(self)?;
        Ok(String::from_utf8(buf)?)
    }

    pub fn to_parquet(&mut self) -> Result<Vec<u8>, SqError> {
        let mut buf = vec![];
        ParquetWriter::new(&mut buf).finish(self)?;
        Ok(buf)
    }
}

pub trait Loader {
    type Error;
    fn load(&self) -> Result<DataSet, Self::Error>;
}

#[derive(Debug)]
struct CsvLoader<'a>(&'a Vec<u8>);

impl<'a> Loader for CsvLoader<'a> {
    type Error = SqError;

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
    type Error = SqError;

    fn load(&self) -> Result<DataSet, Self::Error> {
        let df = ParquetReader::new(Cursor::new(self.0))
            .read_parallel(ParallelStrategy::Columns)
            .finish()?;
        Ok(DataSet(df))
    }
}

#[derive(Debug)]
struct CommandLoader<'a>(&'a Vec<u8>);
impl <'a> Loader for CommandLoader<'a> {
    type Error = SqError;

    fn load(&self) -> Result<DataSet, Self::Error> {
        lazy_static! {
            static ref PAT: regex::Regex = regex::Regex::new(r"\p{White_Space}+").unwrap();
        }

        let mut rows = vec![];

        let mut s = std::str::from_utf8(self.0.as_slice())?.lines();
        let schema = s.next().unwrap().split_ascii_whitespace().collect::<Vec<_>>();

        for ln in s {
            let r = ln.trim().splitn(schema.len(), &*PAT).map(|s| AnyValue::Utf8(s.trim())).collect();
            rows.push(row::Row::new(r));
        }

        let schema = schema.iter().map(|n| Ok(Field::new(n, DataType::Utf8))).collect::<Vec<_>>();
        let df = DataFrame::from_rows_and_schema(&rows, &Schema::try_from_fallible(schema)?)?;
        Ok(DataSet(df))
    }
}

#[derive(Debug)]
struct GuessLoader<'a>(&'a Vec<u8>);

impl<'a> Loader for GuessLoader<'a> {
    type Error = SqError;

    fn load(&self) -> Result<DataSet, Self::Error> {
        Err(SqError::LoadError("Guess content failed".to_owned()))
    }
}

fn load(data: &FetchData) -> Result<DataSet, SqError> {
    match data.hint.as_ref().map(|s| s.as_ref()).unwrap_or("") {
        "csv" => CsvLoader(&data.data).load(),
        "parquet" => ParquetLoader(&data.data).load(),
        "console" => CommandLoader(&data.data).load(),
        _ => GuessLoader(&data.data).load(),
    }
}

pub async fn execute<S: AsRef<str>>(sql: S) -> Result<DataSet, SqError> {
    let Query {
        projections,
        source,
        condition,
        limit,
        offset,
        order_by
    } = parse(sql)?;

    match source {
        Some(source) => {
            println!("source: [{}]", source);
            let data = fetch(&source).await?;

            let ds = {
                let ds = load(&data)?.0.lazy().select(projections);
                let ds = if condition.is_some() {
                    ds.filter(condition.unwrap())
                } else {
                    ds
                };
                let ds = if order_by.len() > 0 {
                    let (by, asc): (Vec<_>, Vec<_>) = order_by.into_iter().unzip();
                    ds.sort_by_exprs(by, asc, false)
                } else {
                    ds
                };
                if offset.is_some() || limit.is_some() {
                    ds.slice(offset.unwrap_or(0), limit.unwrap_or(usize::MAX) as u32)
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
