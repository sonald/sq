use super::SqError;
use async_trait::async_trait;

#[derive(Debug)]
pub struct FetchData {
    pub data: Vec<u8>,
    pub hint: Option<String>,
}

#[async_trait]
pub trait Fetch {
    type Error;
    async fn fetch(&self) -> Result<FetchData, Self::Error>;
}

#[derive(Debug)]
struct HttpFetcher<'a>(pub(crate) &'a str);

#[async_trait]
impl<'a> Fetch for HttpFetcher<'a> {
    type Error = SqError;

    async fn fetch(&self) -> Result<FetchData, Self::Error> {
        let url = self.0;
        let hint = if url.ends_with(".csv") {
            Some("csv".to_owned())
        } else if url.ends_with(".parquet") {
            Some("parquet".to_owned())
        } else {
            None
        };
        Ok(FetchData {
            data: reqwest::get(url).await?.bytes().await?.to_vec(),
            hint,
        })
    }
}

#[derive(Debug)]
struct FileFetcher<'a>(pub(crate) &'a str);

#[async_trait]
impl<'a> Fetch for FileFetcher<'a> {
    type Error = SqError;

    async fn fetch(&self) -> Result<FetchData, Self::Error> {
        let url = &self.0[7..];
        let hint = if url.ends_with(".csv") {
            Some("csv".to_owned())
        } else if url.ends_with(".parquet") {
            Some("parquet".to_owned())
        } else {
            None
        };
        Ok(FetchData {
            data: std::fs::read(url)?,
            hint,
        })
    }
}

pub async fn fetch<S: AsRef<str>>(s: S) -> Result<FetchData, SqError> {
    let url = s.as_ref();
    match &url[0..4] {
        "http" => HttpFetcher(url).fetch().await,
        "file" => FileFetcher(url).fetch().await,
        _ => todo!("unsupported url"),
    }
}
