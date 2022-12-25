use super::SqError;
use async_trait::async_trait;
use std::process::Command;

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

#[derive(Debug)]
struct CommandFetcher<'a>(pub(crate) &'a str);

#[async_trait]
impl<'a> Fetch for CommandFetcher<'a> {
    type Error = SqError;

    async fn fetch(&self) -> Result<FetchData, Self::Error> {
        let cmd = &self.0[6..];
        let (cmd, args) = {
            let mut parts = cmd.split_terminator('?');
            (parts.next().unwrap(), parts.next().unwrap())
        };

        Ok(FetchData {
            data: Command::new(cmd).arg(args).output().unwrap().stdout,
            hint: Some("console".to_owned()),
        })
    }
}

pub async fn fetch<S: AsRef<str>>(s: S) -> Result<FetchData, SqError> {
    let url = s.as_ref();
    match &url[0..4] {
        "http" => HttpFetcher(url).fetch().await,
        "file" => FileFetcher(url).fetch().await,
        "cmd:" => CommandFetcher(url).fetch().await,
        _ => todo!("unsupported url"),
    }
}
