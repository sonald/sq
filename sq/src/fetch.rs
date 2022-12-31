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
    async fn fetch(&self, data: &str) -> Result<FetchData, SqError>;
}

#[derive(Debug)]
struct HttpFetcher;

#[async_trait]
impl Fetch for HttpFetcher {
    async fn fetch(&self, data: &str) -> Result<FetchData, SqError> {
        let hint = if data.ends_with(".csv") {
            Some("csv".to_owned())
        } else if data.ends_with(".parquet") {
            Some("parquet".to_owned())
        } else {
            None
        };
        Ok(FetchData {
            data: reqwest::get(data).await?.bytes().await?.to_vec(),
            hint,
        })
    }
}

#[derive(Debug)]
struct FileFetcher;

#[async_trait]
impl Fetch for FileFetcher {
    async fn fetch(&self, data: &str) -> Result<FetchData, SqError> {
        let url = &data[7..];
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
struct CommandFetcher;

#[async_trait]
impl Fetch for CommandFetcher {
    async fn fetch(&self, data: &str) -> Result<FetchData, SqError> {
        let cmd = &data[6..];
        let (cmd, args) = {
            let mut parts = cmd.split_terminator('?');
            (parts.next().unwrap(), parts.next().map(|s| vec![s]).unwrap_or(vec![]))
        };

        let output = Command::new(cmd).args(args).output()?;
        Ok(FetchData {
            data: output.stdout,
            hint: Some("console".to_owned()),
        })
    }
}

pub async fn fetch<S: AsRef<str>>(s: S) -> Result<FetchData, SqError> {
    let url = s.as_ref();
    let f: Box<dyn Fetch> = match &url[0..4] {
        "http" => Box::new(HttpFetcher),
        "file" => Box::new(FileFetcher),
        "cmd:" => Box::new(CommandFetcher),
        _ => todo!("unsupported url"),
    };
    f.fetch(url).await
}
