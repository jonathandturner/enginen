use futures::stream::StreamExt;
use smol::{blocking, iter};
use std::fs;

use async_trait::async_trait;

type OutStream<T> = Box<dyn std::marker::Send + std::marker::Unpin + futures::Stream<Item = T>>;
type LazyGlobStream = OutStream<std::result::Result<std::path::PathBuf, glob::GlobError>>;
type Input = Box<dyn PipelineElement + std::marker::Send>;
type PipelineError = Box<dyn std::error::Error>;

#[async_trait]
trait PipelineElement {
    async fn connect(&mut self, input: Option<Input>) -> Result<(), PipelineError>;
    async fn next(&mut self) -> Result<Option<String>, PipelineError>;
}

struct WhereCommand {
    input: Option<Input>,
}

impl WhereCommand {
    fn new() -> WhereCommand {
        WhereCommand { input: None }
    }
}

#[async_trait]
impl PipelineElement for WhereCommand {
    async fn connect(&mut self, input: Option<Input>) -> Result<(), PipelineError> {
        self.input = input;

        Ok(())
    }

    async fn next(&mut self) -> Result<Option<String>, PipelineError> {
        if let Some(input) = &mut self.input {
            while let Some(res) = input.next().await? {
                if !res.contains("thirdparty") {
                    return Ok(Some(res));
                }
            }
        }

        Ok(None)
    }
}

struct LsCommand {
    inner: Option<LazyGlobStream>,
}

impl LsCommand {
    fn new() -> LsCommand {
        LsCommand { inner: None }
    }
}

#[async_trait]
impl PipelineElement for LsCommand {
    async fn connect(&mut self, _input: Option<Input>) -> Result<(), PipelineError> {
        let dir = blocking!(glob::glob("**/*"))?;
        let dir = iter(dir);

        self.inner = Some(Box::new(dir));

        Ok(())
    }

    async fn next(&mut self) -> Result<Option<String>, PipelineError> {
        if let Some(inner) = &mut self.inner {
            if let Some(res) = inner.next().await {
                let res = res?;
                let metadata = fs::metadata(&res)?;
                let out = format!("{} {:?}", res.to_string_lossy(), metadata);
                return Ok(Some(out));
            }
        }

        Ok(None)
    }
}

fn main() -> Result<(), PipelineError> {
    smol::run(async {
        // Build up our pipeline: ls | where name =~ thirdparty
        let mut ls = LsCommand::new();
        ls.connect(None).await?;

        let mut where_ = WhereCommand::new();
        where_.connect(Some(Box::new(ls))).await?;

        while let Some(res) = where_.next().await? {
            println!("{}", res);
        }
        Ok(())
    })
}
