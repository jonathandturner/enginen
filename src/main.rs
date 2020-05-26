use futures::prelude::*;
use futures::stream::StreamExt;
use smol::{blocking, iter};
use std::fmt::Display;
use std::fs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

mod table;

use indexmap::IndexMap;

use async_trait::async_trait;

type OutStream<T> = Box<dyn std::marker::Send + std::marker::Unpin + futures::Stream<Item = T>>;
type LazyGlobStream = OutStream<std::result::Result<std::path::PathBuf, glob::GlobError>>;
type Connector = Box<dyn PipelineConnector + std::marker::Send>;
type Element = Box<dyn PipelineElement + std::marker::Send>;
type PipelineError = Box<dyn std::error::Error>;

#[derive(Debug)]
pub enum Value {
    // Basic values
    String(String),
    Bool(bool),
    Nothing,

    // Compound values
    Row(IndexMap<String, Value>),
    List(Vec<Value>),
}

impl Value {
    pub fn column_names(&self) -> Vec<&String> {
        match self {
            Value::Row(row) => row.keys().collect(),
            _ => vec![],
        }
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::String(s) => write!(f, "{}", s),
            Value::Bool(b) => write!(f, "{}", b),
            Value::Row(row) => {
                let mut first = true;
                write!(f, "{{")?;
                for (k, v) in row {
                    if !first {
                        write!(f, ", ")?;
                    } else {
                        first = false;
                    }
                    write!(f, "{}: {}", k, v)?
                }
                write!(f, "}}")?;
                Ok(())
            }
            Value::List(list) => {
                write!(f, "[")?;
                let mut first = true;
                for v in list {
                    if !first {
                        write!(f, ", ")?;
                    } else {
                        first = false;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")?;
                Ok(())
            }
            Value::Nothing => Ok(()),
        }
    }
}

#[derive(Debug)]
enum Action {
    Increment,
    Greet,
}

#[derive(Debug)]
enum ReturnValue {
    Action(Action),
    Value(Value),
}

#[async_trait]
trait PipelineElement {
    async fn connect(&mut self, input: Option<Connector>) -> Result<(), PipelineError>;
    async fn next(&mut self) -> Result<Option<ReturnValue>, PipelineError>;
}

#[async_trait]
trait PipelineConnector {
    async fn connect(&mut self, input: Option<Element>) -> Result<(), PipelineError>;
    async fn next(&mut self) -> Result<Option<Value>, PipelineError>;
}

struct WhereCommand {
    input: Option<Connector>,
}

impl WhereCommand {
    fn new() -> WhereCommand {
        WhereCommand { input: None }
    }
}

#[async_trait]
impl PipelineElement for WhereCommand {
    async fn connect(&mut self, input: Option<Connector>) -> Result<(), PipelineError> {
        self.input = input;

        Ok(())
    }

    async fn next(&mut self) -> Result<Option<ReturnValue>, PipelineError> {
        if let Some(input) = &mut self.input {
            while let Some(inp) = input.next().await? {
                if let Value::Row(s) = &inp {
                    if let Some(v) = s.get("name") {
                        if let Value::String(filename) = v {
                            if !filename.contains("thirdparty") {
                                return Ok(Some(ReturnValue::Value(inp)));
                            }
                        }
                    }
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
    async fn connect(&mut self, _input: Option<Connector>) -> Result<(), PipelineError> {
        let dir = blocking!(glob::glob("**/*"))?;
        let dir = iter(dir);

        self.inner = Some(Box::new(dir));

        Ok(())
    }

    async fn next(&mut self) -> Result<Option<ReturnValue>, PipelineError> {
        if let Some(inner) = &mut self.inner {
            if let Some(res) = inner.next().await {
                let res = res?;
                let metadata = fs::metadata(&res)?;

                let mut output = IndexMap::new();
                output.insert(
                    "name".to_owned(),
                    Value::String(res.to_string_lossy().to_string()),
                );

                let filetype = if metadata.is_dir() {
                    Value::String("Dir".to_owned())
                } else if metadata.is_file() {
                    Value::String("File".to_owned())
                } else {
                    Value::Nothing
                };
                output.insert("type".to_owned(), filetype);

                return Ok(Some(ReturnValue::Value(Value::Row(output))));
            }
        }

        Ok(None)
    }
}

struct ActionRunner {
    current_shell: Option<Arc<AtomicUsize>>,
    ctrl_c: Option<piper::Receiver<()>>,
    input: Option<Element>,
}

impl ActionRunner {
    pub fn new(current_shell: Arc<AtomicUsize>, ctrl_c: piper::Receiver<()>) -> ActionRunner {
        ActionRunner {
            current_shell: Some(current_shell),
            ctrl_c: Some(ctrl_c),
            input: None,
        }
    }
}

#[async_trait]
impl PipelineConnector for ActionRunner {
    async fn connect(&mut self, input: Option<Element>) -> Result<(), PipelineError> {
        self.input = input;

        Ok(())
    }

    async fn next(&mut self) -> Result<Option<Value>, PipelineError> {
        if let Some(input) = &mut self.input {
            while let Some(res) = input.next().await? {
                if let Some(ctrl_c) = &mut self.ctrl_c {
                    if ctrl_c.try_recv().is_some() {
                        return Err(Box::new(std::io::Error::new(
                            std::io::ErrorKind::Interrupted,
                            "Ctrl-C pressed".to_string(),
                        )));
                    }
                }
                match res {
                    ReturnValue::Action(Action::Increment) => {
                        if let Some(current_shell) = &mut self.current_shell {
                            current_shell.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    ReturnValue::Action(Action::Greet) => {
                        println!("Hello world!");
                    }
                    ReturnValue::Value(value) => {
                        return Ok(Some(value));
                    }
                }
            }
        }

        Ok(None)
    }
}

fn main() -> Result<(), PipelineError> {
    let (s, ctrl_c) = piper::chan(1);
    let handle = move || {
        let _ = s.send(()).now_or_never();
    };
    ctrlc::set_handler(handle).unwrap();

    let counter = Arc::new(AtomicUsize::new(10));

    smol::run(async {
        // Build up our pipeline: ls | where name =~ thirdparty
        let mut ls = LsCommand::new();
        ls.connect(None).await?;

        let mut glue = ActionRunner::new(counter.clone(), ctrl_c.clone());
        glue.connect(Some(Box::new(ls))).await?;

        let mut where_ = WhereCommand::new();
        where_.connect(Some(Box::new(glue))).await?;

        let mut drain = ActionRunner::new(counter.clone(), ctrl_c.clone());
        drain.connect(Some(Box::new(where_))).await?;

        while let Some(res) = drain.next().await? {
            // if ctrl_c.try_recv().is_some() {
            //     break;
            // }
            println!("{}", res);
        }

        dbg!(counter);
        Ok(())
    })
}
