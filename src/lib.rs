use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Context as _;
use serde::{Deserialize, Serialize};
use tracing::debug;

mod eval;
mod master;
mod worker;

/// Environment variable used to distinguish worker subprocesses spawned by
/// [`evaluate`]. A binary that re-executes itself to host workers should check
/// this variable and call [`run_worker`] when it is set.
pub const WORKER_ENV: &str = "EVIX_WORKER";

/// Input source for a Nix evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Input {
    Flake(String),
    Expr(String),
    File(PathBuf),
}

/// Argument passed to a Nix function parameter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AutoArg {
    Expr(String),
    Str(String),
}

/// Configuration for an evaluation run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub input: Input,
    pub auto_args: Vec<(String, AutoArg)>,
    pub force_recurse: bool,
    pub gc_roots_dir: Option<PathBuf>,
    pub workers: usize,
    pub max_memory_size: usize,
}

/// A derivation emitted by evaluation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Derivation {
    pub attr: String,
    pub attr_path: Vec<String>,
    pub name: String,
    pub system: String,
    pub drv_path: String,
    pub outputs: BTreeMap<String, Option<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gc_root_error: Option<String>,
}

/// An evaluation error associated with a specific attribute path.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvalError {
    pub attr: String,
    pub attr_path: Vec<String>,
    pub error: String,
    pub fatal: bool,
}

/// Event produced while traversing a Nix expression.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Event {
    Derivation(Derivation),
    AttrSet {
        attr: String,
        attr_path: Vec<String>,
        attrs: Vec<String>,
    },
    Error(EvalError),
}

impl Event {
    /// Attribute path rendered with dots.
    pub fn attr(&self) -> &str {
        match self {
            Event::Derivation(d) => &d.attr,
            Event::AttrSet { attr, .. } => attr,
            Event::Error(e) => &e.attr,
        }
    }

    /// Attribute path as a list of names.
    pub fn attr_path(&self) -> &[String] {
        match self {
            Event::Derivation(d) => &d.attr_path,
            Event::AttrSet { attr_path, .. } => attr_path,
            Event::Error(e) => &e.attr_path,
        }
    }
}

/// Run an evaluation and deliver each event to `sink`.
///
/// The implementation uses worker subprocesses to isolate evaluation memory.
/// Each worker re-executes the current binary; the binary must call
/// [`run_worker`] when the [`WORKER_ENV`] environment variable is set.
///
/// ```no_run
/// use evix::{Config, Event, Input};
///
/// let config = Config {
///     input: Input::Expr("import <nixpkgs> {}".into()),
///     auto_args: vec![],
///     force_recurse: false,
///     gc_roots_dir: None,
///     workers: 4,
///     max_memory_size: 4096,
/// };
///
/// evix::evaluate(&config, |event| {
///     println!("{:?}", event);
///     Ok(())
/// }).unwrap();
/// ```
pub fn evaluate<F>(config: &Config, sink: F) -> anyhow::Result<()>
where
    F: FnMut(&Event) -> anyhow::Result<()> + Send + 'static,
{
    debug!("evaluating input, {} workers", config.workers);

    if let Some(dir) = &config.gc_roots_dir {
        std::fs::create_dir_all(dir).with_context(|| format!("creating gc-roots dir {dir:?}"))?;
        debug!("ensured gc-roots directory exists");
    }

    let sink = Arc::new(Mutex::new(sink));
    master::run(config, sink)
}

/// Worker entrypoint.
///
/// Reads the [`Config`] as a JSON line from stdin, then processes attribute
/// paths requested by the master process.
pub fn run_worker() -> anyhow::Result<()> {
    worker::run()
}
