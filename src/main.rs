use std::io::Write;
use std::{env, path::PathBuf};

use anyhow::Result;
use clap::{ArgGroup, Parser};
use serde_json::{Map, Value as Json, json};
use tracing::{info, warn};

use evix::{AutoArg, Config, Event, Input, WORKER_ENV};

#[derive(Parser, Clone)]
#[command(
    name = "evix",
    about = "Evaluate a Nix attrset and emit one JSON line per derivation"
)]
#[command(group(ArgGroup::new("input").required(true).args(["flake", "expr", "file"])))]
struct Args {
    /// Evaluate a flake output (e.g. `.#hydraJobs`).
    #[arg(long)]
    flake: Option<String>,

    /// Evaluate an inline Nix expression.
    #[arg(long)]
    expr: Option<String>,

    /// Evaluate a Nix file.
    #[arg(long)]
    file: Option<PathBuf>,

    /// Pass a Nix expression as an argument: `--arg NAME EXPR`.
    #[arg(long = "arg", value_names = ["NAME", "EXPR"], num_args = 2, action = clap::ArgAction::Append)]
    arg: Vec<String>,

    /// Pass a string value as an argument: `--argstr NAME VALUE`.
    #[arg(long = "argstr", value_names = ["NAME", "VALUE"], num_args = 2, action = clap::ArgAction::Append)]
    argstr: Vec<String>,

    /// Number of worker processes.
    #[arg(long, default_value_t = 1)]
    workers: usize,

    /// Memory limit per worker in MB; worker restarts when exceeded.
    #[arg(long, default_value_t = 4096)]
    max_memory_size: usize,

    /// Recurse into all attrsets, ignoring recurseForDerivations.
    #[arg(long)]
    force_recurse: bool,

    /// Directory in which to register GC roots for evaluated derivations.
    #[arg(long)]
    gc_roots_dir: Option<PathBuf>,

    /// Increase logging verbosity. Use multiple times for more detail.
    #[arg(short, long, action = clap::ArgAction::Count)]
    verbose: u8,
}

impl Args {
    fn to_config(&self) -> Config {
        let input = if let Some(flake) = &self.flake {
            Input::Flake(flake.clone())
        } else if let Some(expr) = &self.expr {
            Input::Expr(expr.clone())
        } else if let Some(file) = &self.file {
            Input::File(file.clone())
        } else {
            unreachable!("clap requires one input source")
        };

        let mut auto_args = Vec::new();
        for chunk in self.arg.chunks(2) {
            let [name, expr] = chunk else { continue };
            auto_args.push((name.clone(), AutoArg::Expr(expr.clone())));
        }
        for chunk in self.argstr.chunks(2) {
            let [name, value] = chunk else { continue };
            auto_args.push((name.clone(), AutoArg::Str(value.clone())));
        }

        Config {
            input,
            auto_args,
            force_recurse: self.force_recurse,
            gc_roots_dir: self.gc_roots_dir.clone(),
            workers: self.workers,
            max_memory_size: self.max_memory_size,
        }
    }

    fn init_tracing(&self) {
        init_tracing_subscriber(self.verbose);
    }
}

fn init_tracing_subscriber(verbose: u8) {
    let level = match verbose {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_target(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(level)),
        )
        .init();
}

fn main() -> Result<()> {
    if env::var(WORKER_ENV).is_ok() {
        init_tracing_subscriber(0);
        return evix::run_worker();
    }

    let args = Args::parse();
    args.init_tracing();

    let config = args.to_config();
    info!(workers = config.workers, "starting evix evaluation");

    evix::evaluate(&config, |event| {
        let line = format_event(event);
        writeln!(std::io::stdout().lock(), "{line}")?;

        if let Event::Derivation(d) = event
            && let Some(ref err) = d.gc_root_error
        {
            warn!(drv_path = %d.drv_path, error = %err, "failed to register gc root");
        }

        Ok(())
    })
}

fn format_event(event: &Event) -> String {
    let json = match event {
        Event::Derivation(d) => {
            let mut outputs = Map::new();
            for (k, v) in &d.outputs {
                outputs.insert(
                    k.clone(),
                    v.as_ref().map_or(Json::Null, |p| Json::String(p.clone())),
                );
            }
            json!({
                "attr": d.attr,
                "attrPath": d.attr_path,
                "name": d.name,
                "system": d.system,
                "drvPath": d.drv_path,
                "outputs": outputs,
            })
        }
        Event::AttrSet {
            attr,
            attr_path,
            attrs,
        } => json!({
            "attr": attr,
            "attrPath": attr_path,
            "attrs": attrs,
        }),
        Event::Error(e) => json!({
            "attr": e.attr,
            "attrPath": e.attr_path,
            "error": e.error,
            "fatal": e.fatal,
        }),
    };
    json.to_string()
}
