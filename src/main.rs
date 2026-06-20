use std::io::Write;
use std::path::PathBuf;
use std::{env, process};

use anyhow::Result;
use serde_json::{Map, Value as Json, json};
use tracing::{info, warn};

use evix::{AutoArg, Config, Event, Input, WORKER_ENV};
use pound::Parse;

/// Evaluate a Nix attrset and emit one JSON line per derivation.
#[derive(Parse, Clone)]
#[pound(name = "evix", required_group = "input")]
struct Args {
    /// Evaluate a flake output (e.g. `.#hydraJobs`).
    #[pound(long, group = "input")]
    flake: Option<String>,

    /// Evaluate an inline Nix expression.
    #[pound(long, group = "input")]
    expr: Option<String>,

    /// Evaluate a Nix file.
    #[pound(long, group = "input")]
    file: Option<PathBuf>,

    /// Pass a Nix expression as an argument (`--arg NAME --arg EXPR` pairs).
    #[pound(long)]
    arg: Vec<String>,

    /// Pass a string value as an argument (`--argstr NAME --argstr VALUE` pairs).
    #[pound(long)]
    argstr: Vec<String>,

    /// Number of worker processes.
    #[pound(long, default = "1")]
    workers: usize,

    /// Memory limit per worker in MB; worker restarts when exceeded.
    #[pound(long, default = "4096")]
    max_memory_size: usize,

    /// Recurse into all attrsets, ignoring recurseForDerivations.
    #[pound(long)]
    force_recurse: bool,

    /// Directory in which to register GC roots for evaluated derivations.
    #[pound(long)]
    gc_roots_dir: Option<PathBuf>,

    /// Increase logging verbosity. Use multiple times for more detail.
    #[pound(short, long, count)]
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
            unreachable!("pound requires one of --flake, --expr, --file")
        };

        let mut auto_args = Vec::new();
        for chunk in self.arg.chunks(2) {
            let [name, expr] = chunk else {
                eprintln!("--arg requires paired NAME EXPR values: use --arg NAME --arg EXPR");
                process::exit(2);
            };
            auto_args.push((name.clone(), AutoArg::Expr(expr.clone())));
        }
        for chunk in self.argstr.chunks(2) {
            let [name, value] = chunk else {
                eprintln!(
                    "--argstr requires paired NAME VALUE values: use --argstr NAME --argstr VALUE"
                );
                process::exit(2);
            };
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
