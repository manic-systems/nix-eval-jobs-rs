use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};
use evix::{AutoArg, Config, Filter, Input, Remote};
use pound::Parse;

macro_rules! cli_enum {
  ($($name:ident, $doc:literal, { $($extra:tt)* };)+) => {
    #[derive(Parse)]
    #[pound(name = "evix", version = "0.3.3")]
    enum Cli {
      $(
        #[doc = $doc]
        $name {
          #[pound(long, group = "input")]
          flake:           Option<String>,
          #[pound(long, group = "input")]
          expr:            Option<String>,
          #[pound(long, group = "input")]
          file:            Option<PathBuf>,
          #[pound(long)]
          arg:             Vec<String>,
          #[pound(long)]
          argstr:          Vec<String>,
          #[pound(long)]
          override_input:  Vec<String>,
          #[pound(long)]
          option:          Vec<String>,
          #[pound(long)]
          remote:          Vec<String>,
          #[pound(long)]
          meta:            bool,
          #[pound(long)]
          show_input_drvs: bool,
          #[pound(long, default = "1")]
          workers:         usize,
          #[pound(long, default = "4096", alias = "max-memory-size")]
          max_memory:      usize,
          #[pound(long)]
          force_recurse:   bool,
          #[pound(long)]
          gc_roots_dir:    Option<PathBuf>,
          $($extra)*
          #[pound(short, long, count)]
          verbose:         u8,
        },
      )+

      /// Start the evix daemon.
      Daemon {
        #[pound(long)]
        socket:     Option<PathBuf>,
        #[pound(long)]
        foreground: bool,
        #[pound(short, long, count)]
        verbose:    u8,
      },
    }
  };
}

cli_enum! {
  Eval, "Evaluate and stream derivations as NDJSON.", {
    #[pound(long)]
    socket:          Option<PathBuf>,
    #[pound(long, hidden)]
    no_daemon:       bool,
  };
  Watch, "Watch inputs and stream diffs as NDJSON.", {
    #[pound(long)]
    socket:          Option<PathBuf>,
    #[pound(long, hidden)]
    no_daemon:       bool,
  };
  Query, "Query warm daemon state.", {
    #[pound(long)]
    system:          Vec<String>,
    #[pound(long)]
    attr_prefix:     Vec<String>,
    #[pound(long)]
    socket:          Option<PathBuf>,
  };
  Diff, "Re-evaluate once and print a diff.", {
    #[pound(long)]
    socket:          Option<PathBuf>,
  };
}

struct EvalArgs {
  flake:           Option<String>,
  expr:            Option<String>,
  file:            Option<PathBuf>,
  arg:             Vec<String>,
  argstr:          Vec<String>,
  override_input:  Vec<String>,
  option:          Vec<String>,
  remote:          Vec<String>,
  meta:            bool,
  show_input_drvs: bool,
  workers:         usize,
  max_memory:      usize,
  force_recurse:   bool,
  gc_roots_dir:    Option<PathBuf>,
}

pub enum CommandPlan {
  Eval {
    config:     Config,
    socket:     Option<PathBuf>,
    use_daemon: bool,
  },
  Watch {
    config:     Config,
    socket:     Option<PathBuf>,
    use_daemon: bool,
  },
  Query {
    config: Config,
    filter: Filter,
    socket: Option<PathBuf>,
  },
  Diff {
    config: Config,
    socket: Option<PathBuf>,
  },
  Daemon {
    socket:     Option<PathBuf>,
    foreground: bool,
  },
}

pub fn parse_plan() -> Result<(u8, CommandPlan)> {
  command_plan(Cli::parse())
}

fn command_plan(cli: Cli) -> Result<(u8, CommandPlan)> {
  match cli {
    Cli::Eval {
      flake,
      expr,
      file,
      arg,
      argstr,
      override_input,
      option,
      remote,
      meta,
      show_input_drvs,
      workers,
      max_memory,
      force_recurse,
      gc_roots_dir,
      socket,
      no_daemon,
      verbose,
    } => {
      Ok((verbose, CommandPlan::Eval {
        config: eval_config(EvalArgs {
          flake,
          expr,
          file,
          arg,
          argstr,
          override_input,
          option,
          remote,
          meta,
          show_input_drvs,
          workers,
          max_memory,
          force_recurse,
          gc_roots_dir,
        })?,
        socket,
        use_daemon: !no_daemon,
      }))
    },
    Cli::Watch {
      flake,
      expr,
      file,
      arg,
      argstr,
      override_input,
      option,
      remote,
      meta,
      show_input_drvs,
      workers,
      max_memory,
      force_recurse,
      gc_roots_dir,
      socket,
      no_daemon,
      verbose,
    } => {
      let config = eval_config(EvalArgs {
        flake,
        expr,
        file,
        arg,
        argstr,
        override_input,
        option,
        remote,
        meta,
        show_input_drvs,
        workers,
        max_memory,
        force_recurse,
        gc_roots_dir,
      })?;
      Ok((verbose, CommandPlan::Watch {
        config: Config {
          watch: true,
          ..config
        },
        socket,
        use_daemon: !no_daemon,
      }))
    },
    Cli::Query {
      flake,
      expr,
      file,
      arg,
      argstr,
      override_input,
      option,
      remote,
      meta,
      show_input_drvs,
      workers,
      max_memory,
      force_recurse,
      gc_roots_dir,
      system,
      attr_prefix,
      socket,
      verbose,
    } => {
      Ok((verbose, CommandPlan::Query {
        config: eval_config(EvalArgs {
          flake,
          expr,
          file,
          arg,
          argstr,
          override_input,
          option,
          remote,
          meta,
          show_input_drvs,
          workers,
          max_memory,
          force_recurse,
          gc_roots_dir,
        })?,
        filter: filter(system, attr_prefix),
        socket,
      }))
    },
    Cli::Diff {
      flake,
      expr,
      file,
      arg,
      argstr,
      override_input,
      option,
      remote,
      meta,
      show_input_drvs,
      workers,
      max_memory,
      force_recurse,
      gc_roots_dir,
      socket,
      verbose,
    } => {
      Ok((verbose, CommandPlan::Diff {
        config: eval_config(EvalArgs {
          flake,
          expr,
          file,
          arg,
          argstr,
          override_input,
          option,
          remote,
          meta,
          show_input_drvs,
          workers,
          max_memory,
          force_recurse,
          gc_roots_dir,
        })?,
        socket,
      }))
    },
    Cli::Daemon {
      socket,
      foreground,
      verbose,
    } => Ok((verbose, CommandPlan::Daemon { socket, foreground })),
  }
}

fn eval_config(args: EvalArgs) -> Result<Config> {
  let input = match (args.flake, args.expr, args.file) {
    (Some(flake), None, None) => Input::Flake(flake),
    (None, Some(expr), None) => Input::Expr(expr),
    (None, None, Some(file)) => Input::File(file),
    _ => bail!("exactly one of --flake, --expr, or --file is required"),
  };

  Ok(Config {
    input,
    auto_args: parse_auto_args(args.arg, args.argstr)?,
    force_recurse: args.force_recurse,
    gc_roots_dir: args.gc_roots_dir,
    workers: args.workers,
    max_memory_size: args.max_memory,
    meta: args.meta,
    show_input_drvs: args.show_input_drvs,
    override_inputs: parse_pairs(args.override_input, "--override-input")?,
    nix_options: parse_pairs(args.option, "--option")?,
    watch: false,
    remotes: parse_remotes(args.remote)?,
  })
}

fn parse_auto_args(
  expr_args: Vec<String>,
  str_args: Vec<String>,
) -> Result<Vec<(String, AutoArg)>> {
  let mut out = Vec::new();
  for (name, value) in parse_pairs(expr_args, "--arg")? {
    out.push((name, AutoArg::Expr(value)));
  }
  for (name, value) in parse_pairs(str_args, "--argstr")? {
    out.push((name, AutoArg::Str(value)));
  }
  Ok(out)
}

fn parse_pairs(
  values: Vec<String>,
  flag: &str,
) -> Result<Vec<(String, String)>> {
  let mut chunks = values.chunks_exact(2);
  let pairs = chunks
    .by_ref()
    .map(|chunk| (chunk[0].clone(), chunk[1].clone()))
    .collect();
  if !chunks.remainder().is_empty() {
    bail!("{flag} requires paired NAME VALUE entries");
  }
  Ok(pairs)
}

fn parse_remotes(values: Vec<String>) -> Result<Vec<Remote>> {
  let mut chunks = values.chunks_exact(3);
  let mut remotes = Vec::new();
  for chunk in chunks.by_ref() {
    let workers = chunk[2].parse::<usize>().with_context(|| {
      format!("parsing --remote worker count {:?}", chunk[2])
    })?;
    if workers == 0 {
      bail!("--remote worker count must be greater than zero");
    }
    remotes.push(Remote {
      host: chunk[0].clone(),
      systems: chunk[1]
        .split(',')
        .filter(|system| !system.is_empty())
        .map(str::to_owned)
        .collect(),
      workers,
    });
  }
  if !chunks.remainder().is_empty() {
    bail!("--remote requires HOST SYSTEM[,SYSTEM] WORKERS entries");
  }
  Ok(remotes)
}

fn filter(systems: Vec<String>, prefixes: Vec<String>) -> Filter {
  Filter {
    systems:     (!systems.is_empty()).then_some(systems),
    attr_prefix: (!prefixes.is_empty()).then(|| {
      prefixes
        .into_iter()
        .flat_map(|prefix| {
          prefix
            .split('.')
            .filter(|part| !part.is_empty())
            .map(str::to_owned)
            .collect::<Vec<_>>()
        })
        .collect()
    }),
  }
}
