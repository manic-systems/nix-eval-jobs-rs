use std::path::PathBuf;

use anyhow::Result;
use pound::Parse;

#[derive(Parse)]
#[pound(name = "evixd", version = "0.3.3")]
struct Args {
  /// Unix socket path.
  #[pound(long)]
  socket: Option<PathBuf>,

  /// Keep the daemon in the foreground.
  #[pound(long)]
  foreground: bool,
}

fn main() -> Result<()> {
  let args = Args::parse();
  evix_daemon::run(evix_daemon::socket_path(args.socket), args.foreground)
}
