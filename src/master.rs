use std::{
  env,
  io,
  io::{BufRead, BufReader, Read, Write},
  process::{Child, ChildStdin, ChildStdout, Command, Stdio},
  sync::{
    Arc,
    Condvar,
    Mutex,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  time::Duration,
};

use anyhow::{Context as _, Result, bail};
use tracing::{debug, error, info, trace};

use crate::{Config, EvalError, Event, WORKER_ENV};

/// How often a collector parked waiting for work re-checks the cancellation
/// flag.
const CANCEL_POLL_INTERVAL: Duration = Duration::from_millis(200);
const STDERR_CAPTURE_LIMIT: usize = 16 * 1024;

struct WorkerProcess {
  proc:   Child,
  stdin:  ChildStdin,
  stdout: BufReader<ChildStdout>,
  stderr: WorkerStderr,
}

#[derive(Clone)]
struct WorkerStderr {
  bytes: Arc<Mutex<Vec<u8>>>,
}

impl WorkerStderr {
  fn capture(stderr: impl Read + Send + 'static) -> Self {
    let bytes = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&bytes);
    thread::spawn(move || capture_stderr(stderr, sink));
    Self { bytes }
  }

  fn snapshot(&self) -> String {
    let bytes = self.bytes.lock().unwrap();
    String::from_utf8_lossy(&bytes).trim().to_string()
  }
}

struct SharedState {
  todo:   Vec<Vec<String>>,
  active: usize,
  error:  Option<String>,
}

/// Spawn worker threads, each running a collector loop, and block until all
/// work is drained or a fatal error occurs.
///
/// The shared work-queue is seeded with the empty path `[]` (the root of the
/// expression). As attrsets are expanded their children are pushed back onto
/// the queue and dispatched to idle workers.
pub fn run<F>(
  config: &Config,
  cancel: &Arc<AtomicBool>,
  sink: Arc<Mutex<F>>,
) -> Result<()>
where
  F: FnMut(&Event) -> Result<()> + Send + 'static,
{
  let shared = Arc::new((
    Mutex::new(SharedState {
      todo:   vec![vec![]],
      active: 0,
      error:  None,
    }),
    Condvar::new(),
  ));

  let n = config.workers.max(1);
  let mut handles = Vec::with_capacity(n);
  for _ in 0..n {
    let shared = Arc::clone(&shared);
    let sink = Arc::clone(&sink);
    let cancel = Arc::clone(cancel);
    let config = config.clone();
    handles.push(thread::spawn(move || {
      collector(&config, &cancel, shared, sink)
    }));
  }

  for h in handles {
    h.join()
      .map_err(|_| anyhow::anyhow!("collector thread panicked"))??;
  }

  let (lock, _) = &*shared;
  if let Some(e) = &lock.lock().unwrap().error {
    bail!("{e}");
  }

  Ok(())
}

/// One collector thread: owns a worker child process and feeds it attribute
/// paths drawn from the shared queue. Incoming [`Event`]s are handed to `sink`;
/// attrset expansions push new paths back onto the queue.
///
/// The collector parks when the queue is empty (but work is still in-flight)
/// and periodically wakes to check the cancellation flag. On cancellation or
/// empty-queue, it sends `exit` to the worker, waits for the process, and
/// returns.
fn collector<F>(
  config: &Config,
  cancel: &Arc<AtomicBool>,
  shared: Arc<(Mutex<SharedState>, Condvar)>,
  sink: Arc<Mutex<F>>,
) -> Result<()>
where
  F: FnMut(&Event) -> Result<()>,
{
  let (lock, cvar) = &*shared;

  let mut worker = spawn_worker_pair(config)?;

  loop {
    let path = {
      let mut s = lock.lock().unwrap();
      loop {
        if cancel.load(Ordering::Relaxed) {
          writeln!(worker.stdin, "exit")?;
          worker.stdin.flush()?;
          let _ = worker.proc.wait();
          info!("cancellation requested, collector exiting");
          return Ok(());
        }
        if let Some(ref e) = s.error {
          let msg = e.clone();
          writeln!(worker.stdin, "exit")?;
          worker.stdin.flush()?;
          error!(error = %msg, "stopping collector due to fatal error");
          bail!("{msg}");
        }
        if !s.todo.is_empty() {
          let p = s.todo.remove(0);
          s.active += 1;
          debug!(attr = %p.join("."), active = s.active, pending = s.todo.len(), "dispatched attribute");
          break Some(p);
        }
        if s.active == 0 {
          writeln!(worker.stdin, "exit")?;
          worker.stdin.flush()?;
          let _ = worker.proc.wait();
          info!("evaluation queue empty, exiting worker");
          return Ok(());
        }
        // Park until new work arrives, but wake periodically so a
        // cancellation request is observed even when no events flow.
        let (guard, _timeout) =
          cvar.wait_timeout(s, CANCEL_POLL_INTERVAL).unwrap();
        s = guard;
      }
    };

    let Some(path) = path else {
      continue;
    };

    let attr = path.join(".");
    trace!(attr = %attr, "sending work to worker");
    writeln!(worker.stdin, "do {}", serde_json::to_string(&path)?)?;
    worker.stdin.flush()?;

    let event = read_event(&mut worker, &path)?;

    {
      let mut s = lock.lock().unwrap();
      s.active -= 1;

      if let Event::AttrSet { attrs, .. } = &event {
        debug!(attr = %attr, new_attrs = attrs.len(), "expanded attrset");
        for name in attrs {
          let mut child = path.clone();
          child.push(name.clone());
          s.todo.push(child);
        }
      } else {
        if let Event::Error(EvalError {
          fatal: true, error, ..
        }) = &event
        {
          error!(attr = %attr, error = %error, "fatal evaluation error");
          s.error = Some(error.clone());
        }
        sink.lock().unwrap()(&event).context("event sink returned an error")?;
      }

      cvar.notify_all();
    }

    let status = read_worker_line(&mut worker, "status", &attr)?;
    trace!(attr = %attr, status = %status, "received worker status");
    match status.as_str() {
      "ready" => {},
      "restart" => {
        info!("restarting worker due to memory limit");
        if let Ok(status) = worker.proc.wait() {
          debug!(?status, "previous worker exited");
        }
        worker = spawn_worker_pair(config)?;
      },
      other => {
        if let Ok(Event::Error(EvalError { error, .. })) =
          serde_json::from_str::<Event>(other)
        {
          error!(error = %error, "worker reported error");
          let mut s = lock.lock().unwrap();
          s.error = Some(error.clone());
          cvar.notify_all();
          bail!("{error}");
        }
        bail!("unexpected worker message: {other}");
      },
    }
  }
}

fn read_event(worker: &mut WorkerProcess, path: &[String]) -> Result<Event> {
  let attr = path.join(".");
  let resp = read_worker_line(worker, "event", &attr)?;
  serde_json::from_str(&resp)
    .with_context(|| format!("parsing worker response for {path:?}: {resp}"))
}

fn read_line(reader: &mut BufReader<impl io::Read>) -> Result<Option<String>> {
  let mut line = String::new();
  if reader.read_line(&mut line)? == 0 {
    return Ok(None);
  }
  Ok(Some(line.trim_matches(['\n', '\r', ' ']).to_string()))
}

fn read_worker_line(
  worker: &mut WorkerProcess,
  phase: &str,
  attr: &str,
) -> Result<String> {
  read_line(&mut worker.stdout)?
    .ok_or_else(|| worker_closed_stdout_error(worker, phase, attr))
}

fn worker_closed_stdout_error(
  worker: &mut WorkerProcess,
  phase: &str,
  attr: &str,
) -> anyhow::Error {
  let status = worker_status(&mut worker.proc);
  let stderr = worker.stderr.snapshot();
  anyhow::anyhow!(
    "{}",
    format_worker_closed_stdout_error(phase, attr, &status, &stderr)
  )
}

fn format_worker_closed_stdout_error(
  phase: &str,
  attr: &str,
  status: &str,
  stderr: &str,
) -> String {
  let mut message = format!(
    "evix worker closed stdout while reading {phase} for {attr}; worker \
     status: {status}"
  );
  if stderr.is_empty() {
    message.push_str("; worker stderr was empty");
  } else {
    message.push_str("; worker stderr:\n");
    message.push_str(stderr);
  }
  message
}

/// Spawn a worker subprocess, handshake, and return the child handle together
/// with its stdin writer and stdout reader.
///
/// The worker is the same binary re-executed with [`WORKER_ENV`] set.
fn spawn_worker_pair(config: &Config) -> Result<WorkerProcess> {
  let exe = env::current_exe().context("resolving current exe")?;
  debug!("spawning worker process");

  let mut child = Command::new(&exe)
    .env(WORKER_ENV, "1")
    .stdin(Stdio::piped())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .with_context(|| format!("spawning worker process from {exe:?}"))?;

  let mut stdin = child.stdin.take().context("worker stdin")?;
  writeln!(stdin, "{}", serde_json::to_string(config)?)?;
  stdin.flush()?;

  let stdout = BufReader::new(child.stdout.take().context("worker stdout")?);
  let stderr =
    WorkerStderr::capture(child.stderr.take().context("worker stderr")?);
  let mut worker = WorkerProcess {
    proc: child,
    stdin,
    stdout,
    stderr,
  };
  read_ready(&mut worker)?;
  info!("worker ready");

  Ok(worker)
}

fn read_ready(worker: &mut WorkerProcess) -> Result<()> {
  let line = read_worker_line(worker, "handshake", "<startup>")?;
  if line != "ready" {
    bail!("unexpected worker handshake: {line:?}");
  }
  Ok(())
}

fn worker_status(proc: &mut Child) -> String {
  proc
    .try_wait()
    .ok()
    .flatten()
    .map_or_else(|| "unknown".to_string(), |s| s.to_string())
}

fn capture_stderr(mut stderr: impl Read, sink: Arc<Mutex<Vec<u8>>>) {
  let mut buffer = [0; 1024];
  loop {
    match stderr.read(&mut buffer) {
      Ok(0) => break,
      Ok(n) => {
        let mut bytes = sink.lock().unwrap();
        bytes.extend_from_slice(&buffer[..n]);
        if bytes.len() > STDERR_CAPTURE_LIMIT {
          let drain = bytes.len() - STDERR_CAPTURE_LIMIT;
          bytes.drain(..drain);
        }
      },
      Err(_) => break,
    }
  }
}

#[cfg(test)]
mod tests {
  use super::format_worker_closed_stdout_error;

  #[test]
  fn closed_stdout_error_includes_phase_attr_status_and_stderr() {
    let message = format_worker_closed_stdout_error(
      "event",
      "packages.x86_64-linux.bad",
      "exit status: 1",
      "evix worker failed: locking flake\ncaused by: missing input",
    );

    assert!(message.contains("reading event"));
    assert!(message.contains("packages.x86_64-linux.bad"));
    assert!(message.contains("exit status: 1"));
    assert!(message.contains("locking flake"));
    assert!(!message.contains("worker closed stdout unexpectedly"));
  }

  #[test]
  fn closed_stdout_error_notes_empty_stderr() {
    let message = format_worker_closed_stdout_error(
      "handshake",
      "<startup>",
      "exit status: 101",
      "",
    );

    assert!(message.contains("reading handshake"));
    assert!(message.contains("<startup>"));
    assert!(message.contains("worker stderr was empty"));
  }
}
