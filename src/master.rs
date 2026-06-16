use std::{
    env, io,
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
    sync::{Arc, Condvar, Mutex},
    thread,
};

use anyhow::{Context as _, Result, bail};
use tracing::{debug, error, info, trace};

use crate::{Config, EvalError, Event, WORKER_ENV};

struct SharedState {
    todo: Vec<Vec<String>>,
    active: usize,
    error: Option<String>,
}

pub fn run<F>(config: &Config, sink: Arc<Mutex<F>>) -> Result<()>
where
    F: FnMut(&Event) -> Result<()> + Send + 'static,
{
    let shared = Arc::new((
        Mutex::new(SharedState {
            todo: vec![vec![]],
            active: 0,
            error: None,
        }),
        Condvar::new(),
    ));

    let n = config.workers.max(1);
    let mut handles = Vec::with_capacity(n);
    for _ in 0..n {
        let shared = Arc::clone(&shared);
        let sink = Arc::clone(&sink);
        let config = config.clone();
        handles.push(thread::spawn(move || collector(&config, shared, sink)));
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

fn collector<F>(
    config: &Config,
    shared: Arc<(Mutex<SharedState>, Condvar)>,
    sink: Arc<Mutex<F>>,
) -> Result<()>
where
    F: FnMut(&Event) -> Result<()>,
{
    let (lock, cvar) = &*shared;

    let (mut proc, mut child_stdin, mut reader) = spawn_worker_pair(config)?;

    loop {
        let path = {
            let mut s = lock.lock().unwrap();
            loop {
                if let Some(ref e) = s.error {
                    let msg = e.clone();
                    writeln!(child_stdin, "exit")?;
                    child_stdin.flush()?;
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
                    writeln!(child_stdin, "exit")?;
                    child_stdin.flush()?;
                    info!("evaluation queue empty, exiting worker");
                    return Ok(());
                }
                s = cvar.wait(s).unwrap();
            }
        };

        let Some(path) = path else {
            continue;
        };

        let attr = path.join(".");
        trace!(attr = %attr, "sending work to worker");
        writeln!(child_stdin, "do {}", serde_json::to_string(&path)?)?;
        child_stdin.flush()?;

        let event = read_event(&mut reader, &path).with_context(|| {
            format!(
                "reading event for {attr}; worker status: {}",
                worker_status(&mut proc)
            )
        })?;

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

        let status = read_line(&mut reader).with_context(|| {
            format!(
                "reading worker status for {attr}; worker status: {}",
                worker_status(&mut proc)
            )
        })?;
        trace!(attr = %attr, status = %status, "received worker status");
        match status.as_str() {
            "ready" => {}
            "restart" => {
                info!("restarting worker due to memory limit");
                if let Ok(status) = proc.wait() {
                    debug!(?status, "previous worker exited");
                }
                (proc, child_stdin, reader) = spawn_worker_pair(config)?;
            }
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
            }
        }
    }
}

fn read_event(reader: &mut BufReader<impl io::Read>, path: &[String]) -> Result<Event> {
    let resp = read_line(reader)?;
    serde_json::from_str(&resp)
        .with_context(|| format!("parsing worker response for {path:?}: {resp}"))
}

fn read_line(reader: &mut BufReader<impl io::Read>) -> Result<String> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        bail!("worker closed stdout unexpectedly");
    }
    Ok(line.trim_matches(['\n', '\r', ' ']).to_string())
}

fn spawn_worker_pair(config: &Config) -> Result<(Child, impl Write, BufReader<impl io::Read>)> {
    let exe = env::current_exe().context("resolving current exe")?;
    debug!("spawning worker process");

    let mut child = Command::new(&exe)
        .env(WORKER_ENV, "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .with_context(|| format!("spawning worker process from {exe:?}"))?;

    let mut stdin = child.stdin.take().context("worker stdin")?;
    writeln!(stdin, "{}", serde_json::to_string(config)?)?;
    stdin.flush()?;

    let mut reader = BufReader::new(child.stdout.take().context("worker stdout")?);
    read_ready(&mut reader)?;
    info!("worker ready");

    Ok((child, stdin, reader))
}

fn read_ready(reader: &mut BufReader<impl io::Read>) -> Result<()> {
    let line = read_line(reader)?;
    if line != "ready" {
        bail!("unexpected worker handshake: {line:?}");
    }
    Ok(())
}

fn worker_status(proc: &mut Child) -> String {
    proc.try_wait()
        .ok()
        .flatten()
        .map_or_else(|| "unknown".to_string(), |s| s.to_string())
}
