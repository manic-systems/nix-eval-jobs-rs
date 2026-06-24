use std::{
  collections::VecDeque,
  future::Future,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  time::Duration,
};

use anyhow::{Context as _, Result, bail};
use tokio::{
  sync::{Mutex, Notify},
  time::timeout,
};
use tracing::{debug, error, info, trace};

use crate::{
  Config,
  EvalError,
  Event,
  Remote,
  remote_worker::RemoteWorker,
  worker_config::WorkerConfig,
  worker_process::{WorkResponse, WorkerProcess, WorkerStatus},
};

struct Scheduler {
  todo:         VecDeque<WorkItem>,
  active:       usize,
  worker_count: usize,
  error:        Option<String>,
}

#[derive(Clone)]
struct WorkItem {
  path:        Vec<String>,
  rejected_by: Vec<usize>,
}

#[derive(Clone)]
struct WorkerSpec {
  id:    usize,
  label: String,
  kind:  WorkerKind,
}

#[derive(Clone)]
enum WorkerKind {
  Local,
  Remote(Remote),
}

enum WorkerClient {
  Local(Box<WorkerProcess>),
  Remote(RemoteWorker),
}

enum EventDisposition {
  Emit,
  Requeue { system: String },
}

enum NextWork {
  Dispatch(WorkItem),
  Wait,
  Done,
  Fatal(String),
}

struct CompletedWork {
  emit:        bool,
  fatal_error: Option<String>,
}

pub async fn run<F, Fut>(
  config: Config,
  cancel: Arc<AtomicBool>,
  on_event: F,
) -> Result<()>
where
  F: FnMut(Event) -> Fut + Send + 'static,
  Fut: Future<Output = Result<()>> + Send + 'static,
{
  validate_config(&config)?;
  let specs = worker_specs(&config);
  if specs.is_empty() {
    bail!("evaluation requires at least one local or remote worker");
  }

  let shared = Arc::new(Mutex::new(Scheduler {
    todo:         VecDeque::from([WorkItem {
      path:        Vec::new(),
      rejected_by: Vec::new(),
    }]),
    active:       0,
    worker_count: specs.len(),
    error:        None,
  }));
  let notify = Arc::new(Notify::new());
  let on_event = Arc::new(Mutex::new(on_event));
  let worker_config = WorkerConfig::from(&config);

  let mut handles = Vec::with_capacity(specs.len());
  for spec in specs {
    handles.push(tokio::spawn(collector(
      worker_config.clone(),
      spec,
      Arc::clone(&cancel),
      Arc::clone(&shared),
      Arc::clone(&notify),
      Arc::clone(&on_event),
    )));
  }

  for handle in handles {
    handle.await.context("collector task panicked")??;
  }

  if let Some(error) = &shared.lock().await.error {
    bail!("{error}");
  }

  Ok(())
}

fn validate_config(config: &Config) -> Result<()> {
  for remote in &config.remotes {
    if remote.workers == 0 {
      bail!(
        "remote worker count for {} must be greater than zero",
        remote.endpoint
      );
    }
  }
  Ok(())
}

fn worker_specs(config: &Config) -> Vec<WorkerSpec> {
  let remote_workers: usize = config.remotes.iter().map(|r| r.workers).sum();
  let local_workers = if config.workers == 0 && remote_workers > 0 {
    0
  } else {
    config.workers.max(1)
  };

  let mut specs = Vec::with_capacity(local_workers + remote_workers);
  for _ in 0..local_workers {
    specs.push(WorkerSpec {
      id:    specs.len(),
      label: "local".into(),
      kind:  WorkerKind::Local,
    });
  }
  for remote in config.remotes.clone() {
    for index in 0..remote.workers {
      specs.push(WorkerSpec {
        id:    specs.len(),
        label: format!("remote:{}#{index}", remote.endpoint),
        kind:  WorkerKind::Remote(remote.clone()),
      });
    }
  }
  specs
}

impl Scheduler {
  fn next_for(&mut self, worker_id: usize) -> NextWork {
    if let Some(error) = self.error.clone() {
      return NextWork::Fatal(error);
    }
    if let Some(index) = self
      .todo
      .iter()
      .position(|item| !item.rejected_by.contains(&worker_id))
    {
      let item = self
        .todo
        .remove(index)
        .expect("position returned a valid index");
      self.active += 1;
      return NextWork::Dispatch(item);
    }
    if !self.todo.is_empty()
      && let Some(error) = self.exhausted_error()
    {
      self.error = Some(error.clone());
      return NextWork::Fatal(error);
    }
    if self.todo.is_empty() && self.active == 0 {
      return NextWork::Done;
    }
    NextWork::Wait
  }

  fn complete(
    &mut self,
    spec: &WorkerSpec,
    mut item: WorkItem,
    event: &Event,
  ) -> CompletedWork {
    let attr = display_attr(&item.path);
    self.active -= 1;

    match event {
      Event::AttrSet { attrs, .. } => {
        debug!(attr = %attr, new_attrs = attrs.len(), "expanded attrset");
        for name in attrs {
          let mut child = item.path.clone();
          child.push(name.clone());
          self.todo.push_back(WorkItem {
            path:        child,
            rejected_by: Vec::new(),
          });
        }
        CompletedWork {
          emit:        true,
          fatal_error: None,
        }
      },
      Event::Error(EvalError {
        fatal: true, error, ..
      }) => {
        error!(attr = %attr, error = %error, "fatal evaluation error");
        self.error = Some(error.clone());
        CompletedWork {
          emit:        true,
          fatal_error: Some(error.clone()),
        }
      },
      Event::Derivation(_) => {
        match event_disposition(spec, event) {
          EventDisposition::Emit => {
            CompletedWork {
              emit:        true,
              fatal_error: None,
            }
          },
          EventDisposition::Requeue { system } => {
            item.rejected_by.push(spec.id);
            if item.rejected_by.len() >= self.worker_count {
              let error = format!(
                "no worker accepted derivation at {attr} for system {system}"
              );
              self.error = Some(error.clone());
              CompletedWork {
                emit:        false,
                fatal_error: Some(error),
              }
            } else {
              debug!(
                worker = %spec.label,
                attr = %attr,
                system = %system,
                "worker rejected derivation system; requeueing"
              );
              self.todo.push_back(item);
              CompletedWork {
                emit:        false,
                fatal_error: None,
              }
            }
          },
        }
      },
      Event::Error(_) => {
        CompletedWork {
          emit:        true,
          fatal_error: None,
        }
      },
    }
  }

  fn exhausted_error(&self) -> Option<String> {
    let item = self
      .todo
      .iter()
      .find(|item| item.rejected_by.len() >= self.worker_count)?;
    Some(format!(
      "no worker accepted derivation at {}",
      display_attr(&item.path)
    ))
  }
}

async fn collector<F, Fut>(
  config: WorkerConfig,
  spec: WorkerSpec,
  cancel: Arc<AtomicBool>,
  shared: Arc<Mutex<Scheduler>>,
  notify: Arc<Notify>,
  on_event: Arc<Mutex<F>>,
) -> Result<()>
where
  F: FnMut(Event) -> Fut + Send + 'static,
  Fut: Future<Output = Result<()>> + Send + 'static,
{
  let mut worker = WorkerClient::connect(&config, &spec).await?;

  loop {
    let Some(item) =
      next_work(&spec, &cancel, &shared, &notify, &mut worker).await?
    else {
      return Ok(());
    };
    let attr = item.path.join(".");
    trace!(worker = %spec.label, attr = %attr, "sending work to worker");

    let event = worker.work(&item.path, &config, &spec).await?;
    let completed =
      record_response(&spec, item, event.clone(), &shared).await?;

    if completed.emit {
      let mut sink = on_event.lock().await;
      (*sink)(event)
        .await
        .context("event sink returned an error")?;
    }

    notify.notify_waiters();

    if let Some(error) = completed.fatal_error {
      worker.stop().await;
      bail!("{error}");
    }
  }
}

async fn next_work(
  spec: &WorkerSpec,
  cancel: &AtomicBool,
  shared: &Mutex<Scheduler>,
  notify: &Notify,
  worker: &mut WorkerClient,
) -> Result<Option<WorkItem>> {
  loop {
    if cancel.load(Ordering::Relaxed) {
      worker.stop().await;
      info!(worker = %spec.label, "cancellation requested, collector exiting");
      return Ok(None);
    }

    let next = shared.lock().await.next_for(spec.id);
    match next {
      NextWork::Dispatch(item) => {
        debug!(
          worker = %spec.label,
          attr = %item.path.join("."),
          "dispatched attribute"
        );
        return Ok(Some(item));
      },
      NextWork::Fatal(error) => {
        worker.stop().await;
        error!(worker = %spec.label, error = %error, "stopping collector due to fatal error");
        bail!("{error}");
      },
      NextWork::Done => {
        worker.stop().await;
        info!(worker = %spec.label, "evaluation queue empty, exiting worker");
        return Ok(None);
      },
      NextWork::Wait => {},
    }

    let _ = timeout(Duration::from_millis(200), notify.notified()).await;
  }
}

async fn record_response(
  spec: &WorkerSpec,
  item: WorkItem,
  event: Event,
  shared: &Mutex<Scheduler>,
) -> Result<CompletedWork> {
  Ok(shared.lock().await.complete(spec, item, &event))
}

fn event_disposition(spec: &WorkerSpec, event: &Event) -> EventDisposition {
  let WorkerKind::Remote(remote) = &spec.kind else {
    return EventDisposition::Emit;
  };
  let Event::Derivation(drv) = event else {
    return EventDisposition::Emit;
  };
  if remote.systems.is_empty()
    || remote.systems.iter().any(|system| system == &drv.system)
  {
    EventDisposition::Emit
  } else {
    EventDisposition::Requeue {
      system: drv.system.clone(),
    }
  }
}

fn display_attr(path: &[String]) -> String {
  if path.is_empty() {
    "<root>".into()
  } else {
    path.join(".")
  }
}

impl WorkerClient {
  async fn connect(config: &WorkerConfig, spec: &WorkerSpec) -> Result<Self> {
    match &spec.kind {
      WorkerKind::Local => {
        Ok(Self::Local(Box::new(
          WorkerProcess::spawn_local(config, &spec.label).await?,
        )))
      },
      WorkerKind::Remote(remote) => {
        Ok(Self::Remote(
          RemoteWorker::connect(&remote.endpoint, config, &spec.label).await?,
        ))
      },
    }
  }

  async fn work(
    &mut self,
    path: &[String],
    config: &WorkerConfig,
    spec: &WorkerSpec,
  ) -> Result<Event> {
    match self {
      Self::Local(worker) => {
        let WorkResponse { event, status } = worker.work(path).await?;
        if status == WorkerStatus::Restart {
          info!(worker = %spec.label, "restarting worker due to memory limit");
          worker.wait_for_restart().await;
          **worker = WorkerProcess::spawn_local(config, &spec.label).await?;
        }
        Ok(event)
      },
      Self::Remote(worker) => worker.work(path).await,
    }
  }

  async fn stop(&mut self) {
    match self {
      Self::Local(worker) => worker.stop().await,
      Self::Remote(worker) => worker.stop().await,
    }
  }
}

#[cfg(test)]
mod tests {
  use std::collections::{BTreeMap, BTreeSet, HashMap};

  use super::*;
  use crate::Derivation;

  #[test]
  fn scheduler_requeues_derivation_rejected_by_remote_system() {
    let first = WorkerSpec {
      id:    0,
      label: "remote:x86".into(),
      kind:  WorkerKind::Remote(Remote {
        endpoint: "x86:7357".into(),
        systems:  vec!["x86_64-linux".into()],
        workers:  1,
      }),
    };
    let second = WorkerSpec {
      id:    1,
      label: "remote:aarch64".into(),
      kind:  WorkerKind::Remote(Remote {
        endpoint: "aarch64:7357".into(),
        systems:  vec!["aarch64-linux".into()],
        workers:  1,
      }),
    };
    let mut scheduler = scheduler_with_workers(2);

    let item = match scheduler.next_for(first.id) {
      NextWork::Dispatch(item) => item,
      _ => panic!("expected dispatch"),
    };
    let completed =
      scheduler.complete(&first, item, &derivation("aarch64-linux"));
    assert!(!completed.emit);
    assert!(completed.fatal_error.is_none());

    assert!(matches!(scheduler.next_for(first.id), NextWork::Wait));
    let item = match scheduler.next_for(second.id) {
      NextWork::Dispatch(item) => item,
      _ => panic!("expected second worker dispatch"),
    };
    let completed =
      scheduler.complete(&second, item, &derivation("aarch64-linux"));
    assert!(completed.emit);
    assert!(completed.fatal_error.is_none());
  }

  #[test]
  fn scheduler_keeps_rejected_worker_alive_for_later_compatible_work() {
    let first = WorkerSpec {
      id:    0,
      label: "remote:x86".into(),
      kind:  WorkerKind::Remote(Remote {
        endpoint: "x86:7357".into(),
        systems:  vec!["x86_64-linux".into()],
        workers:  1,
      }),
    };
    let second = WorkerSpec {
      id:    1,
      label: "remote:aarch64".into(),
      kind:  WorkerKind::Remote(Remote {
        endpoint: "aarch64:7357".into(),
        systems:  vec!["aarch64-linux".into()],
        workers:  1,
      }),
    };
    let mut scheduler = scheduler_with_workers(2);

    let item = match scheduler.next_for(first.id) {
      NextWork::Dispatch(item) => item,
      _ => panic!("expected dispatch"),
    };
    let completed =
      scheduler.complete(&first, item, &derivation("aarch64-linux"));
    assert!(!completed.emit);

    assert!(matches!(scheduler.next_for(first.id), NextWork::Wait));
    let item = match scheduler.next_for(second.id) {
      NextWork::Dispatch(item) => item,
      _ => panic!("expected second worker dispatch"),
    };
    scheduler.complete(&second, item, &Event::AttrSet {
      attr:      "job".into(),
      attr_path: vec!["job".into()],
      attrs:     vec!["x86".into()],
    });

    let item = match scheduler.next_for(first.id) {
      NextWork::Dispatch(item) => item,
      _ => panic!("expected first worker to stay alive for new work"),
    };
    assert_eq!(item.path, vec!["job".to_owned(), "x86".to_owned()]);
  }

  #[test]
  fn scheduler_fails_when_no_worker_accepts_derivation_system() {
    let worker = WorkerSpec {
      id:    0,
      label: "remote:x86".into(),
      kind:  WorkerKind::Remote(Remote {
        endpoint: "x86:7357".into(),
        systems:  vec!["x86_64-linux".into()],
        workers:  1,
      }),
    };
    let mut scheduler = scheduler_with_workers(1);
    let item = match scheduler.next_for(worker.id) {
      NextWork::Dispatch(item) => item,
      _ => panic!("expected dispatch"),
    };

    let completed =
      scheduler.complete(&worker, item, &derivation("aarch64-linux"));
    assert!(!completed.emit);
    assert!(
      completed
        .fatal_error
        .as_deref()
        .is_some_and(|error| error.contains("aarch64-linux"))
    );
  }

  #[test]
  fn config_rejects_zero_remote_workers() {
    let config = Config {
      remotes: vec![Remote {
        endpoint: "worker:7357".into(),
        systems:  vec!["x86_64-linux".into()],
        workers:  0,
      }],
      ..Config::default()
    };

    let error = validate_config(&config).unwrap_err().to_string();
    assert!(error.contains("must be greater than zero"));
  }

  fn scheduler_with_workers(worker_count: usize) -> Scheduler {
    Scheduler {
      todo: VecDeque::from([WorkItem {
        path:        vec!["job".into()],
        rejected_by: Vec::new(),
      }]),
      active: 0,
      worker_count,
      error: None,
    }
  }

  fn derivation(system: &str) -> Event {
    Event::Derivation(Derivation {
      attr:          "job".into(),
      attr_path:     vec!["job".into()],
      name:          "job".into(),
      system:        system.into(),
      drv_path:      "/nix/store/job.drv".into(),
      outputs:       BTreeMap::new(),
      meta:          None,
      input_drvs:    BTreeMap::new(),
      constituents:  None,
      gc_root_error: None,
    })
  }

  struct Rng(u64);
  impl Rng {
    fn new(seed: u64) -> Self {
      Self(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
      let mut x = self.0;
      x ^= x << 13;
      x ^= x >> 7;
      x ^= x << 17;
      self.0 = x;
      x
    }

    fn below(&mut self, n: usize) -> usize {
      (self.next_u64() % n as u64) as usize
    }

    fn chance(&mut self, pct: u64) -> bool {
      self.next_u64() % 100 < pct
    }
  }

  enum Node {
    Set(Vec<String>),
    Drv(String),
    Err,
  }

  type Tree = HashMap<Vec<String>, Node>;

  const SYSTEMS: [&str; 3] = ["x86_64-linux", "aarch64-linux", "riscv64-linux"];

  fn gen_tree(rng: &mut Rng) -> Tree {
    let mut tree = Tree::new();
    build_node(&mut tree, Vec::new(), 0, rng);
    tree
  }

  fn build_node(
    tree: &mut Tree,
    path: Vec<String>,
    depth: usize,
    rng: &mut Rng,
  ) {
    let count = 1 + rng.below(4);
    let mut names = Vec::with_capacity(count);
    for i in 0..count {
      let name = format!("a{i}");
      let mut child = path.clone();
      child.push(name.clone());
      names.push(name);

      let leaf = depth >= 3 || rng.chance(55);
      if !leaf {
        build_node(tree, child, depth + 1, rng);
      } else if rng.chance(10) {
        tree.insert(child, Node::Err);
      } else {
        let system = SYSTEMS[rng.below(SYSTEMS.len())].to_string();
        tree.insert(child, Node::Drv(system));
      }
    }
    tree.insert(path, Node::Set(names));
  }

  fn expected_drvs(tree: &Tree) -> BTreeSet<Vec<String>> {
    tree
      .iter()
      .filter(|(_, node)| matches!(node, Node::Drv(_)))
      .map(|(path, _)| path.clone())
      .collect()
  }

  fn systems_present(tree: &Tree) -> BTreeSet<String> {
    tree
      .values()
      .filter_map(|node| {
        match node {
          Node::Drv(system) => Some(system.clone()),
          _ => None,
        }
      })
      .collect()
  }

  /// Event a worker would return for `path` in the generated tree. The
  /// scheduler consumes these through the same interface as real worker
  /// output and cannot tell the difference, which is what makes driving it
  /// this way a faithful test of the production state machine.
  fn produce(tree: &Tree, path: &[String]) -> Event {
    match tree
      .get(path)
      .expect("dispatched a path absent from the tree")
    {
      Node::Set(names) => {
        Event::AttrSet {
          attr:      display_attr(path),
          attr_path: path.to_vec(),
          attrs:     names.clone(),
        }
      },
      Node::Drv(system) => {
        Event::Derivation(Derivation {
          attr:          display_attr(path),
          attr_path:     path.to_vec(),
          name:          path.join("-"),
          system:        system.clone(),
          drv_path:      format!("/nix/store/{}.drv", path.join("-")),
          outputs:       BTreeMap::new(),
          meta:          None,
          input_drvs:    BTreeMap::new(),
          constituents:  None,
          gc_root_error: None,
        })
      },
      Node::Err => {
        Event::Error(EvalError {
          attr:      display_attr(path),
          attr_path: path.to_vec(),
          error:     "synthetic non-fatal error".into(),
          fatal:     false,
        })
      },
    }
  }

  fn local_worker(id: usize) -> WorkerSpec {
    WorkerSpec {
      id,
      label: format!("local#{id}"),
      kind: WorkerKind::Local,
    }
  }

  fn remote_worker(id: usize, systems: &[&str]) -> WorkerSpec {
    WorkerSpec {
      id,
      label: format!("remote#{id}"),
      kind: WorkerKind::Remote(Remote {
        endpoint: format!("h{id}:7357"),
        systems:  systems.iter().map(|s| (*s).to_string()).collect(),
        workers:  1,
      }),
    }
  }

  enum SimResult {
    Done(Vec<Vec<String>>),
    Fatal(String),
  }

  /// Drive the scheduler to completion: poll a worker, evaluate the dispatched
  /// path, feed the event back. Round-robins workers so a `Wait` yields to one
  /// that can make progress; caps steps to fail on a livelock instead of
  /// hanging.
  fn run_sim(tree: &Tree, workers: &[WorkerSpec]) -> SimResult {
    let mut scheduler = Scheduler {
      todo:         VecDeque::from([WorkItem {
        path:        Vec::new(),
        rejected_by: Vec::new(),
      }]),
      active:       0,
      worker_count: workers.len(),
      error:        None,
    };
    let mut emitted: Vec<Vec<String>> = Vec::new();
    let cap = (tree.len() + 1) * (workers.len() + 1) * 64 + 1024;
    let mut steps = 0usize;
    let mut cursor = 0usize;
    let mut waits = 0usize;

    loop {
      steps += 1;
      assert!(steps < cap, "scheduler did not terminate (livelock?)");
      let worker = &workers[cursor % workers.len()];
      match scheduler.next_for(worker.id) {
        NextWork::Dispatch(item) => {
          let event = produce(tree, &item.path);
          let is_drv = matches!(event, Event::Derivation(_));
          let path = item.path.clone();
          let completed = scheduler.complete(worker, item, &event);
          if completed.emit && is_drv {
            emitted.push(path);
          }
          if let Some(error) = completed.fatal_error {
            return SimResult::Fatal(error);
          }
          cursor = 0;
          waits = 0;
        },
        NextWork::Wait => {
          waits += 1;
          assert!(
            waits <= workers.len(),
            "every worker stalled while systems were covered"
          );
          cursor += 1;
        },
        NextWork::Done => return SimResult::Done(emitted),
        NextWork::Fatal(error) => return SimResult::Fatal(error),
      }
    }
  }

  /// Worker pools that together can evaluate every system.
  fn covered_topologies() -> Vec<(&'static str, Vec<WorkerSpec>)> {
    vec![
      ("single local", vec![local_worker(0)]),
      ("three local", vec![
        local_worker(0),
        local_worker(1),
        local_worker(2),
      ]),
      ("remote, one system each", vec![
        remote_worker(0, &["x86_64-linux"]),
        remote_worker(1, &["aarch64-linux"]),
        remote_worker(2, &["riscv64-linux"]),
      ]),
      ("remote, overlapping ownership", vec![
        remote_worker(0, &["x86_64-linux", "aarch64-linux"]),
        remote_worker(1, &["aarch64-linux", "riscv64-linux"]),
        remote_worker(2, &["riscv64-linux", "x86_64-linux"]),
      ]),
      ("local plus catch-all remote", vec![
        local_worker(0),
        remote_worker(1, &["x86_64-linux"]),
        remote_worker(2, &[]),
      ]),
    ]
  }

  #[test]
  fn distributed_eval_is_topology_invariant() {
    for seed in 1..=300u64 {
      let mut rng = Rng::new(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15));
      let tree = gen_tree(&mut rng);
      let expected = expected_drvs(&tree);

      for (label, workers) in covered_topologies() {
        match run_sim(&tree, &workers) {
          SimResult::Done(emitted) => {
            let set: BTreeSet<Vec<String>> = emitted.iter().cloned().collect();
            assert_eq!(
              set.len(),
              emitted.len(),
              "seed {seed} / {label}: a derivation was emitted more than once"
            );
            assert_eq!(
              set, expected,
              "seed {seed} / {label}: emitted derivation set diverged"
            );
          },
          SimResult::Fatal(error) => {
            panic!("seed {seed} / {label}: covered topology failed: {error}")
          },
        }
      }
    }
  }

  #[test]
  fn distributed_eval_fails_when_a_system_is_unowned() {
    let mut exercised = 0usize;
    for seed in 1..=200u64 {
      let mut rng = Rng::new(seed.wrapping_mul(0x0100_0000_01B3));
      let tree = gen_tree(&mut rng);
      // Only meaningful when the tree actually contains a riscv64 derivation
      // that the pool below cannot place.
      if !systems_present(&tree).contains("riscv64-linux") {
        continue;
      }
      exercised += 1;

      let workers = vec![
        remote_worker(0, &["x86_64-linux"]),
        remote_worker(1, &["aarch64-linux"]),
      ];
      match run_sim(&tree, &workers) {
        SimResult::Fatal(error) => {
          assert!(
            error.contains("no worker accepted"),
            "seed {seed}: unexpected failure message: {error}"
          )
        },
        SimResult::Done(_) => {
          panic!("seed {seed}: a pool with no riscv64 owner should have failed")
        },
      }
    }
    assert!(
      exercised > 0,
      "no generated tree exercised the unowned path"
    );
  }
}
