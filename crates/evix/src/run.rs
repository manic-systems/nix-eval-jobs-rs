use std::{
  future::Future,
  mem,
  sync::{Arc, atomic::AtomicBool},
};

use anyhow::Result;
use tokio::sync::Mutex;

use crate::{
  Config,
  EvalError,
  Event,
  state::{EvalAccumulator, EvalGraph},
};

pub async fn evaluate<F, Fut>(
  config: Config,
  cancel: Arc<AtomicBool>,
  on_event: F,
) -> Result<(EvalGraph, Vec<EvalError>)>
where
  F: FnMut(Event) -> Fut + Send + 'static,
  Fut: Future<Output = Result<()>> + Send + 'static,
{
  let accumulator = Arc::new(Mutex::new(EvalAccumulator::default()));
  let on_event = Arc::new(Mutex::new(on_event));

  crate::async_master::run(config, Arc::clone(&cancel), {
    let accumulator = Arc::clone(&accumulator);
    let on_event = Arc::clone(&on_event);
    move |event| {
      let accumulator = Arc::clone(&accumulator);
      let on_event = Arc::clone(&on_event);
      async move {
        accumulator.lock().await.record(&event);
        let mut sink = on_event.lock().await;
        (*sink)(event).await
      }
    }
  })
  .await?;

  let mut accumulator = accumulator.lock().await;
  Ok((
    mem::take(&mut accumulator.graph),
    mem::take(&mut accumulator.errors),
  ))
}
