use tokio::time::Instant;
use tracing::{info, instrument};

pub mod dyn_graph;
pub mod expand_graph;

pub async fn time_async<F: Future<Output = O>, O>(future: F) -> (O, f64) {
  let start = Instant::now();
  let result = future.await;
  let elapsed = start.elapsed().as_millis_f64();
  (result, elapsed)
}

#[instrument(skip(future), fields(name = desc))]
pub async fn time_async_with_desc<F: Future<Output = O>, O>(future: F, desc: String) -> O {
  let (result, elapsed) = time_async(future).await;
  info!("{} ✅ {elapsed:.2}ms\n", desc);
  result
}
