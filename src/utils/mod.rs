use tokio::time::Instant;

pub mod dyn_graph;
pub mod expand_graph;
pub mod parallel;

pub async fn time_async<F: Future<Output = O>, O>(future: F) -> (O, f64) {
  let start = Instant::now();
  let result = future.await;
  let elapsed = start.elapsed().as_millis_f64();
  (result, elapsed)
}

#[cfg(not(feature = "use_tracing"))]
pub async fn time_async_with_desc<F: Future<Output = O>, O>(future: F, _desc: String) -> O {
  let (result, _elapsed) = time_async(future).await;
  println!("{} ✅ {_elapsed:.2}ms\n", _desc);
  result
}

#[cfg(feature = "use_tracing")]
use tracing::{info, instrument};

#[cfg(feature = "use_tracing")]
#[instrument(skip(future), fields(name = desc))]
pub async fn time_async_with_desc<F: Future<Output = O>, O>(future: F, desc: String) -> O {
  let (result, elapsed) = time_async(future).await;
  info!("{} ✅ {elapsed:.2}ms\n", desc);
  result
}
