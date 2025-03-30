use tokio::time::Instant;

pub mod dyn_graph;
pub mod expand_graph;

pub async fn time_async<F: Future<Output = O>, O>(future: F) -> (O, f64) {
  let start = Instant::now();
  let result = future.await;
  let elapsed = start.elapsed().as_millis_f64();
  (result, elapsed)
}

pub async fn time_async_with_desc<F: Future<Output = O>, O>(future: F, desc: impl AsRef<str>) -> O {
  let (result, elapsed) = time_async(future).await;

  println!("{} ✅ {elapsed:.2}ms\n", desc.as_ref());

  result
}
