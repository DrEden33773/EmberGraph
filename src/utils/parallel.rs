pub async fn spawn_blocking<F, R>(f: F) -> R
where
  F: FnOnce() -> R + Send + 'static,
  R: Send + 'static,
{
  #[cfg(feature = "block_spawn_via_rayon")]
  {
    let (tx, rx) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
      let result = f();
      let _ = tx.send(result);
    });
    rx.await.unwrap()
  }
  #[cfg(not(feature = "block_spawn_via_rayon"))]
  {
    tokio::task::spawn_blocking(f).await.unwrap()
  }
}

pub fn config_before_run<F: Future>(to_run: F) -> F::Output {
  // rayon config
  rayon::ThreadPoolBuilder::new()
    .num_threads(num_cpus::get())
    .thread_name(|i| format!("rayon-{}", i))
    .build_global()
    .unwrap();

  // tokio config
  tokio::runtime::Builder::new_multi_thread()
    .enable_all()
    .worker_threads(num_cpus::get())
    .thread_name_fn(|| {
      use std::sync::atomic::{AtomicUsize, Ordering};
      static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
      let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
      format!("tokio-{}", id)
    })
    .build()
    .unwrap()
    .block_on(to_run)
}
