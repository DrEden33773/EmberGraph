pub async fn spawn_blocking<F, R>(f: F) -> R
where
  F: FnOnce() -> R + Send + 'static,
  R: Send + 'static,
{
  #[cfg(feature = "block_spawn_via_rayon")]
  {
    let (send, recv) = tokio::sync::oneshot::channel();
    rayon::spawn(move || {
      let result = f();
      let _ = send.send(result);
    });
    recv.await.unwrap()
  }
  #[cfg(not(feature = "block_spawn_via_rayon"))]
  {
    tokio::task::spawn_blocking(f).await.unwrap()
  }
}
