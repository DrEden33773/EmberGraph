use tqdm::tqdm_async;

#[tokio::test]
async fn test_tqdm_async() {
  use tokio::time::{Duration, sleep};
  let future_iter = (0..100).map(|i| sleep(Duration::from_secs_f64(i as f64 / 100.0)));
  futures::future::join_all(tqdm_async(future_iter)).await;
}
