#[tokio::test(flavor = "multi_thread")]
async fn futures_with_tokio() {
  async fn operate(l: &i32, r: &i32) -> i32 {
    l * r
  }

  let outer = Vec::from_iter(1..=4);
  let inner = Vec::from_iter(1..=4);
  let mut futures = Vec::with_capacity(outer.len() * inner.len());
  let mut handles = Vec::with_capacity(outer.len() * inner.len());

  for i in outer.iter() {
    for j in inner.iter() {
      let future = operate(i, j);
      futures.push(future);
    }
  }

  for i in outer.iter() {
    for j in inner.iter() {
      let i = *i;
      let j = *j;
      let handle = tokio::spawn(async move { operate(&i, &j).await });
      handles.push(handle);
    }
  }

  let results_via_futures = futures::future::join_all(futures).await;
  let mut results_via_handles = vec![];
  for handle in handles {
    let result = handle.await.unwrap();
    results_via_handles.push(result);
  }
  assert_eq!(results_via_futures, results_via_handles);
}
