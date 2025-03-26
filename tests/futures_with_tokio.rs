#[tokio::test(flavor = "multi_thread")]
async fn futures_with_tokio() {
  async fn operate(l: &i32, r: &i32) -> i32 {
    l * r
  }

  let outer = Vec::from_iter(1..=4);
  let inner = Vec::from_iter(1..=4);
  let mut futures = vec![];

  for i in outer.iter() {
    for j in inner.iter() {
      let future = operate(i, j);
      futures.push(future);
    }
  }

  let results = futures::future::join_all(futures).await;
  println!("Results: {:?}", results);
}
