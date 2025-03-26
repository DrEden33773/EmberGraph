use ahash::AHashMap;

#[test]
fn hashmap_indexer_demo() {
  let dict = AHashMap::from_iter((0..100).map(|i| (i, (i * 2).to_string())));
  let a = &dict[&0];
  let b = &dict[&1];
  let c = &dict[&2];
  assert_eq!(a, "0");
  assert_eq!(b, "2");
  assert_eq!(c, "4");
}
