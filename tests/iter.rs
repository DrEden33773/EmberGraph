#[test]
fn test_iter() {
  let v = [1, 2, 3, 4, 5];
  let mut iter = v.iter();
  assert_eq!(iter.next(), Some(&1));
  assert_eq!(iter.next(), Some(&2));
  assert_eq!(iter.copied().collect::<Vec<_>>(), vec![3, 4, 5]);
}
