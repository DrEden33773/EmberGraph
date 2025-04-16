#[derive(Clone)]
pub struct KVPair<K: Clone, V: Clone> {
  pub key: K,
  pub value: V,
}

impl<K: Clone + PartialEq, V: Clone> PartialEq for KVPair<K, V> {
  fn eq(&self, other: &Self) -> bool {
    self.key == other.key
  }
}

impl<K: Clone + Eq, V: Clone> Eq for KVPair<K, V> {}

impl<K: Clone + PartialOrd, V: Clone> PartialOrd for KVPair<K, V> {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    self.key.partial_cmp(&other.key)
  }
}

impl<K: Clone + Ord, V: Clone> Ord for KVPair<K, V> {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self.key.cmp(&other.key)
  }
}
