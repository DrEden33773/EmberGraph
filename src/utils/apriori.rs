use std::collections::BTreeSet;

use hashbrown::{HashMap, HashSet};

pub struct Apriori<'data> {
  data_list: &'data [HashSet<String>],
  min_support: usize,
}

impl<'data> Apriori<'data> {
  pub fn gen_max_size_freq_set(&self) -> HashMap<BTreeSet<String>, usize> {
    let mut max_freq_set = HashMap::new();
    let c1 = self.find_candidates_1();
    let mut temp_freq_set = self.find_freq_set(c1);

    while !temp_freq_set.is_empty() {
      max_freq_set = temp_freq_set;
      let ck = self.apriori_gen(&max_freq_set.keys().cloned().collect::<Vec<_>>());
      temp_freq_set = self.find_freq_set(ck.into_iter().map(|s| s.into_iter().collect()).collect());
    }

    max_freq_set
  }

  /// Find all `c1` (1-item sized candidates)
  fn find_candidates_1(&self) -> Vec<HashSet<String>> {
    let mut c1 = Vec::new();
    let mut item_set = HashSet::new();

    for data in self.data_list {
      item_set.extend(data.iter());
    }
    for item in item_set {
      c1.push([item.clone()].into());
    }
    c1
  }

  /// Lk -> C_{k+1}
  ///
  /// - Join `l1` and `l2` for each item in `Lk`
  /// - Then generate `C_{k+1}`
  fn apriori_gen(&self, lk: &[BTreeSet<String>]) -> Vec<BTreeSet<String>> {
    let mut ck: HashSet<BTreeSet<_>> = HashSet::new();

    for (i, s1) in lk.iter().enumerate() {
      for s2 in &lk[i + 1..] {
        let mut s_temp = s1.clone();
        s_temp.extend(s2.iter().cloned());
        if s_temp.len() == s1.len() + 1 && !ck.contains(&s_temp) {
          ck.insert(s_temp.clone());
        }
      }
    }

    ck.into_iter().collect()
  }

  /// Ck -> Lk
  ///
  /// - Find candidates (in `Ck`) which satisfies `support >= min_support`
  fn find_freq_set(&self, ck: Vec<HashSet<String>>) -> HashMap<BTreeSet<String>, usize> {
    let mut freq_set = HashMap::new();

    for c in ck {
      let count = self
        .data_list
        .iter()
        .filter(|data| c.is_subset(data))
        .count();
      if count >= self.min_support {
        freq_set.insert(c.into_iter().collect(), count);
      }
    }

    freq_set
  }
}

pub struct AprioriBuilder<'data> {
  data_list: &'data [HashSet<String>],
  min_support: Option<usize>,
}

impl<'data> AprioriBuilder<'data> {
  pub fn new(data_list: &'data [HashSet<String>]) -> Self {
    Self {
      data_list,
      min_support: None,
    }
  }

  pub fn min_support(mut self, min_support: usize) -> Self {
    self.min_support = Some(min_support);
    self
  }

  pub fn build(self) -> Apriori<'data> {
    let min_support = self.min_support.unwrap_or(2);
    Apriori {
      data_list: self.data_list,
      min_support,
    }
  }
}
