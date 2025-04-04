use crate::schemas::*;
use hashbrown::{HashMap, HashSet};
use std::{
  hash::Hash,
  ops::{BitOr, BitOrAssign},
};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VNode {
  pub(crate) e_in: HashSet<Eid>,
  pub(crate) e_out: HashSet<Eid>,
}

impl Hash for VNode {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.e_in.iter().for_each(|eid| eid.hash(state));
    self.e_out.iter().for_each(|eid| eid.hash(state));
  }
}

impl BitOrAssign for VNode {
  fn bitor_assign(&mut self, rhs: Self) {
    self.e_in.extend(rhs.e_in);
    self.e_out.extend(rhs.e_out);
  }
}

impl BitOr for VNode {
  type Output = VNode;

  fn bitor(self, rhs: Self) -> Self::Output {
    let mut e_in = self.e_in;
    let mut e_out = self.e_out;
    e_in.extend(rhs.e_in);
    e_out.extend(rhs.e_out);
    VNode { e_in, e_out }
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynGraph<VType: VBase = DataVertex, EType: EBase = DataEdge> {
  /// vid -> v_entity
  pub(crate) v_entities: HashMap<Vid, VType>,

  /// eid -> e_entity
  pub(crate) e_entities: HashMap<Eid, EType>,

  pub(crate) adj_table: HashMap<Vid, VNode>,

  /// vid -> v_pattern_str
  pub(crate) vid_2_pattern: HashMap<Vid, String>,

  /// eid -> e_pattern_str
  pub(crate) eid_2_pattern: HashMap<Eid, String>,

  /// v_pattern_str -> [vid]
  pub(crate) pattern_2_vids: HashMap<String, HashSet<Vid>>,

  /// e_pattern_str -> [eid]
  pub(crate) pattern_2_eids: HashMap<String, HashSet<Eid>>,
}

impl<VType: VBase, EType: EBase> Hash for DynGraph<VType, EType> {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    for (vid, v_node) in &self.adj_table {
      vid.hash(state);
      v_node.hash(state);
    }
  }
}

impl<VType: VBase, EType: EBase> AsRef<Self> for DynGraph<VType, EType> {
  fn as_ref(&self) -> &Self {
    self
  }
}

impl<VType: VBase, EType: EBase> Default for DynGraph<VType, EType> {
  fn default() -> Self {
    Self {
      v_entities: Default::default(),
      e_entities: Default::default(),
      adj_table: Default::default(),
      vid_2_pattern: Default::default(),
      eid_2_pattern: Default::default(),
      pattern_2_vids: Default::default(),
      pattern_2_eids: Default::default(),
    }
  }
}

impl<VType: VBase, EType: EBase> BitOr for DynGraph<VType, EType> {
  type Output = Self;

  fn bitor(self, rhs: Self) -> Self::Output {
    let mut v_entities = self.v_entities;
    let mut e_entities = self.e_entities;
    let mut v_patterns = self.vid_2_pattern;
    let mut e_patterns = self.eid_2_pattern;
    v_entities.extend(rhs.v_entities);
    e_entities.extend(rhs.e_entities);
    v_patterns.extend(rhs.vid_2_pattern);
    e_patterns.extend(rhs.eid_2_pattern);

    let mut res = DynGraph {
      v_entities,
      e_entities,
      vid_2_pattern: v_patterns,
      eid_2_pattern: e_patterns,
      ..Default::default()
    };

    // deal with `adj_table`
    for (vid, v_node) in self.adj_table {
      res.adj_table.insert(vid, v_node);
    }
    for (vid, v_node) in rhs.adj_table {
      if res.adj_table.contains_key(&vid) {
        res.adj_table.get_mut(&vid).unwrap().bitor_assign(v_node);
      } else {
        res.adj_table.insert(vid, v_node);
      }
    }

    // deal with `pattern_2_vs`
    for (p, vids) in self.pattern_2_vids.into_iter().chain(rhs.pattern_2_vids) {
      res.pattern_2_vids.entry(p).or_default().extend(vids);
    }

    // deal with `pattern_2_es`
    for (p, eids) in self.pattern_2_eids.into_iter().chain(rhs.pattern_2_eids) {
      res.pattern_2_eids.entry(p).or_default().extend(eids);
    }

    res
  }
}

impl<VType: VBase, EType: EBase> DynGraph<VType, EType> {
  pub fn is_subset_of(&self, other: &Self) -> bool {
    // adj_table
    for (vid, v_node) in self.adj_table.iter() {
      // vertex
      if !other.adj_table.contains_key(vid) {
        return false;
      }
      // in_edge
      if !v_node.e_in.is_subset(&other.adj_table[vid].e_in) {
        return false;
      }
      // out_edge
      if !v_node.e_out.is_subset(&other.adj_table[vid].e_out) {
        return false;
      }
    }
    // v_patterns
    for (vid, v_pattern) in self.vid_2_pattern.iter() {
      if !other.vid_2_pattern.contains_key(vid) {
        return false;
      }
      if other.vid_2_pattern[vid] != *v_pattern {
        return false;
      }
    }
    // e_patterns
    for (eid, e_pattern) in self.eid_2_pattern.iter() {
      if !other.eid_2_pattern.contains_key(eid) {
        return false;
      }
      if other.eid_2_pattern[eid] != *e_pattern {
        return false;
      }
    }
    true
  }

  pub fn is_superset_of(&self, other: &Self) -> bool {
    other.is_subset_of(self)
  }

  pub fn v_entities(&self) -> &HashMap<Vid, VType> {
    &self.v_entities
  }

  pub fn e_entities(&self) -> &HashMap<Eid, EType> {
    &self.e_entities
  }
}

impl<VType: VBase, EType: EBase> DynGraph<VType, EType> {
  pub fn update_v(&mut self, vertex: VType, pattern: impl AsRef<str>) -> &mut Self {
    let vid = vertex.vid().to_string();

    self.v_entities.insert(vid.clone(), vertex);
    self.adj_table.entry(vid.clone()).or_default();

    let old_pattern = self
      .vid_2_pattern
      .insert(vid.clone(), pattern.as_ref().to_string());
    if let Some(old_pattern) = old_pattern {
      self
        .pattern_2_vids
        .get_mut(&old_pattern)
        .unwrap()
        .remove(&vid);
    }
    self
      .pattern_2_vids
      .entry(pattern.as_ref().to_string())
      .or_default()
      .insert(vid);

    self
  }

  pub fn update_v_batch(
    &mut self,
    v_pattern_pairs: impl IntoIterator<Item = (VType, String)>,
  ) -> &mut Self {
    for (vertex, pattern) in v_pattern_pairs {
      self.update_v(vertex, pattern);
    }
    self
  }

  pub fn update_e(&mut self, edge: EType, pattern: String) -> &mut Self {
    let eid = edge.eid().to_string();
    let src_vid = edge.src_vid().to_string();
    let dst_vid = edge.dst_vid().to_string();

    if self.has_all_vids(&[&src_vid, &dst_vid]) {
      self.e_entities.insert(eid.clone(), edge);

      self
        .adj_table
        .entry(src_vid)
        .or_default()
        .e_out
        .insert(eid.clone());

      self
        .adj_table
        .entry(dst_vid)
        .or_default()
        .e_in
        .insert(eid.clone());

      let old_pattern = self.eid_2_pattern.insert(eid.clone(), pattern.clone());
      if let Some(old_pattern) = old_pattern {
        self
          .pattern_2_eids
          .get_mut(&old_pattern)
          .unwrap()
          .remove(&eid);
      }
      self.pattern_2_eids.entry(pattern).or_default().insert(eid);

      self
    } else if self.has_vid(&src_vid) {
      panic!(
        "Detected `half-dangling edge`:\n\t(vid: {}) -[eid: {}]-> ?",
        src_vid, eid
      );
    } else if self.has_vid(&dst_vid) {
      panic!(
        "Detected `half-dangling edge`:\n\t? -[eid: {}]-> (vid: {})",
        eid, dst_vid
      );
    } else {
      panic!("Detected `dangling edge`:\n\t? -[eid: {}]-> ?", eid);
    }
  }

  pub fn update_e_batch(
    &mut self,
    e_pattern_pairs: impl IntoIterator<Item = (EType, String)>,
  ) -> &mut Self {
    for (edge, pattern) in e_pattern_pairs {
      self.update_e(edge, pattern);
    }
    self
  }

  pub fn remove_e(&mut self, eid: EidRef) -> &mut Self {
    if !self.has_eid(eid) {
      return self;
    }

    for v in self.adj_table.values_mut() {
      v.e_in.remove(eid);
      v.e_out.remove(eid);
    }
    self.e_entities.remove(eid);

    if let Some(pattern) = self.eid_2_pattern.remove(eid) {
      self.pattern_2_eids.get_mut(&pattern).unwrap().remove(eid);
    }

    self
  }

  pub fn remove_e_batch(&mut self, eids: &[EidRef]) -> &mut Self {
    for eid in eids {
      self.remove_e(eid);
    }
    self
  }
}

impl<VType: VBase, EType: EBase> DynGraph<VType, EType> {
  #[inline]
  pub fn get_v_from_vid(&self, vid: VidRef) -> Option<&VType> {
    self.v_entities.get(vid)
  }
  #[inline]
  pub fn get_e_from_eid(&self, eid: EidRef) -> Option<&EType> {
    self.e_entities.get(eid)
  }

  pub fn get_first_connective_vid_for_e(&self, edge: &EType) -> Option<Vid> {
    let src_vid = edge.src_vid();
    let dst_vid = edge.dst_vid();
    if self.has_vid(src_vid) {
      Some(src_vid.to_string())
    } else if self.has_vid(dst_vid) {
      Some(dst_vid.to_string())
    } else {
      None
    }
  }

  #[inline]
  pub fn get_vid_set(&self) -> HashSet<Vid> {
    self.v_entities.keys().cloned().collect()
  }
  #[inline]
  pub fn get_eid_set(&self) -> HashSet<Eid> {
    self.e_entities.keys().cloned().collect()
  }
  #[inline]
  pub fn get_v_entities(&self) -> Vec<VType> {
    self.v_entities.values().cloned().collect()
  }
  #[inline]
  pub fn get_e_entities(&self) -> Vec<EType> {
    self.e_entities.values().cloned().collect()
  }
  #[inline]
  pub fn get_v_pat_str_set(&self) -> HashSet<String> {
    self.vid_2_pattern.values().cloned().collect()
  }
  #[inline]
  pub fn get_e_pat_str_set(&self) -> HashSet<String> {
    self.eid_2_pattern.values().cloned().collect()
  }
  #[inline]
  pub fn get_all_pat_str_set(&self) -> HashSet<String> {
    let mut res = self.get_v_pat_str_set();
    res.extend(self.get_e_pat_str_set());
    res
  }
  #[inline]
  pub fn get_v_pattern_pairs(&self) -> Vec<(VType, String)> {
    self
      .v_entities
      .iter()
      .map(|(vid, v_entity)| {
        let pattern = self.vid_2_pattern[vid].clone();
        (v_entity.clone(), pattern)
      })
      .collect()
  }
  #[inline]
  pub fn drain_v_pattern_pairs(&mut self) -> Vec<(VType, String)> {
    self
      .v_entities
      .drain()
      .map(|(vid, v_entity)| {
        let pattern = self.vid_2_pattern.remove(&vid).unwrap();
        (v_entity, pattern)
      })
      .collect()
  }
  #[inline]
  pub fn get_e_pattern_pairs(&self) -> Vec<(EType, String)> {
    self
      .e_entities
      .iter()
      .map(|(eid, e_entity)| {
        let pattern = self.eid_2_pattern[eid].clone();
        (e_entity.clone(), pattern)
      })
      .collect()
  }
  #[inline]
  pub fn drain_e_pattern_pairs(&mut self) -> Vec<(EType, String)> {
    self
      .e_entities
      .drain()
      .map(|(eid, e_entity)| {
        let pattern = self.eid_2_pattern.remove(&eid).unwrap();
        (e_entity, pattern)
      })
      .collect()
  }
  #[inline]
  pub fn get_v_patterns(&self) -> HashSet<String> {
    self.vid_2_pattern.values().cloned().collect()
  }
  #[inline]
  pub fn get_e_patterns(&self) -> HashSet<String> {
    self.eid_2_pattern.values().cloned().collect()
  }
  #[inline]
  pub fn get_v_count(&self) -> usize {
    self.v_entities.len()
  }
  #[inline]
  pub fn get_e_count(&self) -> usize {
    self.e_entities.len()
  }
}

impl<VType: VBase, EType: EBase> DynGraph<VType, EType> {
  #[inline]
  pub fn has_vid(&self, vid: VidRef) -> bool {
    self.v_entities.contains_key(vid)
  }
  #[inline]
  pub fn has_all_vids(&self, vids: &[VidRef]) -> bool {
    vids.iter().all(|vid| self.has_vid(vid))
  }
  #[inline]
  pub fn has_any_vids(&self, vids: &[VidRef]) -> bool {
    vids.iter().any(|vid| self.has_vid(vid))
  }

  #[inline]
  pub fn has_eid(&self, eid: EidRef) -> bool {
    self.e_entities.contains_key(eid)
  }
  #[inline]
  pub fn has_all_eids(&self, eids: &[EidRef]) -> bool {
    eids.iter().all(|eid| self.has_eid(eid))
  }
  #[inline]
  pub fn has_any_eids(&self, eids: &[EidRef]) -> bool {
    eids.iter().any(|eid| self.has_eid(eid))
  }
}

impl<VType: VBase, EType: EBase> DynGraph<VType, EType> {
  #[inline]
  pub fn is_e_connective(&self, edge: &EType) -> bool {
    self.has_any_vids(&[edge.src_vid(), edge.dst_vid()])
  }
  #[inline]
  pub fn is_e_full_connective(&self, edge: &EType) -> bool {
    self.has_all_vids(&[edge.src_vid(), edge.dst_vid()])
  }
}

impl<VType: VBase, EType: EBase> DynGraph<VType, EType> {
  /// get all adj edges grouped by target vid
  /// (edge's direction: in | out)
  pub fn get_adj_es_grouped_by_target_vid(&self, curr_vid: VidRef) -> HashMap<Vid, HashSet<EType>> {
    let mut res: HashMap<Vid, HashSet<_>> = HashMap::new();
    if let Some(v_node) = self.adj_table.get(curr_vid) {
      for eid in v_node.e_in.union(&v_node.e_out) {
        let edge = self.get_e_from_eid(eid).unwrap();
        // target_vid != curr_vid
        let target_vid = if edge.src_vid() == curr_vid {
          edge.dst_vid().to_string()
        } else {
          edge.src_vid().to_string()
        };
        let target_edge = self.e_entities.get(eid).unwrap().clone();
        res.entry(target_vid).or_default().insert(target_edge);
      }
    }
    res
  }

  pub fn get_adj_eids(&self, vid: VidRef) -> HashSet<Eid> {
    if let Some(v_node) = self.adj_table.get(vid) {
      v_node.e_in.union(&v_node.e_out).cloned().collect()
    } else {
      HashSet::new()
    }
  }

  pub fn get_adj_vids(&self, vid: VidRef) -> HashSet<Vid> {
    if let Some(v_node) = self.adj_table.get(vid) {
      v_node
        .e_in
        .union(&v_node.e_out)
        .map(|eid| {
          let edge = self.get_e_from_eid(eid).unwrap();
          if edge.src_vid() == vid {
            edge.dst_vid().to_string()
          } else {
            edge.src_vid().to_string()
          }
        })
        .collect()
    } else {
      HashSet::new()
    }
  }

  pub fn get_out_degree(&self, vid: VidRef) -> usize {
    if let Some(v_node) = self.adj_table.get(vid) {
      v_node.e_out.len()
    } else {
      0
    }
  }

  pub fn get_in_degree(&self, vid: VidRef) -> usize {
    if let Some(v_node) = self.adj_table.get(vid) {
      v_node.e_in.len()
    } else {
      0
    }
  }
}
