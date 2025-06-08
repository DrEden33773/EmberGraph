use crate::schemas::*;
use hashbrown::{HashMap, HashSet};
use std::ops::{BitOr, BitOrAssign};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VNode {
  pub(crate) e_in: HashSet<Eid>,
  pub(crate) e_out: HashSet<Eid>,
}

impl BitOrAssign for VNode {
  fn bitor_assign(&mut self, rhs: Self) {
    self.e_in.extend(rhs.e_in);
    self.e_out.extend(rhs.e_out);
  }
}

#[derive(Debug, Clone)]
pub struct DynGraph<VType: VBase = DataVertex, EType: EBase = DataEdge> {
  /// vid -> v_entity
  pub(crate) v_entities: HashMap<Vid, VType>,

  /// eid -> e_entity
  pub(crate) e_entities: HashMap<Eid, EType>,

  pub(crate) adj_table: HashMap<Vid, VNode>,

  /// v_pattern_str -> [vid]
  pub(crate) pattern_2_vids: HashMap<String, HashSet<Vid>>,

  /// e_pattern_str -> [eid]
  pub(crate) pattern_2_eids: HashMap<String, HashSet<Eid>>,
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
    v_entities.extend(rhs.v_entities);
    e_entities.extend(rhs.e_entities);

    let mut res = DynGraph {
      v_entities,
      e_entities,
      adj_table: HashMap::with_capacity(self.adj_table.len() + rhs.adj_table.len()),
      pattern_2_vids: HashMap::with_capacity(self.pattern_2_vids.len() + rhs.pattern_2_vids.len()),
      pattern_2_eids: HashMap::with_capacity(self.pattern_2_eids.len() + rhs.pattern_2_eids.len()),
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
  #[inline]
  pub fn has_common_v(&self, other: &Self) -> bool {
    let (shorter, longer) = if self.v_entities.len() < other.v_entities.len() {
      (self, other)
    } else {
      (other, self)
    };

    shorter
      .v_entities
      .keys()
      .any(|vid| longer.v_entities.contains_key(vid))
  }
}

impl<VType: VBase, EType: EBase> DynGraph<VType, EType> {
  #[inline]
  pub fn v_entities(&self) -> &HashMap<Vid, VType> {
    &self.v_entities
  }
  #[inline]
  pub fn e_entities(&self) -> &HashMap<Eid, EType> {
    &self.e_entities
  }
}

impl<VType: VBase, EType: EBase> DynGraph<VType, EType> {
  pub fn update_v(&mut self, vertex: VType, pattern: impl AsRef<str>) -> &mut Self {
    let vid = vertex.vid().to_string();

    self.v_entities.insert(vid.clone(), vertex);
    self.adj_table.entry(vid.clone()).or_default();

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

      self.pattern_2_eids.entry(pattern).or_default().insert(eid);

      self
    } else if self.has_vid(&src_vid) {
      panic!("Detected `half-dangling edge`:\n\t(vid: {src_vid}) -[eid: {eid}]-> ?");
    } else if self.has_vid(&dst_vid) {
      panic!("Detected `half-dangling edge`:\n\t? -[eid: {eid}]-> (vid: {dst_vid})");
    } else {
      panic!("Detected `dangling edge`:\n\t? -[eid: {eid}]-> ?");
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
}

impl<VType: VBase, EType: EBase> DynGraph<VType, EType> {
  #[inline]
  pub fn view_v_from_vid(&self, vid: VidRef) -> Option<&VType> {
    self.v_entities.get(vid)
  }
  #[inline]
  pub fn view_e_from_eid(&self, eid: EidRef) -> Option<&EType> {
    self.e_entities.get(eid)
  }

  #[inline]
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
  pub fn view_vids(&self) -> Vec<VidRef<'_>> {
    self.v_entities.keys().map(String::as_str).collect()
  }
  #[inline]
  pub fn view_eids(&self) -> Vec<EidRef<'_>> {
    self.e_entities.keys().map(String::as_str).collect()
  }
  #[inline]
  pub fn view_v_entities(&self) -> Vec<&VType> {
    self.v_entities.values().collect()
  }
  #[inline]
  pub fn view_e_entities(&self) -> Vec<&EType> {
    self.e_entities.values().collect()
  }

  #[inline]
  pub fn get_v_pattern_pairs_cloned(&self) -> Vec<(VType, String)> {
    self
      .pattern_2_vids
      .iter()
      .flat_map(|(pattern, vids)| {
        vids.iter().filter_map(|vid| {
          self
            .v_entities
            .get(vid)
            .map(|v_entity| (v_entity.clone(), pattern.clone()))
        })
      })
      .collect()
  }
  #[inline]
  pub fn get_e_pattern_pairs_cloned(&self) -> Vec<(EType, String)> {
    self
      .pattern_2_eids
      .iter()
      .flat_map(|(pattern, eids)| {
        eids.iter().filter_map(|eid| {
          self
            .e_entities
            .get(eid)
            .map(|e_entity| (e_entity.clone(), pattern.clone()))
        })
      })
      .collect()
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
  pub fn contains_e_pattern(&self, pattern: &str) -> bool {
    self.pattern_2_eids.contains_key(pattern)
  }
  #[inline]
  pub fn contains_v_pattern(&self, pattern: &str) -> bool {
    self.pattern_2_vids.contains_key(pattern)
  }

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
  #[inline]
  pub fn pick_e_connective_vid<'a>(
    &'a self,
    edge: &'a EType,
  ) -> (Option<VidRef<'a>>, Option<VidRef<'a>>) {
    let src_vid = edge.src_vid();
    let dst_vid = edge.dst_vid();
    if self.has_all_vids(&[src_vid, dst_vid]) {
      (Some(src_vid), Some(dst_vid))
    } else if self.has_vid(src_vid) {
      (Some(src_vid), None)
    } else if self.has_vid(dst_vid) {
      (None, Some(dst_vid))
    } else {
      (None, None)
    }
  }
}

impl<VType: VBase, EType: EBase> DynGraph<VType, EType> {
  /// get all adj edges grouped by target vid
  /// (edge's direction: in | out)
  pub fn view_adj_es_grouped_by_target_vid<'g>(
    &'g self,
    curr_vid: VidRef<'g>,
  ) -> HashMap<VidRef<'g>, HashSet<&'g EType>> {
    let mut res: HashMap<VidRef<'g>, HashSet<_>> = HashMap::new();
    if let Some(v_node) = self.adj_table.get(curr_vid) {
      for eid in v_node.e_in.union(&v_node.e_out) {
        let edge = self.view_e_from_eid(eid).unwrap();
        // target_vid != curr_vid
        let target_vid = if edge.src_vid() == curr_vid {
          edge.dst_vid()
        } else {
          edge.src_vid()
        };
        let target_edge = self.e_entities.get(eid).unwrap();
        res.entry(target_vid).or_default().insert(target_edge);
      }
    }
    res
  }

  #[inline]
  pub fn get_adj_eids(&self, vid: VidRef) -> HashSet<Eid> {
    if let Some(v_node) = self.adj_table.get(vid) {
      v_node.e_in.union(&v_node.e_out).cloned().collect()
    } else {
      HashSet::new()
    }
  }
  #[inline]
  pub fn get_adj_vids(&self, vid: VidRef) -> HashSet<Vid> {
    if let Some(v_node) = self.adj_table.get(vid) {
      v_node
        .e_in
        .union(&v_node.e_out)
        .map(|eid| {
          let edge = self.view_e_from_eid(eid).unwrap();
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

  #[inline]
  pub fn get_out_degree(&self, vid: VidRef) -> usize {
    if let Some(v_node) = self.adj_table.get(vid) {
      v_node.e_out.len()
    } else {
      0
    }
  }
  #[inline]
  pub fn get_in_degree(&self, vid: VidRef) -> usize {
    if let Some(v_node) = self.adj_table.get(vid) {
      v_node.e_in.len()
    } else {
      0
    }
  }
}
