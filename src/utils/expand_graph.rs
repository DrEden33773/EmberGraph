use ahash::{AHashMap, AHashSet};

use crate::schemas::{DataEdge, DataVertex, EBase, Eid, VBase, Vid};

use super::dyn_graph::{DynGraph, VNode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpandGraph<VType: VBase = DataVertex, EType: EBase = DataEdge> {
  pub(crate) dyn_graph: DynGraph<VType, EType>,
  pub(crate) target_v_adj_table: AHashMap<Vid, VNode>,

  pub(crate) dangling_e_entities: AHashMap<Eid, EType>,
  pub(crate) target_v_entities: AHashMap<Vid, VType>,

  pub(crate) dangling_e_patterns: AHashMap<Eid, String>,
  pub(crate) target_v_patterns: AHashMap<Vid, String>,
}

impl<VType: VBase, EType: EBase> Default for ExpandGraph<VType, EType> {
  fn default() -> Self {
    Self {
      dyn_graph: Default::default(),
      target_v_adj_table: Default::default(),
      dangling_e_entities: Default::default(),
      target_v_entities: Default::default(),
      dangling_e_patterns: Default::default(),
      target_v_patterns: Default::default(),
    }
  }
}

impl<VType: VBase, EType: EBase> From<&DynGraph<VType, EType>> for ExpandGraph<VType, EType> {
  fn from(val: &DynGraph<VType, EType>) -> Self {
    Self {
      dyn_graph: val.clone(),
      ..Default::default()
    }
  }
}

impl<VType: VBase, EType: EBase> From<ExpandGraph<VType, EType>> for DynGraph<VType, EType> {
  fn from(mut val: ExpandGraph<VType, EType>) -> Self {
    let mut graph = val.dyn_graph;

    graph.update_v_batch(val.target_v_entities.into_values().map(|v| {
      let pattern = val.target_v_patterns.remove(v.vid()).unwrap();
      (v, pattern)
    }));

    for target_v in val.target_v_adj_table.keys() {
      let mut dangling_eids = val.target_v_adj_table[target_v].e_out.to_owned();
      dangling_eids.extend(val.target_v_adj_table[target_v].e_in.clone());

      let dangling_e_pattern_pairs = dangling_eids
        .into_iter()
        .filter_map(|eid| val.dangling_e_entities.remove(&eid))
        .map(|e| {
          let pattern = val.dangling_e_patterns.remove(e.eid()).unwrap();
          (e, pattern)
        });
      graph.update_e_batch(dangling_e_pattern_pairs);
    }

    graph
  }
}

impl<VType: VBase, EType: EBase> ExpandGraph<VType, EType> {
  pub fn get_vid_set(&self) -> AHashSet<String> {
    self.dyn_graph.get_vid_set()
  }
  pub fn get_eid_set(&self) -> AHashSet<String> {
    self.dyn_graph.get_eid_set()
  }
  pub fn get_v_count(&self) -> usize {
    self.dyn_graph.get_v_count()
  }
  pub fn get_e_count(&self) -> usize {
    self.dyn_graph.get_e_count()
  }
  pub fn dangling_e_patterns(&self) -> &AHashMap<Eid, String> {
    &self.dangling_e_patterns
  }
  pub fn target_v_patterns(&self) -> &AHashMap<Vid, String> {
    &self.target_v_patterns
  }
}

impl<VType: VBase, EType: EBase> ExpandGraph<VType, EType> {
  pub fn group_dangling_e_by_pending_v(&self) -> AHashMap<String, Vec<EType>> {
    let mut grouped: AHashMap<String, Vec<EType>> = AHashMap::new();

    for dangling_e in self.dangling_e_entities.values() {
      if self.dyn_graph.has_vid(dangling_e.src_vid()) {
        grouped
          .entry(dangling_e.src_vid().to_owned())
          .or_default()
          .push(dangling_e.clone());
      } else if self.dyn_graph.has_vid(dangling_e.dst_vid()) {
        grouped
          .entry(dangling_e.dst_vid().to_owned())
          .or_default()
          .push(dangling_e.clone());
      }
    }

    grouped
  }

  fn is_valid_edge(&self, e: &EType) -> bool {
    self.dyn_graph.is_e_connective(e) && !self.dyn_graph.is_e_full_connective(e)
  }

  pub fn update_valid_dangling_edges(
    &mut self,
    dangling_edges: impl IntoIterator<Item = (EType, String)>,
  ) -> AHashSet<String> {
    let mut legal_eids = AHashSet::new();

    for (e, pattern) in dangling_edges {
      if !self.is_valid_edge(&e) {
        continue;
      }
      legal_eids.insert(e.eid().to_owned());
      self.dangling_e_patterns.insert(e.eid().to_owned(), pattern);
      self.dangling_e_entities.insert(e.eid().to_owned(), e);
    }

    legal_eids
  }

  fn is_valid_target(&self, v: &VType) -> bool {
    for e in self.dangling_e_entities.values() {
      if e.contains(v.vid()) {
        return true;
      }
    }
    false
  }

  pub fn update_valid_target_vertices(
    &mut self,
    target_vertices: impl IntoIterator<Item = (VType, String)>,
  ) -> AHashSet<String> {
    let mut legal_vids = AHashSet::new();
    for (v, pattern) in target_vertices {
      if !self.is_valid_target(&v) {
        continue;
      }
      legal_vids.insert(v.vid().to_owned());
      self.target_v_patterns.insert(v.vid().to_owned(), pattern);
      self.target_v_entities.insert(v.vid().to_owned(), v);
    }

    for dangling_e in self.dangling_e_entities.keys() {
      let e = &self.dangling_e_entities[dangling_e];
      if self.target_v_entities.contains_key(e.src_vid()) {
        self
          .target_v_adj_table
          .entry(e.src_vid().to_owned())
          .or_default()
          .e_out
          .insert(e.eid().to_owned());
      }
      if self.target_v_entities.contains_key(e.dst_vid()) {
        self
          .target_v_adj_table
          .entry(e.dst_vid().to_owned())
          .or_default()
          .e_in
          .insert(e.eid().to_owned());
      }
    }
    legal_vids
  }
}

pub fn union_then_intersect_on_connective_v<VType: VBase, EType: EBase>(
  left_expand_graph: &ExpandGraph<VType, EType>,
  right_expand_graph: &ExpandGraph<VType, EType>,
) -> Vec<ExpandGraph<VType, EType>> {
  let l_graph = &left_expand_graph.dyn_graph;
  let r_graph = &right_expand_graph.dyn_graph;

  let l_v_pat_pairs = l_graph.get_v_pattern_pairs();
  let r_v_pat_pairs = r_graph.get_v_pattern_pairs();
  let l_e_pat_pairs = l_graph.get_e_pattern_pairs();
  let r_e_pat_pairs = r_graph.get_e_pattern_pairs();

  let mut new_g = DynGraph::<VType, EType>::default();
  new_g.update_v_batch(l_v_pat_pairs.into_iter().chain(r_v_pat_pairs));
  new_g.update_e_batch(r_e_pat_pairs.into_iter().chain(l_e_pat_pairs));

  let grouped_l = left_expand_graph.group_dangling_e_by_pending_v();
  let grouped_r = right_expand_graph.group_dangling_e_by_pending_v();

  let mut result = vec![];

  for (l_pending_vid, l_dangling_es) in &grouped_l {
    for (r_pending_vid, r_dangling_es) in &grouped_r {
      if l_pending_vid != r_pending_vid {
        continue;
      }

      let mut expanding_dg: ExpandGraph<VType, EType> = new_g.as_ref().into();
      expanding_dg.update_valid_dangling_edges(l_dangling_es.clone().into_iter().map(|e| {
        (
          e.to_owned(),
          left_expand_graph.dangling_e_patterns[e.eid()].to_owned(),
        )
      }));
      expanding_dg.update_valid_dangling_edges(r_dangling_es.clone().into_iter().map(|e| {
        (
          e.to_owned(),
          right_expand_graph.dangling_e_patterns[e.eid()].to_owned(),
        )
      }));
      result.push(expanding_dg);
    }
  }

  result
}
