use std::sync::Arc;

use super::dyn_graph::{DynGraph, VNode};
use crate::schemas::*;
use hashbrown::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpandGraph<VType: VBase = DataVertex, EType: EBase = DataEdge> {
  pub(crate) dyn_graph: Arc<DynGraph<VType, EType>>,
  pub(crate) target_v_adj_table: HashMap<Vid, VNode>,

  pub(crate) dangling_e_entities: HashMap<Eid, EType>,
  pub(crate) target_v_entities: HashMap<Vid, VType>,

  pub(crate) dangling_e_patterns: HashMap<Eid, String>,
  pub(crate) target_v_patterns: HashMap<Vid, String>,
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

impl<VType: VBase, EType: EBase> From<Arc<DynGraph<VType, EType>>> for ExpandGraph<VType, EType> {
  fn from(dyn_graph: Arc<DynGraph<VType, EType>>) -> Self {
    Self {
      dyn_graph,
      ..Default::default()
    }
  }
}

impl<VType: VBase, EType: EBase> From<ExpandGraph<VType, EType>> for DynGraph<VType, EType> {
  fn from(mut val: ExpandGraph<VType, EType>) -> Self {
    let mut graph = (*val.dyn_graph).clone();

    graph.update_v_batch(val.target_v_entities.into_values().map(|v| {
      let pattern = val.target_v_patterns.remove(v.vid()).unwrap();
      (v, pattern)
    }));

    for target_v in val.target_v_adj_table.keys() {
      let mut dangling_eids = val.target_v_adj_table[target_v].e_out.clone();
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
  pub fn get_vids(&self) -> Vec<VidRef<'_>> {
    self.dyn_graph.view_vids()
  }
  pub fn get_eids(&self) -> Vec<EidRef<'_>> {
    self.dyn_graph.view_eids()
  }
  pub fn get_v_count(&self) -> usize {
    self.dyn_graph.get_v_count()
  }
  pub fn get_e_count(&self) -> usize {
    self.dyn_graph.get_e_count()
  }
  pub fn dangling_e_patterns(&self) -> &HashMap<Eid, String> {
    &self.dangling_e_patterns
  }
  pub fn target_v_patterns(&self) -> &HashMap<Vid, String> {
    &self.target_v_patterns
  }
}

impl<VType: VBase, EType: EBase> ExpandGraph<VType, EType> {
  /// Group dangling edges by their pending(unconnected) vertices.
  pub fn group_dangling_e_by_pending_v(&self) -> HashMap<String, Vec<EType>> {
    let mut grouped: HashMap<String, Vec<EType>> = HashMap::new();

    for dangling_e in self.dangling_e_entities.values() {
      if self.dyn_graph.has_vid(dangling_e.src_vid()) {
        grouped
          .entry(dangling_e.dst_vid().to_string())
          .or_default()
          .push(dangling_e.clone());
      } else if self.dyn_graph.has_vid(dangling_e.dst_vid()) {
        grouped
          .entry(dangling_e.src_vid().to_string())
          .or_default()
          .push(dangling_e.clone());
      }
    }

    grouped
  }

  /// Check if the edge is a valid dangling edge
  fn is_valid_dangling_edge(&self, e: &EType) -> bool {
    !self.dyn_graph.has_eid(e.eid())
      && !self.dyn_graph.is_e_full_connective(e)
      && self.dyn_graph.is_e_connective(e)
  }

  /// Update valid dangling edges and return them
  pub fn update_valid_dangling_edges<'a>(
    &'a mut self,
    dangling_edge_pattern_pairs: impl IntoIterator<Item = (&'a EType, &'a str)>,
  ) {
    for (edge, pattern) in dangling_edge_pattern_pairs {
      if !self.is_valid_dangling_edge(edge) {
        continue;
      }
      self
        .dangling_e_patterns
        .insert(edge.eid().to_string(), pattern.to_string());
      self
        .dangling_e_entities
        .insert(edge.eid().to_string(), edge.clone());
    }
  }

  /// Check if the vertex is a valid target vertex
  fn is_valid_target(&self, v: &VType) -> bool {
    for dangling_edge in self.dangling_e_entities.values() {
      if dangling_edge.contains(v.vid()) && !self.dyn_graph.has_vid(v.vid()) {
        return true;
      }
    }
    false
  }

  /// Update valid target vertices and return them
  ///
  /// - Vertices of any `dangling_edge` could be added to `target_v_adj_table`
  pub fn update_valid_target_vertices(
    &mut self,
    target_vertex_pattern_pairs: impl AsRef<Vec<(VType, String)>>,
  ) -> HashSet<String> {
    let mut legal_vids = HashSet::new();
    for (vertex, pattern) in target_vertex_pattern_pairs.as_ref().iter() {
      if !self.is_valid_target(vertex) {
        continue;
      }
      legal_vids.insert(vertex.vid().to_string());
      self
        .target_v_patterns
        .insert(vertex.vid().to_string(), pattern.to_string());
      self
        .target_v_entities
        .insert(vertex.vid().to_string(), vertex.clone());
    }

    for dangling_e in self.dangling_e_entities.keys() {
      let e = &self.dangling_e_entities[dangling_e];
      if self.target_v_entities.contains_key(e.src_vid()) {
        self
          .target_v_adj_table
          .entry(e.src_vid().to_string())
          .or_default()
          .e_out
          .insert(e.eid().to_string());
      }
      if self.target_v_entities.contains_key(e.dst_vid()) {
        self
          .target_v_adj_table
          .entry(e.dst_vid().to_string())
          .or_default()
          .e_in
          .insert(e.eid().to_string());
      }
    }

    legal_vids
  }
}

// TODO: Parallelize this function
/// 1. Take two expand_graphs' `vertices` and `non-dangling-edges` into a new graph
/// 2. Iterate through the `dangling_edges` of both, select those connective ones
pub fn union_then_intersect_on_connective_v<VType: VBase, EType: EBase>(
  l_expand_graph: ExpandGraph<VType, EType>,
  r_expand_graph: ExpandGraph<VType, EType>,
) -> Vec<ExpandGraph<VType, EType>> {
  let grouped_l = l_expand_graph.group_dangling_e_by_pending_v();
  let grouped_r = r_expand_graph.group_dangling_e_by_pending_v();

  let l_graph = l_expand_graph.dyn_graph;
  let r_graph = r_expand_graph.dyn_graph;

  #[cfg(feature = "validate_pattern_uniqueness_before_final_merge")]
  {
    // For each pair of common_v/e_pats, they should lead to the same vs/es in both graphs.
    // If not, we should not consider them as a match.
    // We could also discard those patterns who lead to `multi` vs/es in either graph.

    for common_v_pat in l_graph.view_common_v_patterns(&r_graph) {
      let l_vs = l_graph.pattern_2_vids.get(common_v_pat).unwrap();
      let r_vs = r_graph.pattern_2_vids.get(common_v_pat).unwrap();
      if l_vs.len() > 1 || r_vs.len() > 1 || l_vs != r_vs {
        return vec![];
      }
    }

    for common_e_pat in l_graph.view_common_e_patterns(&r_graph) {
      let l_es = l_graph.pattern_2_eids.get(common_e_pat).unwrap();
      let r_es = r_graph.pattern_2_eids.get(common_e_pat).unwrap();
      if l_es.len() > 1 || r_es.len() > 1 || l_es != r_es {
        return vec![];
      }
    }
  }

  let l_v_pat_pairs = l_graph.get_v_pattern_pairs_cloned();
  let r_v_pat_pairs = r_graph.get_v_pattern_pairs_cloned();
  let l_e_pat_pairs = l_graph.get_e_pattern_pairs_cloned();
  let r_e_pat_pairs = r_graph.get_e_pattern_pairs_cloned();

  let mut new_graph = DynGraph::<VType, EType>::default();
  new_graph.update_v_batch(l_v_pat_pairs.into_iter().chain(r_v_pat_pairs));
  new_graph.update_e_batch(l_e_pat_pairs.into_iter().chain(r_e_pat_pairs));
  let new_graph = Arc::new(new_graph);

  let mut result = vec![];

  for (l_pending_vid, l_dangling_es) in &grouped_l {
    for (r_pending_vid, r_dangling_es) in &grouped_r {
      if l_pending_vid != r_pending_vid {
        continue;
      }

      let mut expanding_dg: ExpandGraph<VType, EType> = new_graph.clone().into();
      expanding_dg.update_valid_dangling_edges(
        l_dangling_es
          .iter()
          .map(|e| (e, l_expand_graph.dangling_e_patterns[e.eid()].as_str())),
      );
      expanding_dg.update_valid_dangling_edges(
        r_dangling_es
          .iter()
          .map(|e| (e, r_expand_graph.dangling_e_patterns[e.eid()].as_str())),
      );

      result.push(expanding_dg);
    }
  }

  result
}
