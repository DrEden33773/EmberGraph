use std::sync::Arc;

use super::dyn_graph::{DynGraph, VNode};
use crate::schemas::*;
use hashbrown::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpandGraph<VType: VBase = DataVertex, EType: EBase = DataEdge> {
  pub(crate) dyn_graph: Arc<DynGraph<VType, EType>>,

  pub(crate) pending_v_grouped_dangling_eids: HashMap<Vid, Vec<Eid>>,
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
      pending_v_grouped_dangling_eids: Default::default(),
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
  /// Update valid dangling edges and return them
  pub fn update_valid_dangling_edges<'a>(
    &'a mut self,
    dangling_edge_pattern_pairs: impl IntoIterator<Item = (&'a EType, &'a str)>,
  ) {
    for (edge, pattern) in dangling_edge_pattern_pairs {
      if self.dyn_graph.has_eid(edge.eid()) {
        continue;
      }

      match self.dyn_graph.pick_e_connective_vid(edge) {
        (Some(_), Some(_)) | (None, None) => continue,
        (Some(_src_vid), None) => {
          // `src_vid` is connected, `dst_vid` is pending
          self
            .pending_v_grouped_dangling_eids
            .entry(edge.dst_vid().to_string())
            .or_default()
            .push(edge.eid().to_string());
        }
        (None, Some(_dst_vid)) => {
          // `dst_vid` is connected, `src_vid` is pending
          self
            .pending_v_grouped_dangling_eids
            .entry(edge.src_vid().to_string())
            .or_default()
            .push(edge.eid().to_string());
        }
      }

      self
        .dangling_e_patterns
        .insert(edge.eid().to_string(), pattern.to_string());

      self
        .dangling_e_entities
        .insert(edge.eid().to_string(), edge.clone());
    }
  }

  /// Update `valid target vertices` and return them
  ///
  /// - Vertices of any `dangling_edge` could be added to `target_v_adj_table`
  pub fn update_valid_target_vertices(
    &mut self,
    target_vertex_pattern_pairs: &[(VType, String)],
  ) -> Vec<String> {
    let mut legal_vids = Vec::with_capacity(target_vertex_pattern_pairs.as_ref().len());

    for (vertex, pattern) in target_vertex_pattern_pairs.as_ref().iter() {
      if self.dyn_graph.has_vid(vertex.vid()) {
        continue;
      }

      match self.pending_v_grouped_dangling_eids.get(vertex.vid()) {
        Some(dangling_eids) => {
          // If the vertex is a valid target, we need to add it to the target_v_adj_table
          for dangling_eid in dangling_eids {
            let dangling_edge = &self.dangling_e_entities[dangling_eid];

            // pick `e_out` / `e_in` by the direction of the edge
            if dangling_edge.src_vid() == vertex.vid() {
              // (vertex)-[dangling_edge]->...
              self
                .target_v_adj_table
                .entry(vertex.vid().to_string())
                .or_default()
                .e_out
                .insert(dangling_edge.eid().to_string());
            } else if dangling_edge.dst_vid() == vertex.vid() {
              // (vertex)<-[dangling_edge]-...
              self
                .target_v_adj_table
                .entry(vertex.vid().to_string())
                .or_default()
                .e_in
                .insert(dangling_edge.eid().to_string());
            }
          }
        }
        None => continue,
      }

      // Don't forget to update other information

      legal_vids.push(vertex.vid().to_string());

      self
        .target_v_patterns
        .insert(vertex.vid().to_string(), pattern.clone());

      self
        .target_v_entities
        .insert(vertex.vid().to_string(), vertex.clone());
    }

    legal_vids
  }
}

/// 1. Take two expand_graphs' `vertices` and `non-dangling-edges` into a new graph
/// 2. Iterate through the `dangling_edges` of both, select those connective ones
pub fn union_then_intersect_on_connective_v<VType: VBase, EType: EBase>(
  l_expand_graph: &ExpandGraph<VType, EType>,
  r_expand_graph: &ExpandGraph<VType, EType>,
) -> Vec<ExpandGraph<VType, EType>> {
  let grouped_l = &l_expand_graph.pending_v_grouped_dangling_eids;
  let grouped_r = &r_expand_graph.pending_v_grouped_dangling_eids;

  if grouped_l.is_empty() || grouped_r.is_empty() {
    return vec![];
  }

  let mut common_pending_vids = HashSet::with_capacity(grouped_l.len().min(grouped_r.len()));

  for pending_vid in grouped_l.keys() {
    if grouped_r.contains_key(pending_vid) {
      common_pending_vids.insert(pending_vid.clone());
    }
  }

  if common_pending_vids.is_empty() {
    return vec![];
  }

  let new_graph = (*l_expand_graph.dyn_graph).clone() | (*r_expand_graph.dyn_graph).clone();
  let new_graph = Arc::new(new_graph);

  let mut result = Vec::with_capacity(common_pending_vids.len());

  for pending_vid in common_pending_vids.iter() {
    let l_dangling_eids = grouped_l.get(pending_vid).unwrap();
    let r_dangling_eids = grouped_r.get(pending_vid).unwrap();

    let mut expanding_dg: ExpandGraph<VType, EType> = new_graph.clone().into();

    expanding_dg.update_valid_dangling_edges(l_dangling_eids.iter().map(|eid| {
      (
        &l_expand_graph.dangling_e_entities[eid],
        l_expand_graph.dangling_e_patterns[eid].as_str(),
      )
    }));

    expanding_dg.update_valid_dangling_edges(r_dangling_eids.iter().map(|eid| {
      (
        &r_expand_graph.dangling_e_entities[eid],
        r_expand_graph.dangling_e_patterns[eid].as_str(),
      )
    }));

    result.push(expanding_dg);
  }

  result
}
