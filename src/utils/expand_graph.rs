use super::dyn_graph::{DynGraph, VNode};
use crate::schemas::*;
use hashbrown::HashMap;
use indexmap::IndexMap;
use std::{cmp::Ordering, sync::Arc};

#[derive(Debug, Clone)]
pub struct ExpandGraph<VType: VBase = DataVertex, EType: EBase = DataEdge> {
  pub(crate) dyn_graph: Arc<DynGraph<VType, EType>>,

  pub(crate) pending_v_grouped_dangling_eids: IndexMap<Vid, Vec<Eid>>,
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

  /// Sort the pending_v_grouped_dangling_eids by the key
  ///
  /// - Note that this function should only be called after `update_valid_target_vertices`
  pub fn sort_key_after_update_valid_target_vertices(&mut self) {
    self.pending_v_grouped_dangling_eids.sort_unstable_keys();
  }

  /// Update `valid target vertices` and return them
  ///
  /// - Vertices of any `dangling_edge` could be added to `target_v_adj_table`
  pub fn update_valid_target_vertices(
    &mut self,
    asc_target_vertex_pattern_pairs: &[(VType, String)],
  ) -> Vec<String> {
    // Skip if either collection is empty
    if asc_target_vertex_pattern_pairs.is_empty() || self.pending_v_grouped_dangling_eids.is_empty()
    {
      return vec![];
    }

    let mut legal_vids = vec![];

    // Pure two-pointer approach
    let mut pending_key_iter = self.pending_v_grouped_dangling_eids.keys();
    let mut pending_value_iter = self.pending_v_grouped_dangling_eids.values();
    let mut vertex_iter = asc_target_vertex_pattern_pairs.iter();

    // Initialize the current elements
    let mut curr_pending_key = pending_key_iter.next();
    let mut curr_pending_value = pending_value_iter.next();
    let mut curr_vertex_pair = vertex_iter.next();

    // Continue until one of the iterators is exhausted
    while let (Some(pending_vid), Some((vertex, pattern))) = (curr_pending_key, curr_vertex_pair) {
      // Skip if the vertex is already in the graph
      if self.dyn_graph.has_vid(vertex.vid()) {
        curr_vertex_pair = vertex_iter.next();
        continue;
      }

      // Compare the pending_vid and vertex.vid()
      match pending_vid.as_str().cmp(vertex.vid()) {
        Ordering::Equal => {
          // Found a match, process the vertex
          let dangling_eids = curr_pending_value.unwrap();

          // Add to adjacency table based on edge direction
          for dangling_eid in dangling_eids {
            let dangling_edge = &self.dangling_e_entities[dangling_eid];

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

          // Update other information
          legal_vids.push(vertex.vid().to_string());

          self
            .target_v_patterns
            .insert(vertex.vid().to_string(), pattern.clone());

          self
            .target_v_entities
            .insert(vertex.vid().to_string(), vertex.clone());

          // Move both pointers
          curr_pending_key = pending_key_iter.next();
          curr_pending_value = pending_value_iter.next();
          curr_vertex_pair = vertex_iter.next();
        }
        Ordering::Less => {
          // pending_vid < vertex.vid(), move pending pointer
          curr_pending_key = pending_key_iter.next();
          curr_pending_value = pending_value_iter.next();
        }
        Ordering::Greater => {
          // pending_vid > vertex.vid(), current vertex is not found in pending,
          // move to next vertex
          curr_vertex_pair = vertex_iter.next();
        }
      }
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

  // Create the basic graph structure (only create once)
  let new_graph = (*l_expand_graph.dyn_graph).clone() | (*r_expand_graph.dyn_graph).clone();
  let new_graph = Arc::new(new_graph);

  let mut result = vec![];

  // Iterate through two sorted maps using two pointers
  let mut l_key_iter = grouped_l.keys();
  let mut r_key_iter = grouped_r.keys();
  let mut l_value_iter = grouped_l.values();
  let mut r_value_iter = grouped_r.values();

  let mut l_key_current = l_key_iter.next();
  let mut r_key_current = r_key_iter.next();
  let mut l_value_current = l_value_iter.next();
  let mut r_value_current = r_value_iter.next();

  // Continue iterating as long as both iterators have elements
  while let (Some(l_vid), Some(r_vid)) = (l_key_current, r_key_current) {
    match l_vid.cmp(r_vid) {
      Ordering::Equal => {
        // Found a common vertex, process it directly
        let l_dangling_eids = l_value_current.unwrap();
        let r_dangling_eids = r_value_current.unwrap();
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
        expanding_dg.sort_key_after_update_valid_target_vertices();

        result.push(expanding_dg);

        // Move both pointers
        l_key_current = l_key_iter.next();
        r_key_current = r_key_iter.next();
        l_value_current = l_value_iter.next();
        r_value_current = r_value_iter.next();
      }
      Ordering::Less => {
        // l_vid < r_vid, move the left pointer
        l_key_current = l_key_iter.next();
        l_value_current = l_value_iter.next();
      }
      Ordering::Greater => {
        // l_vid > r_vid, move the right pointer
        r_key_current = r_key_iter.next();
        r_value_current = r_value_iter.next();
      }
    }
  }

  result
}
