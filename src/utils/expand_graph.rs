use super::dyn_graph::DynGraph;
use crate::{schemas::*, storage::StorageAdapter};
use colored::Colorize;
#[cfg(not(feature = "use_tokio_mpsc_unbounded_channel"))]
use futures::TryFutureExt;
use hashbrown::HashMap;
use indexmap::IndexMap;
#[cfg(feature = "use_sort_merge_join")]
use std::cmp::Ordering;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ExpandGraph<VType: VBase = DataVertex, EType: EBase = DataEdge> {
  pub(crate) dyn_graph: Arc<DynGraph<VType, EType>>,

  pub(crate) pending_v_grouped_dangling_eids: IndexMap<Vid, Vec<Eid>>,

  pub(crate) target_vs: Vec<Vid>,

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
      target_vs: Default::default(),
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

    for target_v in val.target_vs {
      let dangling_eids = val.pending_v_grouped_dangling_eids.get(&target_v).unwrap();

      let dangling_e_pattern_pairs = dangling_eids
        .iter()
        .filter_map(|eid| val.dangling_e_entities.remove(eid))
        .map(|e| {
          let pattern = val.dangling_e_patterns.remove(e.eid()).unwrap();
          (e, pattern)
        });
      graph.update_e_batch(dangling_e_pattern_pairs);
    }

    graph
  }
}

impl ExpandGraph<DataVertex, DataEdge> {
  pub async fn lazy_intersect_valid_target_vertices(
    &mut self,
    pattern_str: impl AsRef<str>,
    expected_label: Arc<str>,
    expected_attr: Option<Arc<PatternAttr>>,
    storage_adapter: Arc<impl StorageAdapter + 'static>,
  ) -> Vec<String> {
    if self.pending_v_grouped_dangling_eids.is_empty() {
      return vec![];
    }

    let len = self.pending_v_grouped_dangling_eids.len();

    self.target_vs.reserve(len);
    let mut legal_vids = Vec::with_capacity(len);

    // channel
    #[cfg(feature = "use_tokio_mpsc_unbounded_channel")]
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    #[cfg(not(feature = "use_tokio_mpsc_unbounded_channel"))]
    let (tx, mut rx) = tokio::sync::mpsc::channel(len + 4);

    for (pending_vid, _) in self.pending_v_grouped_dangling_eids.iter() {
      let tx = tx.clone();
      let pending_vid = pending_vid.clone();
      let storage_adapter = storage_adapter.clone();
      let expected_label = expected_label.clone();
      let expected_attr = expected_attr.clone();

      tokio::spawn(async move {
        let pending_v = storage_adapter.get_v(&pending_vid).await;
        if let Some(pending_v) = pending_v {
          if pending_v.label() != expected_label.as_ref()
            || !pending_v.satisfy_attr(expected_attr.as_ref())
          {
            return;
          }

          #[cfg(feature = "use_tokio_mpsc_unbounded_channel")]
          tx.send(pending_v).unwrap_or_else(|_| {
            panic!(
              "❌  Failed to send {} to channel",
              format!("({}, <pattern_v>)", pending_vid).yellow()
            );
          });
          #[cfg(not(feature = "use_tokio_mpsc_unbounded_channel"))]
          tx.send(pending_v)
            .unwrap_or_else(|_| {
              panic!(
                "❌  Failed to send {} to channel",
                format!("({}, <pattern_v>)", pending_vid).yellow()
              );
            })
            .await;
        }
      });
    }

    // don't forget to close the channel
    drop(tx);

    while let Some(vertex) = rx.recv().await {
      self.target_vs.push(vertex.vid().to_string());
      legal_vids.push(vertex.vid().to_string());
      self
        .target_v_patterns
        .insert(vertex.vid().to_string(), pattern_str.as_ref().to_string());
      self
        .target_v_entities
        .insert(vertex.vid().to_string(), vertex);
    }

    legal_vids
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

  #[cfg(feature = "use_sort_merge_join")]
  /// Sort the pending_v_grouped_dangling_eids by the key
  ///
  /// - Note that this function should only be called after `update_valid_dangling_edges`
  pub fn sort_key_after_update_dangling_edges(&mut self) {
    self.pending_v_grouped_dangling_eids.sort_unstable_keys();
  }

  #[cfg(feature = "use_sort_merge_join")]
  /// Intersect `valid target vertices` and return them (sort-merge-join version)
  ///
  /// - Vertices of any `dangling_edge` could be added to `target_v_adj_table`
  pub fn intersect_valid_target_vertices(
    &mut self,
    asc_target_vertex_pattern_pairs: &[(VType, String)],
  ) -> Vec<String> {
    // Skip if either collection is empty
    if asc_target_vertex_pattern_pairs.is_empty() || self.pending_v_grouped_dangling_eids.is_empty()
    {
      return vec![];
    }

    let pending_v_grouped_dangling_eids = self.pending_v_grouped_dangling_eids.as_slice();

    // Use index-based approach instead of iterators for better debug mode performance
    let pending_len = self.pending_v_grouped_dangling_eids.len();
    let vertex_len = asc_target_vertex_pattern_pairs.len();
    self.target_vs.reserve(pending_len.min(vertex_len));

    let mut legal_vids = Vec::with_capacity(vertex_len.min(pending_len));

    let mut pending_idx = 0;
    let mut vertex_idx = 0;

    // Continue until one of the collections is exhausted
    while pending_idx < pending_len && vertex_idx < vertex_len {
      // Get current elements by index
      let (pending_vid, _) = pending_v_grouped_dangling_eids
        .get_index(pending_idx)
        .unwrap();
      let (vertex, pattern) = &asc_target_vertex_pattern_pairs[vertex_idx];

      // Skip if the vertex is already in the graph
      if self.dyn_graph.has_vid(vertex.vid()) {
        vertex_idx += 1;
        continue;
      }

      // Compare the pending_vid and vertex.vid()
      match pending_vid.as_str().cmp(vertex.vid()) {
        Ordering::Equal => {
          // Found a match, add the vertex to the `target_vs`
          self.target_vs.push(vertex.vid().to_string());

          // Update other information
          legal_vids.push(vertex.vid().to_string());

          self
            .target_v_patterns
            .insert(vertex.vid().to_string(), pattern.clone());

          self
            .target_v_entities
            .insert(vertex.vid().to_string(), vertex.clone());

          // Move both indices
          pending_idx += 1;
          vertex_idx += 1;
        }
        Ordering::Less => {
          // pending_vid < vertex.vid(), move pending index
          pending_idx += 1;
        }
        Ordering::Greater => {
          // pending_vid > vertex.vid(), current vertex is not found in pending,
          // move to next vertex
          vertex_idx += 1;
        }
      }
    }

    legal_vids
  }

  #[cfg(not(feature = "use_sort_merge_join"))]
  /// Intersect `valid target vertices` and return them (hash-join version)
  ///
  /// - Vertices of any `dangling_edge` could be added to `target_v_adj_table`
  pub fn intersect_valid_target_vertices(
    &mut self,
    target_vertex_pattern_pairs: &[(VType, String)],
  ) -> Vec<String> {
    if target_vertex_pattern_pairs.is_empty() || self.pending_v_grouped_dangling_eids.is_empty() {
      return vec![];
    }

    let pending_v_grouped_dangling_eids = &self.pending_v_grouped_dangling_eids;

    let target_vid_2_vertex_pattern_pairs = target_vertex_pattern_pairs
      .iter()
      .map(|(v, p)| (v.vid(), (v, p)))
      .collect::<IndexMap<_, _>>();

    let mut legal_vids = Vec::with_capacity(
      pending_v_grouped_dangling_eids
        .len()
        .min(target_vid_2_vertex_pattern_pairs.len()),
    );

    if pending_v_grouped_dangling_eids.len() <= target_vid_2_vertex_pattern_pairs.len() {
      for (pending_vid, _) in pending_v_grouped_dangling_eids {
        if let Some((vertex, pattern)) = target_vid_2_vertex_pattern_pairs
          .get(pending_vid.as_str())
          .cloned()
        {
          if self.dyn_graph.has_vid(vertex.vid()) {
            continue;
          }

          self.target_vs.push(vertex.vid().to_string());
          legal_vids.push(vertex.vid().to_string());

          self
            .target_v_patterns
            .insert(vertex.vid().to_string(), pattern.clone());

          self
            .target_v_entities
            .insert(vertex.vid().to_string(), vertex.clone());
        }
      }
    } else {
      for (vid, (vertex, pattern)) in target_vid_2_vertex_pattern_pairs {
        if pending_v_grouped_dangling_eids.contains_key(vid) {
          if self.dyn_graph.has_vid(vid) {
            continue;
          }

          self.target_vs.push(vid.to_string());
          legal_vids.push(vid.to_string());

          self
            .target_v_patterns
            .insert(vid.to_string(), pattern.clone());

          self
            .target_v_entities
            .insert(vid.to_string(), vertex.clone());
        }
      }
    }

    legal_vids
  }

  /// Check if two ExpandGraph have common pending vertices.
  ///
  /// This is the performance critical path, need to be implemented efficiently.
  #[inline]
  pub fn has_common_pending_v(&self, other: &Self) -> bool {
    let (shorter, longer) =
      if self.pending_v_grouped_dangling_eids.len() < other.pending_v_grouped_dangling_eids.len() {
        (self, other)
      } else {
        (other, self)
      };

    // for small-scale collections, directly iterate and compare
    if shorter.pending_v_grouped_dangling_eids.len() <= 4 {
      for (vid, _) in &shorter.pending_v_grouped_dangling_eids {
        if longer.pending_v_grouped_dangling_eids.contains_key(vid) {
          return true;
        }
      }
      return false;
    }

    // for medium-scale collections, use binary search to accelerate comparison
    if shorter.pending_v_grouped_dangling_eids.len() <= 16
      && longer.pending_v_grouped_dangling_eids.len() <= 32
    {
      // collect keys of the longer collection
      let keys: Vec<_> = longer.pending_v_grouped_dangling_eids.keys().collect();

      // binary search each key of the shorter collection
      for (vid, _) in &shorter.pending_v_grouped_dangling_eids {
        if keys.binary_search(&vid).is_ok() {
          return true;
        }
      }
      return false;
    }

    // normal case, use standard iteration
    shorter
      .pending_v_grouped_dangling_eids
      .iter()
      .any(|(vid, _)| longer.pending_v_grouped_dangling_eids.contains_key(vid))
  }

  /// High-performance version of common point comparison, optimized for large data sets.
  ///
  /// Use mask to filter out impossible matches in advance.
  #[inline]
  pub fn has_common_pending_v_optimized(&self, other: &Self) -> bool {
    // if one of the collections is empty, return false directly
    if self.pending_v_grouped_dangling_eids.is_empty()
      || other.pending_v_grouped_dangling_eids.is_empty()
    {
      return false;
    }

    // create character masks: record the occurrence of the first character of each vid
    let mut self_mask = [false; 256];
    let mut other_mask = [false; 256];

    // fill the masks: record the occurrence of the first character of each vid
    for (vid, _) in &self.pending_v_grouped_dangling_eids {
      if let Some(first_byte) = vid.bytes().next() {
        self_mask[first_byte as usize] = true;
      }
    }

    // check the other collection and return early
    for (vid, _) in &other.pending_v_grouped_dangling_eids {
      if let Some(first_byte) = vid.bytes().next() {
        // if the current element's mask bit is already set in self_mask, there may be an intersection
        if self_mask[first_byte as usize] {
          // set other_mask, and perform actual comparison
          other_mask[first_byte as usize] = true;

          // directly check if there is an intersection
          for (vid, _) in &self.pending_v_grouped_dangling_eids {
            if let Some(first_byte_self) = vid.bytes().next() {
              // only check the bucket that matches the mask
              if other_mask[first_byte_self as usize]
                && other.pending_v_grouped_dangling_eids.contains_key(vid)
              {
                return true;
              }
            }
          }
        }
      }
    }

    false
  }
}

#[cfg(feature = "use_sort_merge_join")]
/// 1. Take two expand_graphs' `vertices` and `non-dangling-edges` into a new graph
/// 2. Iterate through the `dangling_edges` of both, select those connective ones
///
/// (Sort-Merge-Join version)
pub fn union_then_intersect_on_connective_v<VType: VBase, EType: EBase>(
  l_expand_graph: &ExpandGraph<VType, EType>,
  r_expand_graph: &ExpandGraph<VType, EType>,
) -> Vec<ExpandGraph<VType, EType>> {
  let grouped_l = l_expand_graph.pending_v_grouped_dangling_eids.as_slice();
  let grouped_r = r_expand_graph.pending_v_grouped_dangling_eids.as_slice();

  if grouped_l.is_empty() || grouped_r.is_empty() {
    return vec![];
  }

  // Create the basic graph structure (only create once)
  let new_graph = (*l_expand_graph.dyn_graph).clone() | (*r_expand_graph.dyn_graph).clone();
  let new_graph = Arc::new(new_graph);
  // Use index-based approach instead of iterators for better debug mode performance
  let l_len = grouped_l.len();
  let r_len = grouped_r.len();

  let mut result = Vec::with_capacity(l_len.min(r_len));

  let mut l_idx = 0;
  let mut r_idx = 0;

  // Continue iterating as long as both collections have unprocessed elements
  while l_idx < l_len && r_idx < r_len {
    // Get current elements by index
    let (l_vid, l_dangling_eids) = grouped_l.get_index(l_idx).unwrap();
    let (r_vid, r_dangling_eids) = grouped_r.get_index(r_idx).unwrap();

    match l_vid.cmp(r_vid) {
      Ordering::Equal => {
        // Found a common vertex, process it directly
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
        expanding_dg.sort_key_after_update_dangling_edges();

        result.push(expanding_dg);

        // Move both indices
        l_idx += 1;
        r_idx += 1;
      }
      Ordering::Less => {
        // l_vid < r_vid, move the left index
        l_idx += 1;
      }
      Ordering::Greater => {
        // l_vid > r_vid, move the right index
        r_idx += 1;
      }
    }
  }

  result
}

#[cfg(not(feature = "use_sort_merge_join"))]
/// 1. Take two expand_graphs' `vertices` and `non-dangling-edges` into a new graph
/// 2. Iterate through the `dangling_edges` of both, select those connective ones
///
/// (Hash-Join version)
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

  let (shorter, longer) = if grouped_l.len() < grouped_r.len() {
    (grouped_l, grouped_r)
  } else {
    (grouped_r, grouped_l)
  };

  let mut result = Vec::with_capacity(shorter.len());

  for (vid, _) in shorter {
    if longer.contains_key(vid) {
      let mut expanding_dg: ExpandGraph<VType, EType> = new_graph.clone().into();

      let l_dangling_eids = grouped_l.get(vid).unwrap();
      let r_dangling_eids = grouped_r.get(vid).unwrap();

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
  }

  result
}
