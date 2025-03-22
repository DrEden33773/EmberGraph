use ahash::{AHashMap, AHashSet};

use crate::schemas::{DataEdge, DataVertex, EdgeLike, Eid, VertexLike, Vid};

use super::dyn_graph::{DynGraph, VNode};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpandGraph<VType: VertexLike = DataVertex, EType: EdgeLike = DataEdge> {
  pub(crate) dyn_graph: DynGraph<VType, EType>,
  pub(crate) target_v_adj_table: AHashMap<Vid, VNode>,
  pub(crate) dangling_e_entities: AHashMap<Eid, EType>,
  pub(crate) target_v_entities: AHashMap<Vid, VType>,
}

impl<VType: VertexLike, EType: EdgeLike> Default for ExpandGraph<VType, EType> {
  fn default() -> Self {
    Self {
      dyn_graph: Default::default(),
      target_v_adj_table: Default::default(),
      dangling_e_entities: Default::default(),
      target_v_entities: Default::default(),
    }
  }
}

impl<VType: VertexLike, EType: EdgeLike> From<&DynGraph<VType, EType>>
  for ExpandGraph<VType, EType>
{
  fn from(val: &DynGraph<VType, EType>) -> Self {
    Self {
      dyn_graph: val.clone(),
      ..Default::default()
    }
  }
}

impl<VType: VertexLike, EType: EdgeLike> From<&ExpandGraph<VType, EType>>
  for DynGraph<VType, EType>
{
  fn from(val: &ExpandGraph<VType, EType>) -> Self {
    let mut graph = val.dyn_graph.clone();

    graph.update_v_batch(val.target_v_entities.values().cloned());

    for target_v in val.target_v_adj_table.keys() {
      let mut dangling_eids = val
        .target_v_adj_table
        .get(target_v)
        .unwrap()
        .e_out
        .to_owned();
      dangling_eids.extend(val.target_v_adj_table.get(target_v).unwrap().e_in.clone());

      let dangling_es = dangling_eids
        .iter()
        .filter_map(|eid| val.dangling_e_entities.get(eid))
        .cloned()
        .collect::<Vec<EType>>();
      graph.update_e_batch(dangling_es);
    }

    graph
  }
}

impl<VType: VertexLike, EType: EdgeLike> ExpandGraph<VType, EType> {
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
}

impl<VType: VertexLike, EType: EdgeLike> ExpandGraph<VType, EType> {
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

  pub fn update_valid_dangling_edges(&mut self, dangling_edges: Vec<EType>) {
    let mut legal_es = AHashSet::new();
    let mut illegal_es = AHashSet::new();
    for edge in dangling_edges {
      if self.is_valid_edge(&edge) {
        legal_es.insert(edge.clone());
      } else {
        illegal_es.insert(edge.clone());
      }
    }

    self
      .dangling_e_entities
      .extend(legal_es.into_iter().map(|e| (e.eid().to_owned(), e)));
  }

  fn is_valid_target(&self, v: &VType) -> bool {
    for edge in self.dangling_e_entities.values() {
      if edge.contains(v.vid()) {
        return true;
      }
    }
    false
  }

  pub fn update_valid_target_vertices(&mut self, target_vertices: Vec<VType>) {
    let mut legal_vs = AHashSet::new();
    let mut illegal_vs = AHashSet::new();
    for vertex in target_vertices {
      if self.is_valid_target(&vertex) {
        legal_vs.insert(vertex.clone());
      } else {
        illegal_vs.insert(vertex.clone());
      }
    }

    self
      .target_v_entities
      .extend(legal_vs.into_iter().map(|v| (v.vid().to_owned(), v)));

    for dangling_e in self.dangling_e_entities.keys() {
      let e = self.dangling_e_entities.get(dangling_e).unwrap();
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
  }
}

pub fn intersect_then_union_on_same_v<VType: VertexLike, EType: EdgeLike>(
  potential_unused: &ExpandGraph<VType, EType>,
  potential_incomplete: &ExpandGraph<VType, EType>,
) -> Vec<ExpandGraph<VType, EType>> {
  let mut result = Vec::new();

  let mut unused = potential_unused;
  let mut incomplete = potential_incomplete;
  let unused_set = unused.get_vid_set();
  let incomplete_set = incomplete.get_vid_set();

  if !unused_set.is_subset(&incomplete_set) {
    if !incomplete_set.is_subset(&unused_set) {
      return union_then_intersect_on_connective_v(unused, incomplete);
    }
    (unused, incomplete) = (incomplete, unused);
  }

  let grouped_incomplete = incomplete.group_dangling_e_by_pending_v();
  let grouped_unused = unused.group_dangling_e_by_pending_v();

  for (pending_vid, dangling_es) in &grouped_unused {
    for expected_vid in grouped_incomplete.keys() {
      if pending_vid != expected_vid {
        continue;
      }

      let mut expanding_dg = incomplete.clone();
      expanding_dg.update_valid_dangling_edges(dangling_es.clone());
      result.push(expanding_dg);
    }
  }

  result
}

pub fn union_then_intersect_on_connective_v<VType: VertexLike, EType: EdgeLike>(
  left_expand_graph: &ExpandGraph<VType, EType>,
  right_expand_graph: &ExpandGraph<VType, EType>,
) -> Vec<ExpandGraph<VType, EType>> {
  let left_graph = &left_expand_graph.dyn_graph;
  let right_graph = &right_expand_graph.dyn_graph;

  if !left_graph
    .get_vid_set()
    .is_disjoint(&right_graph.get_vid_set())
  {
    return intersect_then_union_on_same_v(left_expand_graph, right_expand_graph);
  }

  let left_vs = left_graph.get_v_entities();
  let right_vs = right_graph.get_v_entities();
  let left_es = left_graph.get_e_entities();
  let right_es = right_graph.get_e_entities();

  let mut new_graph = DynGraph::<VType, EType>::default();
  new_graph.update_v_batch(left_vs.into_iter().chain(right_vs));
  new_graph.update_e_batch(right_es.into_iter().chain(left_es));

  let grouped_l = left_expand_graph.group_dangling_e_by_pending_v();
  let grouped_r = right_expand_graph.group_dangling_e_by_pending_v();
  let mut result = vec![];

  for (l_pending_vid, l_dangling_es) in &grouped_l {
    for (r_pending_vid, r_dangling_es) in &grouped_r {
      if l_pending_vid != r_pending_vid {
        continue;
      }

      let mut expanding_dg: ExpandGraph<VType, EType> = new_graph.as_ref().into();
      expanding_dg.update_valid_dangling_edges(l_dangling_es.clone());
      expanding_dg.update_valid_dangling_edges(r_dangling_es.clone());
      result.push(expanding_dg);
    }
  }

  result
}
