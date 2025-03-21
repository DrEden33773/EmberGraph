use ahash::{AHashMap, AHashSet};

use crate::schemas::{DataEdge, DataVertex, EdgeLike, Eid, VertexLike, Vid};

use super::dyn_graph::{DynGraph, VNode};

pub struct ExpandGraph<VType: VertexLike = DataVertex, EType: EdgeLike = DataEdge> {
  pub(crate) dyn_graph: DynGraph<VType, EType>,
  pub(crate) target_v_adj_table: AHashMap<Vid, VNode>,
  pub(crate) dangling_e_entities: AHashMap<Eid, EType>,
  pub(crate) target_v_entities: AHashMap<Vid, VType>,
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
    let mut dangling_e_grouped: AHashMap<String, Vec<EType>> = AHashMap::new();

    for dangling_e in self.dangling_e_entities.values() {
      if self.dyn_graph.has_vid(dangling_e.src_vid()) {
        dangling_e_grouped
          .entry(dangling_e.src_vid().clone())
          .or_default()
          .push(dangling_e.clone());
      } else if self.dyn_graph.has_vid(dangling_e.dst_vid()) {
        dangling_e_grouped
          .entry(dangling_e.dst_vid().clone())
          .or_default()
          .push(dangling_e.clone());
      }
    }

    dangling_e_grouped
  }
}
