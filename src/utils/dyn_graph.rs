use crate::schemas::{DataEdge, DataVertex, EdgeLike, Eid, EidRef, VertexLike, Vid, VidRef};
use ahash::{AHashMap, AHashSet};
use std::ops::{BitOr, BitOrAssign};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct VNode {
  pub(crate) e_in: AHashSet<Eid>,
  pub(crate) e_out: AHashSet<Eid>,
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
pub struct DynGraph<VType: VertexLike = DataVertex, EType: EdgeLike = DataEdge> {
  pub(crate) v_entities: AHashMap<Vid, VType>,
  pub(crate) e_entities: AHashMap<Eid, EType>,
  pub(crate) adj_table: AHashMap<Vid, VNode>,
}

impl<VType: VertexLike, EType: EdgeLike> Default for DynGraph<VType, EType> {
  fn default() -> Self {
    Self {
      v_entities: Default::default(),
      e_entities: Default::default(),
      adj_table: Default::default(),
    }
  }
}

impl<VType: VertexLike, EType: EdgeLike> BitOrAssign for DynGraph<VType, EType> {
  fn bitor_assign(&mut self, rhs: Self) {
    self.v_entities.extend(rhs.v_entities);
    self.e_entities.extend(rhs.e_entities);
    for (vid, v_node) in rhs.adj_table {
      self.adj_table.entry(vid).or_default().bitor_assign(v_node);
    }
  }
}

impl<VType: VertexLike, EType: EdgeLike> BitOr for DynGraph<VType, EType> {
  type Output = Self;

  fn bitor(self, rhs: Self) -> Self::Output {
    let mut v_entities = self.v_entities;
    let mut e_entities = self.e_entities;
    v_entities.extend(rhs.v_entities);
    e_entities.extend(rhs.e_entities);

    let mut res = DynGraph {
      v_entities,
      e_entities,
      ..Default::default()
    };

    for (vid, v_node) in self.adj_table {
      res.adj_table.insert(vid, v_node);
    }
    for (vid, v_node) in rhs.adj_table {
      res
        .adj_table
        .entry(vid.clone())
        .or_default()
        .bitor_assign(v_node);
    }
    res
  }
}

impl<VType: VertexLike, EType: EdgeLike> DynGraph<VType, EType> {
  pub fn is_subset_of(&self, other: &Self) -> bool {
    for (vid, v_node) in self.adj_table.iter() {
      // vertex
      if !other.adj_table.contains_key(vid) {
        return false;
      }
      // in-edge
      if !v_node.e_in.is_subset(&other.adj_table[vid].e_in) {
        return false;
      }
      // out-edge
      if !v_node.e_out.is_subset(&other.adj_table[vid].e_out) {
        return false;
      }
    }
    true
  }

  pub fn is_superset_of(&self, other: &Self) -> bool {
    other.is_subset_of(self)
  }
}

impl<VType: VertexLike, EType: EdgeLike> DynGraph<VType, EType> {
  pub fn update_v(&mut self, vertex: VType) -> &Self {
    let vid = vertex.vid().clone();
    self.v_entities.insert(vid.clone(), vertex);
    self.adj_table.insert(vid, VNode::default());
    self
  }

  pub fn update_v_batch(&mut self, vertices: Vec<VType>) -> &Self {
    for vertex in vertices {
      self.update_v(vertex);
    }
    self
  }

  pub fn update_e(&mut self, edge: EType) -> &Self {
    let eid = edge.eid().clone();
    let src_vid = edge.src_vid().clone();
    let dst_vid = edge.dst_vid().clone();

    self.e_entities.insert(eid.clone(), edge);

    if self.has_all_vids(&[&src_vid, &dst_vid]) {
      self
        .adj_table
        .entry(src_vid.clone())
        .or_default()
        .e_out
        .insert(eid.clone());
      self
        .adj_table
        .entry(dst_vid.clone())
        .or_default()
        .e_in
        .insert(eid.clone());
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

  pub fn update_e_batch(&mut self, edges: Vec<EType>) -> &Self {
    for edge in edges {
      self.update_e(edge);
    }
    self
  }

  pub fn remove_e(&mut self, eid: EidRef) -> &Self {
    if !self.has_eid(eid) {
      return self;
    }

    for v in self.adj_table.values_mut() {
      v.e_in.remove(eid);
      v.e_out.remove(eid);
    }
    self.e_entities.remove(eid);
    self
  }

  pub fn remove_e_batch(&mut self, eids: &[EidRef]) -> &Self {
    for eid in eids {
      self.remove_e(eid);
    }
    self
  }
}

impl<VType: VertexLike, EType: EdgeLike> DynGraph<VType, EType> {
  pub fn get_v_from_vid(&self, vid: VidRef) -> Option<&VType> {
    self.v_entities.get(vid)
  }
  pub fn get_e_from_eid(&self, eid: EidRef) -> Option<&EType> {
    self.e_entities.get(eid)
  }

  pub fn get_first_connective_vid_for_e(&self, edge: &EType) -> Option<Vid> {
    let src_vid = edge.src_vid();
    let dst_vid = edge.dst_vid();
    if self.has_vid(src_vid) {
      Some(src_vid.clone())
    } else if self.has_vid(dst_vid) {
      Some(dst_vid.clone())
    } else {
      None
    }
  }

  pub fn get_vid_set(&self) -> AHashSet<Vid> {
    self.v_entities.keys().cloned().collect()
  }
  pub fn get_eid_set(&self) -> AHashSet<Eid> {
    self.e_entities.keys().cloned().collect()
  }
  pub fn get_v_count(&self) -> usize {
    self.v_entities.len()
  }
  pub fn get_e_count(&self) -> usize {
    self.e_entities.len()
  }
}

impl<VType: VertexLike, EType: EdgeLike> DynGraph<VType, EType> {
  pub fn has_vid(&self, vid: VidRef) -> bool {
    self.v_entities.contains_key(vid)
  }
  pub fn has_all_vids(&self, vids: &[VidRef]) -> bool {
    vids.iter().all(|vid| self.has_vid(vid))
  }
  pub fn has_any_vids(&self, vids: &[VidRef]) -> bool {
    vids.iter().any(|vid| self.has_vid(vid))
  }

  pub fn has_eid(&self, eid: EidRef) -> bool {
    self.e_entities.contains_key(eid)
  }
  pub fn has_all_eids(&self, eids: &[EidRef]) -> bool {
    eids.iter().all(|eid| self.has_eid(eid))
  }
  pub fn has_any_eids(&self, eids: &[EidRef]) -> bool {
    eids.iter().any(|eid| self.has_eid(eid))
  }
}
