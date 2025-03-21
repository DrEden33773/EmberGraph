pub mod attr;
pub mod base;
pub mod instruction;

use ahash::AHashMap;
use std::hash::Hash;

pub use {attr::*, base::*, instruction::*};

pub trait VertexLike<T = Self>: Clone + AsRef<T> + Hash + PartialEq + Eq {
  fn vid(&self) -> &Vid;
  fn label(&self) -> &Label;
}

pub trait EdgeLike<T = Self>: Clone + AsRef<T> + Hash + PartialEq + Eq {
  fn eid(&self) -> &Vid;
  fn src_vid(&self) -> &Vid;
  fn dst_vid(&self) -> &Vid;
  fn label(&self) -> &Label;
  fn contains(&self, vid: &Vid) -> bool {
    self.src_vid() == vid || self.dst_vid() == vid
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatternVertex {
  pub(crate) vid: Vid,
  pub(crate) label: Label,
  pub(crate) attr: Option<PatternAttr>,
}

impl AsRef<Self> for PatternVertex {
  fn as_ref(&self) -> &Self {
    self
  }
}

impl VertexLike for PatternVertex {
  fn vid(&self) -> &Vid {
    &self.vid
  }
  fn label(&self) -> &Label {
    &self.label
  }
}

impl Hash for PatternVertex {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.vid.hash(state);
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataVertex {
  pub(crate) vid: Vid,
  pub(crate) label: Label,
  pub(crate) attrs: AHashMap<String, AttrValue>,
}

impl Hash for DataVertex {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.vid.hash(state);
  }
}

impl AsRef<Self> for DataVertex {
  fn as_ref(&self) -> &Self {
    self
  }
}

impl VertexLike for DataVertex {
  fn vid(&self) -> &Vid {
    &self.vid
  }
  fn label(&self) -> &Label {
    &self.label
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatternEdge {
  pub(crate) eid: Vid,
  pub(crate) src_vid: Vid,
  pub(crate) dst_vid: Vid,
  pub(crate) label: Label,
  pub(crate) attr: Option<PatternAttr>,
}

impl Hash for PatternEdge {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.eid.hash(state);
  }
}

impl AsRef<Self> for PatternEdge {
  fn as_ref(&self) -> &Self {
    self
  }
}

impl EdgeLike for PatternEdge {
  fn eid(&self) -> &Vid {
    &self.eid
  }
  fn src_vid(&self) -> &Vid {
    &self.src_vid
  }
  fn dst_vid(&self) -> &Vid {
    &self.dst_vid
  }
  fn label(&self) -> &Label {
    &self.label
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataEdge {
  pub(crate) eid: Vid,
  pub(crate) src_vid: Vid,
  pub(crate) dst_vid: Vid,
  pub(crate) label: Label,
  pub(crate) attrs: AHashMap<String, AttrValue>,
}

impl Hash for DataEdge {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.eid.hash(state);
  }
}

impl AsRef<Self> for DataEdge {
  fn as_ref(&self) -> &Self {
    self
  }
}

impl EdgeLike for DataEdge {
  fn eid(&self) -> &Vid {
    &self.eid
  }
  fn src_vid(&self) -> &Vid {
    &self.src_vid
  }
  fn dst_vid(&self) -> &Vid {
    &self.dst_vid
  }
  fn label(&self) -> &Label {
    &self.label
  }
}
