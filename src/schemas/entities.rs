use super::{AttrValue, Eid, Label, LabelRef, PatternAttr, Vid, VidRef};
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use std::hash::Hash;

pub trait VBase<T = Self>: Clone + AsRef<T> + Hash + PartialEq + Eq {
  fn vid(&self) -> VidRef;
  fn label(&self) -> LabelRef;
}
pub trait EBase<T = Self>: Clone + AsRef<T> + Hash + PartialEq + Eq {
  fn eid(&self) -> VidRef;
  fn src_vid(&self) -> VidRef;
  fn dst_vid(&self) -> VidRef;
  fn label(&self) -> LabelRef;
  fn contains(&self, vid: VidRef) -> bool {
    self.src_vid() == vid || self.dst_vid() == vid
  }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PatternVertex {
  pub(crate) vid: Vid,
  pub(crate) label: Label,
  pub(crate) attr: Option<PatternAttr>,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct DataVertex {
  pub(crate) vid: Vid,
  pub(crate) label: Label,
  pub(crate) attrs: HashMap<String, AttrValue>,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PatternEdge {
  pub(crate) eid: Eid,
  pub(crate) src_vid: Vid,
  pub(crate) dst_vid: Vid,
  pub(crate) label: Label,
  pub(crate) attr: Option<PatternAttr>,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct DataEdge {
  pub(crate) eid: Eid,
  pub(crate) src_vid: Vid,
  pub(crate) dst_vid: Vid,
  pub(crate) label: Label,
  pub(crate) attrs: HashMap<String, AttrValue>,
}

impl AsRef<Self> for PatternVertex {
  fn as_ref(&self) -> &Self {
    self
  }
}
impl VBase for PatternVertex {
  fn vid(&self) -> VidRef<'_> {
    &self.vid
  }
  fn label(&self) -> LabelRef<'_> {
    &self.label
  }
}
impl Hash for PatternVertex {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.vid.hash(state);
  }
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
impl VBase for DataVertex {
  fn vid(&self) -> VidRef<'_> {
    &self.vid
  }
  fn label(&self) -> LabelRef<'_> {
    &self.label
  }
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
impl EBase for PatternEdge {
  fn eid(&self) -> VidRef<'_> {
    &self.eid
  }
  fn src_vid(&self) -> VidRef<'_> {
    &self.src_vid
  }
  fn dst_vid(&self) -> VidRef<'_> {
    &self.dst_vid
  }
  fn label(&self) -> LabelRef<'_> {
    &self.label
  }
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
impl EBase for DataEdge {
  fn eid(&self) -> VidRef<'_> {
    &self.eid
  }
  fn src_vid(&self) -> VidRef<'_> {
    &self.src_vid
  }
  fn dst_vid(&self) -> VidRef<'_> {
    &self.dst_vid
  }
  fn label(&self) -> LabelRef<'_> {
    &self.label
  }
}

impl DataVertex {
  pub fn satisfy_attr(&self, attr: Option<impl AsRef<PatternAttr>>) -> bool {
    if attr.is_none() {
      return true;
    }

    let attr = attr.unwrap();

    match attr.as_ref().op {
      super::Op::Eq => self
        .attrs
        .get(&attr.as_ref().key)
        .is_some_and(|v| v == &attr.as_ref().value),
      super::Op::Ne => self
        .attrs
        .get(&attr.as_ref().key)
        .is_some_and(|v| v != &attr.as_ref().value),
      super::Op::Gt => self
        .attrs
        .get(&attr.as_ref().key)
        .is_some_and(|v| v > &attr.as_ref().value),
      super::Op::Ge => self
        .attrs
        .get(&attr.as_ref().key)
        .is_some_and(|v| v >= &attr.as_ref().value),
      super::Op::Lt => self
        .attrs
        .get(&attr.as_ref().key)
        .is_some_and(|v| v < &attr.as_ref().value),
      super::Op::Le => self
        .attrs
        .get(&attr.as_ref().key)
        .is_some_and(|v| v <= &attr.as_ref().value),
    }
  }
}
