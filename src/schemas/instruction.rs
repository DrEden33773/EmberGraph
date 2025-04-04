use super::{VidRef, base::Vid};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[repr(u8)]
pub enum InstructionType {
  #[serde(rename = "init")]
  Init = 0,
  #[serde(rename = "get_adj")]
  GetAdj = 1,
  #[serde(rename = "intersect")]
  Intersect = 2,
  #[serde(rename = "foreach")]
  Foreach = 3,
  #[serde(rename = "t_cache")]
  TCache = 4,
  #[serde(rename = "report")]
  Report = 5,
}

impl InstructionType {
  pub fn compare(self, other: Self) -> i8 {
    self as u8 as i8 - other as u8 as i8
  }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Instruction {
  pub(crate) vid: Vid,
  #[serde(rename = "type")]
  pub(crate) type_: InstructionType,
  #[serde(rename = "expand_eid_list")]
  pub(crate) expand_eids: Vec<String>,
  pub(crate) single_op: Option<String>,
  pub(crate) multi_ops: Vec<String>,
  pub(crate) target_var: String,
  pub(crate) depend_on: Vec<String>,
}

impl Instruction {
  pub fn is_single_op(&self) -> bool {
    self.single_op.is_some()
  }
}

pub struct InstructionBuilder {
  pub(crate) vid: Vid,
  pub(crate) type_: InstructionType,
  pub(crate) target_var: String,
  pub(crate) expand_eids: Vec<String>,
  pub(crate) single_op: Option<String>,
  pub(crate) multi_ops: Vec<String>,
  pub(crate) depend_on: Vec<String>,
}

impl InstructionBuilder {
  pub fn new(vid: VidRef, type_: InstructionType) -> Self {
    Self {
      vid: vid.to_string(),
      type_,
      target_var: String::new(),
      expand_eids: vec![],
      single_op: None,
      multi_ops: vec![],
      depend_on: vec![],
    }
  }

  pub fn expand_eids(mut self, expand_eids: impl IntoIterator<Item = String>) -> Self {
    self.expand_eids = expand_eids.into_iter().collect();
    self
  }

  pub fn single_op(mut self, single_op: String) -> Self {
    self.single_op = Some(single_op);
    self
  }

  pub fn multi_ops(mut self, multi_ops: impl IntoIterator<Item = String>) -> Self {
    self.multi_ops = multi_ops.into_iter().collect();
    self
  }

  pub fn target_var(mut self, target_var: String) -> Self {
    self.target_var = target_var;
    self
  }

  pub fn depend_on(mut self, depend_on: impl IntoIterator<Item = String>) -> Self {
    self.depend_on = depend_on.into_iter().collect();
    self
  }

  pub fn build(self) -> Instruction {
    Instruction {
      vid: self.vid,
      type_: self.type_,
      expand_eids: self.expand_eids,
      single_op: self.single_op,
      multi_ops: self.multi_ops,
      target_var: self.target_var,
      depend_on: self.depend_on,
    }
  }
}
