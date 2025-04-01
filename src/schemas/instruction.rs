use super::base::Vid;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
pub enum InstructionType {
  #[serde(rename = "init")]
  Init,
  #[serde(rename = "get_adj")]
  GetAdj,
  #[serde(rename = "intersect")]
  Intersect,
  #[serde(rename = "foreach")]
  Foreach,
  #[serde(rename = "t_cache")]
  TCache,
  #[serde(rename = "report")]
  Report,
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
