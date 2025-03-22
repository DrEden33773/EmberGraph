use strum_macros::{Display, EnumString};

use super::base::{Op, Vid};

#[derive(Debug, Clone, Copy, Display, EnumString)]
pub enum InstructionType {
  #[strum(serialize = "init")]
  Init,
  #[strum(serialize = "get_adj")]
  GetAdj,
  #[strum(serialize = "intersect")]
  Intersect,
  #[strum(serialize = "foreach")]
  Foreach,
  #[strum(serialize = "t_cache")]
  TCache,
  #[strum(serialize = "report")]
  Report,
}

#[derive(Debug, Clone)]
pub struct Instruction {
  pub(crate) vid: Vid,
  pub(crate) type_: InstructionType,
  pub(crate) single_op: Option<Op>,
  pub(crate) multi_ops: Vec<Op>,
  pub(crate) target_var: String,
  pub(crate) depend_on: Vec<String>,
}

impl Instruction {
  pub fn is_single_op(&self) -> bool {
    self.single_op.is_some()
  }
}
