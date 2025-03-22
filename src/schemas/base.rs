use std::fmt::Display;
use strum_macros::{Display, EnumString};

pub const STR_TUPLE_SPLITTER: &str = "^";

pub type Vid = String;
pub type VidRef<'a> = &'a str;

pub type Eid = String;
pub type EidRef<'a> = &'a str;

pub type Label = String;
pub type LabelRef<'a> = &'a str;

#[derive(Debug, Clone, Copy, Display, EnumString, PartialEq, Eq)]
pub enum Op {
  #[strum(serialize = "=")]
  Eq,
  #[strum(serialize = "!=")]
  Ne,
  #[strum(serialize = ">")]
  Gt,
  #[strum(serialize = ">=")]
  Ge,
  #[strum(serialize = "<")]
  Lt,
  #[strum(serialize = "<=")]
  Le,
}

impl Op {
  pub fn operate_on<V: PartialEq + PartialOrd>(&self, left: &V, right: &V) -> bool {
    match self {
      Op::Eq => left == right,
      Op::Ne => left != right,
      Op::Gt => left > right,
      Op::Ge => left >= right,
      Op::Lt => left < right,
      Op::Le => left <= right,
    }
  }
}

#[derive(Debug, Clone, Copy, Display, EnumString, PartialEq, Eq)]
pub enum VarPrefix {
  #[strum(serialize = " ")]
  DataGraph,
  #[strum(serialize = "f")]
  EnumerateTarget,
  #[strum(serialize = "A")]
  DbQueryTarget,
  #[strum(serialize = "T")]
  IntersectTarget,
  #[strum(serialize = "C")]
  IntersectCandidate,
  #[strum(serialize = "V")]
  DataVertexSet,
}

impl VarPrefix {
  pub fn with(&self, other: impl Display) -> String {
    format!("{}{STR_TUPLE_SPLITTER}{}", self, other)
  }
}
