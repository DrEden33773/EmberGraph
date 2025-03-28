use serde::{Deserialize, Serialize};
use std::fmt::Display;
use strum_macros::{Display, EnumString};

pub const STR_TUPLE_SPLITTER: &str = "^";

pub type Vid = String;
pub type VidRef<'a> = &'a str;

pub type Eid = String;
pub type EidRef<'a> = &'a str;

pub type Label = String;
pub type LabelRef<'a> = &'a str;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum Op {
  #[serde(rename = "=")]
  Eq,
  #[serde(rename = "!=")]
  Ne,
  #[serde(rename = ">")]
  Gt,
  #[serde(rename = ">=")]
  Ge,
  #[serde(rename = "<")]
  Lt,
  #[serde(rename = "<=")]
  Le,
}

impl Op {
  #[inline]
  pub fn to_neo4j_op(&self) -> &str {
    match self {
      Op::Eq => "=",
      Op::Ne => "<>",
      Op::Gt => ">",
      Op::Ge => ">=",
      Op::Lt => "<",
      Op::Le => "<=",
    }
  }

  #[inline]
  pub fn to_sqlite_op(&self) -> &str {
    match self {
      Op::Eq => "=",
      Op::Ne => "<>",
      Op::Gt => ">",
      Op::Ge => ">=",
      Op::Lt => "<",
      Op::Le => "<=",
    }
  }
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

#[derive(Debug, Clone, Copy, Display, EnumString, PartialEq, Eq, Deserialize, Serialize)]
pub enum VarPrefix {
  #[strum(serialize = " ")]
  #[serde(rename = " ")]
  DataGraph,

  #[strum(serialize = "f")]
  #[serde(rename = "f")]
  EnumerateTarget,

  #[strum(serialize = "A")]
  #[serde(rename = "A")]
  DbQueryTarget,

  #[strum(serialize = "T")]
  #[serde(rename = "T")]
  IntersectTarget,

  #[strum(serialize = "C")]
  #[serde(rename = "C")]
  IntersectCandidate,

  #[strum(serialize = "V")]
  #[serde(rename = "V")]
  DataVertexSet,
}

impl VarPrefix {
  pub fn with(&self, other: impl Display) -> String {
    format!("{}{STR_TUPLE_SPLITTER}{}", self, other)
  }
}
