use super::base::Op;
use ahash::AHashMap;
use std::{fmt::Display, hash::Hash};
use strum_macros::EnumString;

#[derive(Debug, Clone, Copy, strum_macros::Display, EnumString, PartialEq, Eq)]
pub enum AttrType {
  #[strum(serialize = "int")]
  Int,
  #[strum(serialize = "float")]
  Float,
  #[strum(serialize = "string")]
  String,
}

#[derive(Debug, Clone)]
pub enum AttrValue {
  Int(i64),
  Float(f64),
  String(String),
}

impl From<i64> for AttrValue {
  fn from(value: i64) -> Self {
    Self::Int(value)
  }
}

impl From<f64> for AttrValue {
  fn from(value: f64) -> Self {
    Self::Float(value)
  }
}

impl From<String> for AttrValue {
  fn from(value: String) -> Self {
    Self::String(value)
  }
}

impl From<&str> for AttrValue {
  fn from(value: &str) -> Self {
    Self::String(value.to_string())
  }
}

impl Display for AttrValue {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      Self::Int(v) => write!(f, "{}", v),
      Self::Float(v) => write!(f, "{}", v),
      Self::String(v) => write!(f, "{}", v),
    }
  }
}

impl PartialOrd for AttrValue {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    match (self, other) {
      (Self::Int(l0), Self::Int(r0)) => l0.partial_cmp(r0),
      (Self::Int(l0), Self::Float(r0)) => (*l0 as f64).partial_cmp(r0),
      (Self::Float(l0), Self::Float(r0)) => l0.partial_cmp(r0),
      (Self::Float(l0), Self::Int(r0)) => l0.partial_cmp(&(*r0 as f64)),
      (Self::String(l0), Self::String(r0)) => l0.partial_cmp(r0),
      _ => None,
    }
  }
}

impl PartialEq for AttrValue {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (Self::Int(l0), Self::Int(r0)) => l0 == r0,
      (Self::Int(l0), Self::Float(r0)) => *l0 as f64 == *r0,
      (Self::Float(l0), Self::Float(r0)) => l0 == r0,
      (Self::Float(l0), Self::Int(r0)) => *l0 == *r0 as f64,
      (Self::String(l0), Self::String(r0)) => l0 == r0,
      _ => false,
    }
  }
}

impl Eq for AttrValue {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PatternAttr {
  pub(crate) key: String,
  pub(crate) op: Op,
  pub(crate) value: AttrValue,
  pub(crate) _type: AttrType,
}

impl PatternAttr {
  pub fn is_data_attr_satisfied(&self, data_attr: Option<&AttrValue>) -> bool {
    match data_attr {
      Some(data_attr) => self.op.operate_on(data_attr, &self.value),
      None => false,
    }
  }

  pub fn is_data_attrs_satisfied(&self, data_attrs: AHashMap<String, AttrValue>) -> bool {
    match data_attrs.get(&self.key) {
      Some(data_attr) => self.op.operate_on(data_attr, &self.value),
      None => false,
    }
  }
}

impl Hash for PatternAttr {
  fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
    self.key.hash(state);
  }
}
