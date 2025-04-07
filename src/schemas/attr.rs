use super::base::Op;
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use std::{fmt::Display, hash::Hash, str::FromStr};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, strum_macros::Display)]
#[serde(rename_all = "lowercase")]
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

impl AttrValue {
  pub fn to_type(&self) -> AttrType {
    match self {
      Self::Int(_) => AttrType::Int,
      Self::Float(_) => AttrType::Float,
      Self::String(_) => AttrType::String,
    }
  }
}

impl FromStr for AttrValue {
  type Err = String;

  fn from_str(s: &str) -> Result<Self, Self::Err> {
    if let Ok(i) = s.parse::<i64>() {
      return Ok(AttrValue::Int(i));
    }
    if let Ok(f) = s.parse::<f64>() {
      return Ok(AttrValue::Float(f));
    }
    Ok(AttrValue::String(s.to_string()))
  }
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct PatternAttr {
  #[serde(rename = "attr")]
  pub(crate) key: String,
  pub(crate) op: Op,
  pub(crate) value: AttrValue,
  #[serde(rename = "type")]
  pub(crate) _type: AttrType,
}

impl PatternAttr {
  pub fn parse_from_raw(key: String, raw_pred: String) -> Self {
    let mut cursor = 0;

    // op
    let mut raw_op = String::new();
    for c in raw_pred.chars() {
      if c.is_alphanumeric() || c == '\'' || c == '\"' {
        break;
      }
      raw_op.push(c);
      cursor += 1;
    }
    let op = Op::from_str(&raw_op)
      .unwrap_or_else(|_| panic!("⚠️  Invalid operator: `{raw_op}` in `{raw_pred}`."));

    // value
    let last_char = raw_pred.chars().last().unwrap_or(' ');
    let value = match raw_pred.chars().nth(cursor) {
      Some(c) => match c {
        c if c.is_ascii_digit() => {
          let raw_value = &raw_pred[cursor..];
          AttrValue::from_str(raw_value)
            .unwrap_or_else(|_| panic!("⚠️  Malformed literal: `{raw_value}` in `{raw_pred}`."))
        }
        c if c == '\'' && last_char != '\'' => {
          panic!("⚠️  Missing closing quote `'` in `{raw_pred}`.")
        }
        c if c == '\"' && last_char != '\"' => {
          panic!("⚠️  Missing closing quote `\"` in `{raw_pred}`.")
        }
        c if c == '\'' || c == '\"' => {
          let raw_value = &raw_pred[cursor + 1..raw_pred.len() - 1];
          AttrValue::String(raw_value.to_string())
        }
        _ => panic!("⚠️  Invalid character: {c}."),
      },
      None => panic!("⚠️  Missing value in `{raw_pred}`."),
    };

    // Currently, `string` only support `Eq` and `Ne`.
    if matches!(value, AttrValue::String(_)) && op != Op::Eq && op != Op::Ne {
      panic!("⚠️  Invalid operator `{op}` for string attribute `{key}`.");
    }

    // type
    let _type = value.to_type();

    Self {
      key,
      op,
      value,
      _type,
    }
  }
}

impl PatternAttr {
  pub fn to_neo4j_constraint(&self, field: &str) -> String {
    let value_repr = if self._type == AttrType::String {
      format!("\"{}\"", self.value)
    } else {
      format!("{}", self.value)
    };

    let left = format!("{field}.{}", &self.key);
    let mid = self.op.to_neo4j_sqlite_repr();
    let right = value_repr;

    format!("{left} {mid} {right}")
  }

  pub fn is_data_attr_satisfied(&self, data_attr: Option<&AttrValue>) -> bool {
    match data_attr {
      Some(data_attr) => self.op.operate_on(data_attr, &self.value),
      None => false,
    }
  }

  pub fn is_data_attrs_satisfied(&self, data_attrs: HashMap<String, AttrValue>) -> bool {
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
