use serde::{
  Deserialize,
  de::{self, Visitor},
};
use std::str::FromStr;

pub use super::{attr::*, base::*, entities::*, instruction::*};

impl<'de> Deserialize<'de> for AttrValue {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    struct AttrValueVisitor;

    impl<'de> Visitor<'de> for AttrValueVisitor {
      type Value = AttrValue;

      fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("Invalid type detected (support: [i64, f64, String])")
      }

      fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
      where
        E: de::Error,
      {
        AttrValue::from_str(value).map_err(E::custom)
      }

      fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
      where
        E: de::Error,
      {
        Ok(AttrValue::Float(v))
      }

      fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
      where
        E: de::Error,
      {
        Ok(AttrValue::Int(v))
      }
    }

    deserializer.deserialize_any(AttrValueVisitor)
  }
}
