use serde::{
  Deserialize, Serialize,
  de::{self, Visitor},
};
use std::str::FromStr;

pub use super::{attr::*, base::*, entities::*, instruction::*};

impl Serialize for AttrValue {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    match self {
      AttrValue::Int(v) => serializer.serialize_i64(*v),
      AttrValue::Float(v) => serializer.serialize_f64(*v),
      AttrValue::String(v) => serializer.serialize_str(v),
    }
  }
}

impl<'de> Deserialize<'de> for AttrValue {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    struct AttrValueVisitor;

    impl<'de> Visitor<'de> for AttrValueVisitor {
      type Value = AttrValue;

      fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("Integer / Float / String")
      }

      fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
      where
        E: de::Error,
      {
        AttrValue::from_str(value).map_err(E::custom)
      }
      fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
      where
        E: de::Error,
      {
        AttrValue::from_str(&value).map_err(E::custom)
      }

      fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
      where
        E: de::Error,
      {
        Ok(AttrValue::Int(v))
      }
      fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
      where
        E: de::Error,
      {
        if value <= i64::MAX as u64 {
          Ok(AttrValue::Int(value as i64))
        } else {
          Ok(AttrValue::Float(value as f64))
        }
      }
      fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
      where
        E: de::Error,
      {
        Ok(AttrValue::Float(v))
      }
    }

    deserializer.deserialize_any(AttrValueVisitor)
  }
}

#[cfg(test)]
mod test_deserializer {
  use super::*;

  #[test]
  fn test_attr_value_deserializer() {
    let str_v = "\"hello!\"";
    let int_v = "\"1234567890\"";
    let float_v = "\"123.456\"";

    let str_attr_value: AttrValue = serde_json::from_str(str_v).unwrap();
    let int_attr_value: AttrValue = serde_json::from_str(int_v).unwrap();
    let float_attr_value: AttrValue = serde_json::from_str(float_v).unwrap();

    assert_eq!(str_attr_value, AttrValue::String("hello!".to_string()));
    assert_eq!(int_attr_value, AttrValue::Int(1234567890));
    assert_eq!(float_attr_value, AttrValue::Float(123.456));

    let int = 1234567890;
    let float = 123.456;

    let _int_attr_value: AttrValue = serde_json::from_str(&int.to_string()).unwrap();
    let _float_attr_value: AttrValue = serde_json::from_str(&float.to_string()).unwrap();

    assert_eq!(_int_attr_value, AttrValue::Int(int));
    assert_eq!(_float_attr_value, AttrValue::Float(float));
  }
}
