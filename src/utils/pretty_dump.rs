use super::dyn_graph::DynGraph;
use crate::schemas::{AttrValue, DataEdge, DataVertex, EBase, VBase};
use colored::Colorize;
use hashbrown::HashMap;
use itertools::Itertools;

pub trait PrettyDump {
  fn pretty_dump(&self) -> String;
}

impl PrettyDump for DataVertex {
  fn pretty_dump(&self) -> String {
    let label = format!(":{}", self.label()).red();

    let sorted_kv_pairs = self
      .attrs
      .iter()
      .filter(|pair| !pair.0.is_empty())
      .sorted_by_key(|pair| pair.0.clone())
      .collect_vec();
    let len = sorted_kv_pairs.len();

    let mut attrs = "{".to_string();
    for (i, (key, value)) in sorted_kv_pairs.into_iter().enumerate() {
      attrs.push_str(&format!("{}: ", key));
      let value = match value {
        AttrValue::Int(v) => v.to_string().purple(),
        AttrValue::Float(v) => v.to_string().yellow(),
        AttrValue::String(v) => format!(r#""{v}""#).green(),
      };
      // remember to use `to_string` or the color will not be applied
      attrs.push_str(&value.to_string());
      if i < len - 1 {
        attrs.push_str(", ");
      }
    }
    attrs.push('}');

    if attrs != "{}" {
      format!("({} {})", label, attrs)
    } else {
      format!("({})", label)
    }
  }
}

impl PrettyDump for DataEdge {
  fn pretty_dump(&self) -> String {
    let label = format!(":{}", self.label()).red();

    let sorted_kv_pairs = self
      .attrs
      .iter()
      .filter(|pair| !pair.0.is_empty())
      .sorted_by_key(|pair| pair.0.clone())
      .collect_vec();
    let len = sorted_kv_pairs.len();

    let mut attrs = "{".to_string();
    for (i, (key, value)) in sorted_kv_pairs.into_iter().enumerate() {
      attrs.push_str(&format!("{}: ", key));
      let value = match value {
        AttrValue::Int(v) => v.to_string().purple(),
        AttrValue::Float(v) => v.to_string().yellow(),
        AttrValue::String(v) => format!(r#""{v}""#).green(),
      };
      // remember to use `to_string` or the color will not be applied
      attrs.push_str(&value.to_string());
      if i < len - 1 {
        attrs.push_str(", ");
      }
    }
    attrs.push('}');

    if attrs != "{}" {
      format!("[{} {}]", label, attrs)
    } else {
      format!("[{}]", label)
    }
  }
}

impl<VType: VBase + PrettyDump, EType: EBase + PrettyDump> DynGraph<VType, EType> {
  pub fn pre_dump(&self) -> HashMap<String, Vec<String>> {
    self
      .pattern_2_eids
      .iter()
      .chain(self.pattern_2_vids.iter())
      .map(|(pattern, ids)| {
        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
          if let Some(vertex) = self.v_entities.get(id) {
            result.push(vertex.pretty_dump());
          } else if let Some(edge) = self.e_entities.get(id) {
            result.push(edge.pretty_dump());
          }
        }
        (pattern.clone(), result)
      })
      .collect()
  }
}
