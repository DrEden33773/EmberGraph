use super::dyn_graph::DynGraph;
use crate::schemas::{AttrValue, DataEdge, DataVertex, EBase, VBase};
use colored::Colorize;
use hashbrown::HashMap;
use itertools::Itertools;

pub trait PrettyDump {
  fn pretty_dump_detailed(&self, colored: bool) -> String;
  fn pretty_dump_simplified(&self, colored: bool) -> String;
}

impl PrettyDump for DataVertex {
  fn pretty_dump_detailed(&self, colored: bool) -> String {
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
      attrs.push_str(&format!("{key}: "));
      let value = if colored {
        match value {
          AttrValue::Int(v) => v.to_string().purple(),
          AttrValue::Float(v) => v.to_string().yellow(),
          AttrValue::String(v) => format!(r#""{v}""#).green(),
        }
        .to_string()
      } else {
        match value {
          AttrValue::Int(v) => v.to_string(),
          AttrValue::Float(v) => v.to_string(),
          AttrValue::String(v) => format!(r#""{v}""#),
        }
      };
      // remember to use `to_string` or the color will not be applied
      attrs.push_str(&value.to_string());
      if i < len - 1 {
        attrs.push_str(", ");
      }
    }
    attrs.push('}');

    if attrs != "{}" {
      format!("({label} {attrs})")
    } else {
      format!("({label})")
    }
  }

  fn pretty_dump_simplified(&self, colored: bool) -> String {
    let label = if colored {
      format!(":{}", self.label()).red().to_string()
    } else {
      format!(":{}", self.label())
    };
    let vid = if colored {
      self.vid().to_string().cyan().to_string()
    } else {
      self.vid().to_string()
    };

    format!("({label} {vid})")
  }
}

impl PrettyDump for DataEdge {
  fn pretty_dump_detailed(&self, colored: bool) -> String {
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
      attrs.push_str(&format!("{key}: "));
      let value = if colored {
        match value {
          AttrValue::Int(v) => v.to_string().purple(),
          AttrValue::Float(v) => v.to_string().yellow(),
          AttrValue::String(v) => format!(r#""{v}""#).green(),
        }
        .to_string()
      } else {
        match value {
          AttrValue::Int(v) => v.to_string(),
          AttrValue::Float(v) => v.to_string(),
          AttrValue::String(v) => format!(r#""{v}""#),
        }
      };
      // remember to use `to_string` or the color will not be applied
      attrs.push_str(&value.to_string());
      if i < len - 1 {
        attrs.push_str(", ");
      }
    }
    attrs.push('}');

    if attrs != "{}" {
      format!("[{label} {attrs}]")
    } else {
      format!("[{label}]")
    }
  }

  fn pretty_dump_simplified(&self, colored: bool) -> String {
    let label = if colored {
      format!(":{}", self.label()).red().to_string()
    } else {
      format!(":{}", self.label())
    };

    format!("[{label}]")
  }
}

impl<VType: VBase + PrettyDump, EType: EBase + PrettyDump> DynGraph<VType, EType> {
  pub fn pre_dump_detailed(&self, colored: bool) -> HashMap<String, Vec<String>> {
    self
      .pattern_2_eids
      .iter()
      .chain(self.pattern_2_vids.iter())
      .map(|(pattern, ids)| {
        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
          if let Some(vertex) = self.v_entities.get(id) {
            result.push(vertex.pretty_dump_detailed(colored));
          } else if let Some(edge) = self.e_entities.get(id) {
            result.push(edge.pretty_dump_detailed(colored));
          }
        }
        (pattern.clone(), result)
      })
      .collect()
  }

  pub fn pre_dump_simplified(&self, colored: bool) -> HashMap<String, Vec<String>> {
    self
      .pattern_2_eids
      .iter()
      .chain(self.pattern_2_vids.iter())
      .map(|(pattern, ids)| {
        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
          if let Some(vertex) = self.v_entities.get(id) {
            result.push(vertex.pretty_dump_simplified(colored));
          } else if let Some(edge) = self.e_entities.get(id) {
            result.push(edge.pretty_dump_simplified(colored));
          }
        }
        (pattern.clone(), result)
      })
      .collect()
  }
}
