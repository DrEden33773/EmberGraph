pub mod attr;
pub mod base;
pub mod entities;
pub mod instruction;
pub mod serde;

use ::serde::{Deserialize, Serialize};
use hashbrown::HashMap;

#[allow(unused_imports)]
pub use {attr::*, base::*, entities::*, instruction::*, serde::*};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PlanData {
  pub(crate) matching_order: Vec<String>,
  #[serde(rename = "vertices")]
  pub(crate) pattern_vs: HashMap<Vid, PatternVertex>,
  #[serde(rename = "edges")]
  pub(crate) pattern_es: HashMap<Eid, PatternEdge>,
  pub(crate) instructions: Vec<Instruction>,
}

impl PlanData {
  pub fn pattern_vs(&self) -> &HashMap<Vid, PatternVertex> {
    &self.pattern_vs
  }
  pub fn pattern_es(&self) -> &HashMap<Vid, PatternEdge> {
    &self.pattern_es
  }
}
