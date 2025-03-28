pub mod attr;
pub mod base;
pub mod entities;
pub mod instruction;
pub mod serde;

use hashbrown::HashMap;

pub use {attr::*, base::*, entities::*, instruction::*, serde::*};

#[derive(Debug, Clone, Default)]
pub struct PlanData {
  #[allow(unused)]
  pub(crate) matching_order: Vec<String>,

  pub(crate) pattern_vs: HashMap<Vid, PatternVertex>,
  pub(crate) pattern_es: HashMap<Vid, PatternEdge>,

  #[allow(unused)]
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
