pub mod attr;
pub mod base;
pub mod entities;
pub mod instruction;

use ahash::AHashMap;
pub use {attr::*, base::*, entities::*, instruction::*};

#[derive(Debug, Clone, Default)]
pub struct PlanData {
  #[allow(unused)]
  pub(crate) matching_order: Vec<String>,

  pub(crate) pattern_vs: AHashMap<Vid, PatternVertex>,
  pub(crate) pattern_es: AHashMap<Vid, PatternEdge>,

  #[allow(unused)]
  pub(crate) instructions: Vec<Instruction>,
}

impl PlanData {
  pub fn pattern_vs(&self) -> &AHashMap<Vid, PatternVertex> {
    &self.pattern_vs
  }
  pub fn pattern_es(&self) -> &AHashMap<Vid, PatternEdge> {
    &self.pattern_es
  }
}
