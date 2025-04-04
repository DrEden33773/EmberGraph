use super::{plan_gen::PlanGenerator, plan_opt::PlanOptimizer};
use crate::{
  schemas::{Instruction, PatternEdge, PatternVertex, PlanData, Vid},
  utils::dyn_graph::DynGraph,
};
use hashbrown::HashMap;

#[derive(Debug, Clone)]
pub struct PlanDumper {
  matching_order: Vec<Vid>,
  exec_instructions: Vec<Instruction>,
  pattern_graph: DynGraph<PatternVertex, PatternEdge>,
}

impl PlanDumper {
  pub fn to_plan_data(self) -> PlanData {
    let matching_order = self.matching_order;

    let pattern_vs = self
      .pattern_graph
      .v_entities()
      .iter()
      .map(|(vid, vertex)| (vid.clone(), vertex.clone()))
      .collect::<HashMap<_, _>>();
    let pattern_es = self
      .pattern_graph
      .e_entities()
      .iter()
      .map(|(vid, edge)| (vid.clone(), edge.clone()))
      .collect::<HashMap<_, _>>();

    let instructions = self.exec_instructions;

    PlanData {
      matching_order,
      pattern_vs,
      pattern_es,
      instructions,
    }
  }

  pub fn serialize_json(self) -> String {
    serde_json::to_string_pretty(&self.to_plan_data()).unwrap()
  }
}

impl From<PlanGenerator> for PlanDumper {
  fn from(plan_generator: PlanGenerator) -> Self {
    Self {
      matching_order: plan_generator.optimal_order.into_iter().collect(),
      exec_instructions: plan_generator.exec_instructions,
      pattern_graph: plan_generator.pattern_graph,
    }
  }
}

impl From<PlanOptimizer> for PlanDumper {
  fn from(plan_optimizer: PlanOptimizer) -> Self {
    Self {
      matching_order: plan_optimizer.matching_order,
      exec_instructions: plan_optimizer.exec_instructions,
      pattern_graph: plan_optimizer.pattern_graph,
    }
  }
}
