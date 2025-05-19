use super::order_calc::PlanGenInput;
use crate::{
  schemas::{
    Instruction, InstructionBuilder, InstructionType, PatternEdge, PatternVertex, VarPrefix, Vid,
  },
  utils::dyn_graph::DynGraph,
};
use hashbrown::HashSet;
use itertools::Itertools;

#[derive(Debug, Clone)]
pub struct PlanGenerator {
  pub(crate) pattern_graph: DynGraph<PatternVertex, PatternEdge>,
  pub(crate) optimal_order: Vec<Vid>,
  pub(crate) exec_instructions: Vec<Instruction>,
}

impl From<PlanGenInput> for PlanGenerator {
  fn from(input: PlanGenInput) -> Self {
    Self {
      pattern_graph: input.pattern_graph,
      optimal_order: input.optimal_order,
      exec_instructions: vec![],
    }
  }
}

impl PlanGenerator {
  pub fn generate_raw_plan(&mut self) {
    if self.optimal_order.is_empty() {
      return;
    }

    let mut instructions = vec![];
    let mut f_set = HashSet::new();
    let mut expanded_es = HashSet::new();

    // first vertex
    let vid = self.optimal_order[0].clone();
    let adj_eids = self.pattern_graph.get_adj_eids(&vid);
    // Init -> fx
    instructions.push(
      InstructionBuilder::new(&vid, InstructionType::Init)
        .target_var(VarPrefix::EnumerateTarget.with(&vid))
        .build(),
    );
    // GetAdj(fx) -> Ax
    instructions.push(
      InstructionBuilder::new(&vid, InstructionType::GetAdj)
        .expand_eids(adj_eids.clone())
        .single_op(VarPrefix::EnumerateTarget.with(&vid))
        .target_var(VarPrefix::DbQueryTarget.with(&vid))
        .build(),
    );
    // update `f_set` and `expanded_es`
    f_set.insert(vid);
    expanded_es.extend(adj_eids);

    // other vertices
    for vid in self.optimal_order.iter().skip(1).cloned() {
      let mut adj_precursors = f_set.clone();
      adj_precursors.retain(|v| self.pattern_graph.get_adj_vids(&vid).contains(v));

      let mut adj_eids = self.pattern_graph.get_adj_eids(&vid);
      // only pick those edges that are not expanded
      adj_eids.retain(|eid| !expanded_es.contains(eid));

      // Init -> fx
      if adj_precursors.is_empty() {
        instructions.push(
          InstructionBuilder::new(&vid, InstructionType::Init)
            .target_var(VarPrefix::EnumerateTarget.with(&vid))
            .build(),
        );
      }
      // Intersect(Ax, V) -> Cx
      else if adj_precursors.len() == 1 {
        let f = adj_precursors.iter().next().cloned().unwrap();
        instructions.push(
          InstructionBuilder::new(&vid, InstructionType::Intersect)
            .single_op(VarPrefix::DbQueryTarget.with(f))
            .target_var(VarPrefix::IntersectCandidate.with(&vid))
            .build(),
        );
      }
      // Intersect(Aw, Ax, ..., Ay, Az) -> Tx
      // Intersect(Tx, V) -> Cx
      else {
        let multi_ops = adj_precursors
          .iter()
          .map(|vid| VarPrefix::DbQueryTarget.with(vid))
          .collect_vec();
        // Intersect(Aw, Ax, ..., Ay, Az) -> Tx
        instructions.push(
          InstructionBuilder::new(&vid, InstructionType::Intersect)
            .multi_ops(multi_ops)
            .target_var(VarPrefix::IntersectTarget.with(&vid))
            .build(),
        );
        // Intersect(Tx, V) -> Cx
        instructions.push(
          InstructionBuilder::new(&vid, InstructionType::Intersect)
            .single_op(VarPrefix::IntersectTarget.with(&vid))
            .target_var(VarPrefix::IntersectCandidate.with(&vid))
            .build(),
        );
      }

      // Foreach(Cx) -> fx (only need if operands is not empty)
      if !adj_precursors.is_empty() {
        instructions.push(
          InstructionBuilder::new(&vid, InstructionType::Foreach)
            .single_op(VarPrefix::IntersectCandidate.with(&vid))
            .target_var(VarPrefix::EnumerateTarget.with(&vid))
            .build(),
        );
      }

      // GetAdj(fx) -> Ax
      instructions.push(
        InstructionBuilder::new(&vid, InstructionType::GetAdj)
          .expand_eids(adj_eids.clone())
          .single_op(VarPrefix::EnumerateTarget.with(&vid))
          .target_var(VarPrefix::DbQueryTarget.with(&vid))
          .build(),
      );

      // update `f_set` and `expanded_es`
      f_set.insert(vid);
      expanded_es.extend(adj_eids);
    }

    // Report
    let embedding = f_set
      .iter()
      .map(|vid| VarPrefix::EnumerateTarget.with(vid))
      .collect_vec();
    instructions.push(
      InstructionBuilder::new("", InstructionType::Report)
        .multi_ops(embedding)
        .target_var(VarPrefix::EnumerateTarget.to_string())
        .build(),
    );

    self.exec_instructions = remove_unused_dbq(instructions);
  }
}

#[cfg(not(feature = "no_optimizations"))]
fn remove_unused_dbq(instructions: Vec<Instruction>) -> Vec<Instruction> {
  let mut depend_set = HashSet::new();
  for instr in instructions.iter() {
    if let Some(op) = &instr.single_op {
      if op != VarPrefix::DataVertexSet.as_ref() {
        depend_set.insert(op.clone());
      }
    } else {
      depend_set.extend(instr.multi_ops.iter().cloned());
    }
  }

  instructions
    .into_iter()
    .filter(|instr| {
      instr.type_ != InstructionType::GetAdj || depend_set.contains(&instr.target_var)
    })
    .collect()
}

#[cfg(feature = "no_optimizations")]
fn remove_unused_dbq(instructions: Vec<Instruction>) -> Vec<Instruction> {
  instructions
}
