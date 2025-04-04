use crate::{
  schemas::{
    Instruction, InstructionBuilder, InstructionType, PatternEdge, PatternVertex, VarPrefix, Vid,
  },
  utils::dyn_graph::DynGraph,
};
use hashbrown::{HashMap, HashSet};
use std::collections::VecDeque;

use super::order_calc::PlanGenInput;

#[derive(Debug, Clone)]
pub struct PlanGenerator {
  pub(crate) pattern_graph: DynGraph<PatternVertex, PatternEdge>,
  pub(crate) optimal_order: VecDeque<(Vid, usize)>,
  pub(crate) exec_instructions: Vec<Instruction>,
}

impl From<PlanGenInput> for PlanGenerator {
  fn from(input: PlanGenInput) -> Self {
    Self {
      pattern_graph: input.pattern_graph,
      optimal_order: input.optimal_order.into_iter().collect(),
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

    // first vertex
    let (vid, _) = self.optimal_order.pop_front().unwrap();
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
        .expand_eids(adj_eids)
        .single_op(VarPrefix::EnumerateTarget.with(&vid))
        .target_var(VarPrefix::DbQueryTarget.with(&vid))
        .build(),
    );
    f_set.insert(vid);

    // other vertices
    for (vid, _) in self.optimal_order.drain(0..) {
      let mut operands = f_set.clone();
      operands.extend(self.pattern_graph.get_adj_vids(&vid));

      let adj_eids = self.pattern_graph.get_adj_eids(&vid);

      // Init -> fx
      if operands.is_empty() {
        instructions.push(
          InstructionBuilder::new(&vid, InstructionType::Init)
            .target_var(VarPrefix::EnumerateTarget.with(&vid))
            .build(),
        );
      }
      // Intersect(Ax, V) -> Cx
      else if operands.len() == 1 {
        let f = operands.iter().next().cloned().unwrap();
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
        let multi_ops = operands
          .iter()
          .map(|vid| VarPrefix::DbQueryTarget.with(vid))
          .collect::<Vec<_>>();
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
      if !operands.is_empty() {
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
          .expand_eids(adj_eids)
          .single_op(VarPrefix::EnumerateTarget.with(&vid))
          .target_var(VarPrefix::DbQueryTarget.with(&vid))
          .build(),
      );

      f_set.insert(vid);
    }

    // Report
    let embedding = f_set
      .iter()
      .map(|vid| VarPrefix::EnumerateTarget.with(vid))
      .collect::<Vec<_>>();
    instructions.push(
      InstructionBuilder::new("", InstructionType::Report)
        .multi_ops(embedding)
        .target_var(VarPrefix::EnumerateTarget.to_string())
        .build(),
    );

    Self::compute_instr_dependencies(&mut instructions);

    dbg!(&instructions);

    self.exec_instructions = Self::remove_unused_dbq(instructions);
  }
}

impl PlanGenerator {
  fn compute_instr_dependencies(instructions: &mut [Instruction]) {
    let mut depend_record: HashMap<Vid, HashSet<Vid>> = HashMap::new();

    for instr in instructions {
      let mut depend_set = HashSet::new();

      if let Some(op) = &instr.single_op {
        if op != VarPrefix::DataVertexSet.as_ref() {
          depend_set.insert(op.clone());
        }
        if let Some(depend) = depend_record.get(op) {
          depend_set.extend(depend.clone());
        }
      } else {
        depend_set.extend(instr.multi_ops.iter().cloned());
        for op in instr.multi_ops.iter() {
          if let Some(depend) = depend_record.get(op) {
            depend_set.extend(depend.clone());
          }
        }
      }

      depend_record.insert(instr.target_var.clone(), depend_set.clone());
      instr.depend_on = depend_set.into_iter().collect();
    }
  }

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
        instr.type_ == InstructionType::GetAdj && !depend_set.contains(&instr.target_var)
      })
      .collect()
  }
}
