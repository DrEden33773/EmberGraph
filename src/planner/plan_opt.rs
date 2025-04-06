use super::plan_gen::PlanGenerator;
use crate::{
  schemas::{
    Instruction, InstructionBuilder, InstructionType, PatternEdge, PatternVertex, VarPrefix, Vid,
  },
  utils::{apriori::AprioriBuilder, dyn_graph::DynGraph},
};
use hashbrown::{HashMap, HashSet};
use itertools::Itertools;
use std::collections::{BTreeSet, VecDeque};

pub struct PlanOptimizer {
  pub(crate) pattern_graph: DynGraph<PatternVertex, PatternEdge>,
  pub(crate) exec_instructions: Vec<Instruction>,
  pub(crate) matching_order: Vec<Vid>,

  ti: usize,
}

impl From<PlanGenerator> for PlanOptimizer {
  fn from(plan_generator: PlanGenerator) -> Self {
    Self {
      pattern_graph: plan_generator.pattern_graph,
      exec_instructions: plan_generator.exec_instructions,
      matching_order: plan_generator.optimal_order,
      ti: 0,
    }
  }
}

impl PlanOptimizer {
  pub fn apply_optimization(&mut self) {
    if self.exec_instructions.is_empty() {
      return;
    }

    self.eliminate_cse();
    // self.flatten_multi_ops();
    self.reorder();
  }

  fn reorder(&mut self) {
    let mut certain_set = HashSet::new();
    certain_set.insert(self.exec_instructions[0].target_var.clone());

    for i in 1..self.exec_instructions.len() {
      let mut candidates = vec![];

      // find all instructions that depend on `certain_set`
      for j in i..self.exec_instructions.len() {
        let target_var = &self.exec_instructions[j].target_var;
        if certain_set.contains(target_var) {
          candidates.push(j);
        }
      }

      // select the instruction with minimum cost
      let mut instr_pos = None;
      if !candidates.is_empty() {
        instr_pos = Some(candidates[0]);
        for &idx in &candidates[1..] {
          if self.exec_instructions[idx]
            .type_
            .compare(self.exec_instructions[instr_pos.unwrap()].type_)
            < 0
          {
            instr_pos = Some(idx);
          }
        }
      }

      // move
      if let Some(instr_pos) = instr_pos {
        certain_set.insert(self.exec_instructions[instr_pos].target_var.clone());
        self.exec_instructions.swap(i, instr_pos);
      }

      certain_set.insert(self.exec_instructions[i].target_var.clone());
    }
  }

  /// Flatten multi-ops (at most 2 operands)
  #[deprecated(
    since = "0.1.0",
    note = "ExecEngine supports `Intersect(A1 ... An)`, flatten may break the parallelism."
  )]
  #[allow(dead_code)]
  fn flatten_multi_ops(&mut self) {
    let mut instr_idx = Vec::with_capacity(self.exec_instructions.len());
    let mut defined_vars = Vec::with_capacity(self.exec_instructions.len());

    for (idx, instr) in self.exec_instructions.iter().enumerate() {
      defined_vars.push(instr.target_var.clone());
      // Only case: type_ == Intersect
      if instr.type_ == InstructionType::Intersect && instr.multi_ops.len() > 2 {
        instr_idx.push(idx);
      }
    }

    let mut offset = 0;
    for idx in instr_idx {
      // mark pos before instr, since `add` and `remove` will change position
      let mut pos = idx + offset - 1;
      let mut operators = self.exec_instructions[pos + 1]
        .multi_ops
        .iter()
        .cloned()
        .collect::<VecDeque<_>>();
      operators.retain(|var| defined_vars.contains(var));

      while operators.len() > 2 {
        let op1 = operators.pop_back().unwrap();
        let op2 = operators.pop_back().unwrap();
        let new_operators = vec![op1, op2];

        self.ti += 1;
        operators.push_front(VarPrefix::IntersectTarget.with(format!("@{}", self.ti)));

        let old_instr = &self.exec_instructions[pos + 1];
        let new_instr = InstructionBuilder::new(&old_instr.vid, InstructionType::Intersect)
          .multi_ops(new_operators)
          .target_var(VarPrefix::IntersectTarget.with(format!("@{}", self.ti)))
          .build();

        pos += 1;
        self.exec_instructions.insert(pos, new_instr);
        offset += 1;
      }

      let new_operators = operators.into_iter().collect_vec();
      pos += 1;
      self.exec_instructions[pos].multi_ops = new_operators;
    }
  }

  /// Eliminate common sub-expressions (CSE)
  fn eliminate_cse(&mut self) {
    loop {
      let mut data_list = vec![];
      let mut instr_idx = vec![];
      let mut intersect_pos = HashMap::new();

      // find `intersect`
      for (idx, instr) in self.exec_instructions.iter().enumerate() {
        if instr.type_ == InstructionType::GetAdj {
          intersect_pos.insert(instr.target_var.clone(), idx);
        } else if instr.type_ == InstructionType::Intersect && !instr.is_single_op() {
          intersect_pos.insert(instr.target_var.clone(), idx);

          let operands = instr.multi_ops.iter().cloned().collect::<HashSet<_>>();
          data_list.push(operands);
          instr_idx.push(idx);
        }
      }

      // use `apriori` to find all frequent itemset (support >= 2)
      let apriori = AprioriBuilder::new(&data_list).min_support(2).build();
      let freq_set = apriori.gen_max_size_freq_set();

      // find MAX frequent itemset (support IS MAX)
      let mut max_freq_set = BTreeSet::new();
      let mut max_freq_support = 0;
      for (itemset, support) in freq_set.into_iter() {
        if support > max_freq_support {
          max_freq_set = itemset;
          max_freq_support = support;
        } else if support == max_freq_support {
          let l1 = max_freq_set
            .iter()
            .map(|var| intersect_pos[var])
            .sorted_unstable()
            .collect_vec();
          let l2 = itemset
            .iter()
            .map(|var| intersect_pos[var])
            .sorted_unstable()
            .collect_vec();
          for (e1, e2) in l1.iter().zip(l2.iter()) {
            if e1 > e2 {
              max_freq_support = support;
              max_freq_set = itemset.clone();
            }
          }
        }
      }

      // don't forget to transmute BTreeSet to HashSet
      let max_freq_set = max_freq_set.into_iter().collect::<HashSet<_>>();

      // eliminate CSE
      if max_freq_set.len() >= 2 {
        self.ti += 1;
        // if `frequent itemset` appeared more than twice, this will be false
        let mut flag = true;

        for (i, mut operands) in data_list.into_iter().enumerate() {
          if !max_freq_set.is_subset(&operands) {
            continue;
          }

          operands.insert(VarPrefix::IntersectTarget.with(self.ti));
          operands.retain(|item| !max_freq_set.contains(item));

          if flag {
            let operators = max_freq_set
              .iter()
              .cloned()
              .sorted_unstable_by_key(|s| intersect_pos[s])
              .collect_vec();
            let old_instr = &self.exec_instructions[instr_idx[i]];
            let new_instr = InstructionBuilder::new(&old_instr.vid, InstructionType::Intersect)
              .multi_ops(operators)
              .target_var(VarPrefix::IntersectTarget.with(self.ti))
              .build();
            self.exec_instructions.insert(instr_idx[i], new_instr);
            flag = false;
          }

          if operands.len() > 1 {
            self.exec_instructions[instr_idx[i] + 1].multi_ops = operands.into_iter().collect();
          } else {
            self.exec_instructions[instr_idx[i] + 1].single_op = operands.into_iter().next();
          }
        }
      } else {
        break;
      }
    }
  }
}
