use super::resolve_var;
use crate::{
  matching_ctx::{
    MatchingCtx,
    buckets::{CBucket, TBucket},
  },
  schemas::{DataVertex, Instruction, VarPrefix::*},
  storage::StorageAdapter,
};
use itertools::Itertools;
use parking_lot::Mutex;
use std::{collections::VecDeque, sync::Arc};

#[derive(Debug, Clone)]
pub struct IntersectOperator<S: StorageAdapter> {
  pub(crate) storage_adapter: Arc<S>,
  pub(crate) ctx: Arc<Mutex<MatchingCtx>>,
}

impl<S: StorageAdapter> IntersectOperator<S> {
  pub async fn execute(&mut self, instr: &Instruction) -> Option<()> {
    println!("{instr:#?}\n");

    if instr.is_single_op() {
      let (var_prefix, _) = resolve_var(instr.single_op.as_ref().unwrap());
      match var_prefix {
        DbQueryTarget => self.with_adj_set(instr).await,
        IntersectTarget => self.with_temp_intersected(instr).await,
        _ => panic!("Invalid var_prefix: {var_prefix}"),
      }
    } else {
      self.with_multi_adj_set(instr).await
    }
  }

  /// `Vi` ∩ `Ax` -> `Cy`
  async fn with_adj_set(&mut self, instr: &Instruction) -> Option<()> {
    let loaded_v_pat_pairs = self.load_vertices(instr).await?;

    let a_group = { self.ctx.lock() }
      .pop_group_by_pat_from_a_block(instr.single_op.as_ref().unwrap(), &instr.vid)?;

    let c_bucket = CBucket::build_from_a_group(a_group, loaded_v_pat_pairs).await;

    { self.ctx.lock() }.update_c_block(&instr.target_var, c_bucket);

    Some(())
  }

  /// `A(T)_{i}` ∩ `A_{i+1}` -> `Tx`
  async fn with_multi_adj_set(&mut self, instr: &Instruction) -> Option<()> {
    let mut a_groups: VecDeque<_> = {
      let mut ctx = self.ctx.lock();
      instr
        .multi_ops
        .iter()
        .filter_map(|op| ctx.pop_group_by_pat_from_a_block(op, &instr.vid))
        .collect()
    };

    if a_groups.len() < 2 {
      return None;
    }

    let a1 = a_groups.pop_front().unwrap();
    let a2 = a_groups.pop_front().unwrap();
    let mut t_bucket = TBucket::build_from_a_a(a1, a2, &instr.vid).await;

    if a_groups.len() > 2 {
      let mut prev_t = t_bucket;
      while let Some(a_group) = a_groups.pop_front() {
        t_bucket = TBucket::build_from_t_a(prev_t, a_group).await;
        prev_t = t_bucket;
      }
      t_bucket = prev_t;
    }

    { self.ctx.lock() }.update_t_block(&instr.target_var, t_bucket);

    Some(())
  }

  /// `Vi` ∩ `Tx` -> `Cy`
  async fn with_temp_intersected(&mut self, instr: &Instruction) -> Option<()> {
    let loaded_v_pat_pairs = self.load_vertices(instr).await?;

    let t_bucket = { self.ctx.lock() }.pop_from_t_block(instr.single_op.as_ref().unwrap())?;

    let c_bucket = CBucket::build_from_t(t_bucket, loaded_v_pat_pairs).await;

    { self.ctx.lock() }.update_c_block(&instr.target_var, c_bucket);

    Some(())
  }

  async fn load_vertices(&self, instr: &Instruction) -> Option<Vec<(DataVertex, String)>> {
    let pattern_v = { self.ctx.lock() }.get_pattern_v(&instr.vid)?.clone();

    let label = pattern_v.label.as_str();
    let attr = pattern_v.attr.as_ref();
    let matched_vs = self.storage_adapter.load_v(label, attr).await;

    matched_vs
      .into_iter()
      .map(|v| (v, pattern_v.vid.clone()))
      .collect_vec()
      .into()
  }
}
