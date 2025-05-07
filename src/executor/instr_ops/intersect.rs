use super::resolve_var;
use crate::{
  matching_ctx::{
    MatchingCtx,
    buckets::{CBucket, TBucket},
  },
  schemas::{DataVertex, Instruction, VBase, VarPrefix::*},
  storage::StorageAdapter,
  utils::parallel,
};
use itertools::Itertools;
use rayon::slice::ParallelSliceMut;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct IntersectOperator<S: StorageAdapter + 'static> {
  pub(crate) storage_adapter: Arc<S>,
  pub(crate) ctx: Arc<MatchingCtx>,
}

impl<S: StorageAdapter + 'static> IntersectOperator<S> {
  pub async fn execute(&mut self, instr: &Instruction) -> Option<()> {
    #[cfg(not(feature = "benchmark"))]
    println!("\t{instr}");

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
    let a_group = self
      .ctx
      .pop_group_by_pat_from_a_block(instr.single_op.as_ref().unwrap(), &instr.vid)?;

    #[cfg(not(feature = "lazy_load_v"))]
    let c_bucket = {
      let loaded_v_pat_pairs_asc = self.load_vertices_asc(instr).await?;
      CBucket::build_from_a_group(a_group, loaded_v_pat_pairs_asc).await
    };
    #[cfg(feature = "lazy_load_v")]
    let c_bucket = {
      let pattern_v = self.ctx.get_pattern_v(&instr.vid)?.clone();
      let pattern_vid = pattern_v.vid.clone();
      let expected_label = pattern_v.label.clone();
      let expected_attr = pattern_v.attr.clone();
      CBucket::build_from_a_group_lazy(
        a_group,
        pattern_vid.into(),
        expected_label.into(),
        expected_attr.map(Arc::new),
        self.storage_adapter.clone(),
      )
      .await
    };

    self.ctx.update_c_block(&instr.target_var, c_bucket);

    Some(())
  }

  /// `Ai / Ti` ∩ `Aj / Tj` -> `Tx`
  async fn with_multi_adj_set(&mut self, instr: &Instruction) -> Option<()> {
    #[cfg(debug_assertions)]
    // Assume that `flatten` has been applied
    assert_eq!(instr.multi_ops.len(), 2);

    let (lhs_pref, _) = resolve_var(instr.multi_ops[0].as_str());
    let (rhs_pref, _) = resolve_var(instr.multi_ops[1].as_str());

    let t_bucket = match lhs_pref {
      DbQueryTarget => {
        // `Ai` ∩ `Aj / Tj` -> `Tx`
        let lhs_a_group = self
          .ctx
          .pop_group_by_pat_from_a_block(instr.multi_ops[0].as_str(), &instr.vid)?;
        match rhs_pref {
          DbQueryTarget => {
            // `Ai` ∩ `Aj` -> `Tx`
            let rhs_a_group = self
              .ctx
              .pop_group_by_pat_from_a_block(instr.multi_ops[1].as_str(), &instr.vid)?;
            TBucket::build_from_a_a(lhs_a_group, rhs_a_group, &instr.vid).await
          }
          IntersectTarget => {
            // `Ai` ∩ `Tj` -> `Tx`
            let rhs_t_group = self.ctx.pop_from_t_block(instr.multi_ops[1].as_str())?;
            TBucket::build_from_a_t(lhs_a_group, rhs_t_group).await
          }
          _ => panic!("❌  Invalid var_prefix: {rhs_pref}"),
        }
      }
      IntersectTarget => {
        // `Ti` ∩ `Aj / Tj` -> `Tx`
        let lhs_t_group = self.ctx.pop_from_t_block(instr.multi_ops[0].as_str())?;
        match rhs_pref {
          DbQueryTarget => {
            // `Ti` ∩ `Aj` -> `Tx`
            let rhs_a_group = self
              .ctx
              .pop_group_by_pat_from_a_block(instr.multi_ops[1].as_str(), &instr.vid)?;
            TBucket::build_from_t_a(lhs_t_group, rhs_a_group).await
          }
          IntersectTarget => {
            // `Ti` ∩ `Tj` -> `Tx`
            let rhs_t_group = self.ctx.pop_from_t_block(instr.multi_ops[1].as_str())?;
            TBucket::build_from_t_t(lhs_t_group, rhs_t_group).await
          }
          _ => panic!("❌  Invalid var_prefix: {rhs_pref}"),
        }
      }
      _ => panic!("❌  Invalid var_prefix: {lhs_pref}"),
    };

    self.ctx.update_t_block(&instr.target_var, t_bucket);

    Some(())
  }

  /// `Vi` ∩ `Tx` -> `Cy`
  async fn with_temp_intersected(&mut self, instr: &Instruction) -> Option<()> {
    let t_bucket = self
      .ctx
      .pop_from_t_block(instr.single_op.as_ref().unwrap())?;

    #[cfg(not(feature = "lazy_load_v"))]
    let c_bucket = {
      let loaded_v_pat_pairs_asc = self.load_vertices_asc(instr).await?;
      CBucket::build_from_t(t_bucket, loaded_v_pat_pairs_asc).await
    };
    #[cfg(feature = "lazy_load_v")]
    let c_bucket = {
      let pattern_v = self.ctx.get_pattern_v(&instr.vid)?.clone();
      let pattern_vid = pattern_v.vid.clone();
      let expected_label = pattern_v.label.clone();
      let expected_attr = pattern_v.attr.clone();
      CBucket::build_from_t_lazy(
        t_bucket,
        pattern_vid.into(),
        expected_label.into(),
        expected_attr.map(Arc::new),
        self.storage_adapter.clone(),
      )
      .await
    };

    self.ctx.update_c_block(&instr.target_var, c_bucket);

    Some(())
  }

  #[allow(dead_code)]
  async fn load_vertices_asc(&self, instr: &Instruction) -> Option<Vec<(DataVertex, String)>> {
    let pattern_v = self.ctx.get_pattern_v(&instr.vid)?.clone();
    let pattern_vid = pattern_v.vid.clone();

    let label = pattern_v.label.as_str();
    let attr = pattern_v.attr.as_ref();
    let matched_vs = self.storage_adapter.load_v(label, attr).await;

    let mut raw = matched_vs
      .into_iter()
      .map(|v| (v, pattern_vid.clone()))
      .collect_vec();

    let sorted = parallel::spawn_blocking(move || {
      raw.par_sort_unstable_by(|(v1, _), (v2, _)| v1.vid().cmp(v2.vid()));
      raw
    })
    .await;

    sorted.into()
  }
}
