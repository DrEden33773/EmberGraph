use super::resolve_var;
use crate::{
  matching_ctx::{MatchingCtx, buckets::ABucket},
  schemas::Instruction,
  storage::AdvancedStorageAdapter,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct GetAdjOperator<S: AdvancedStorageAdapter> {
  pub(crate) storage_adapter: Arc<S>,
  pub(crate) ctx: Arc<MatchingCtx>,
}

impl<S: AdvancedStorageAdapter + 'static> GetAdjOperator<S> {
  pub async fn execute(&mut self, instr: &Instruction) -> Option<()> {
    #[cfg(not(feature = "benchmark"))]
    println!("\t{instr}");

    // to resolve current `pattern_vid`
    let (_, curr_pat_vid) = resolve_var(instr.single_op.as_ref().unwrap());

    let f_bucket = self
      .ctx
      .pop_from_f_block(instr.single_op.as_ref().unwrap())?;
    let mut a_bucket = ABucket::from_f_bucket(f_bucket, curr_pat_vid);

    let (pattern_vs, pattern_es) = {
      let pattern_vs = self.ctx.pattern_vs().clone();
      let pattern_es = self
        .ctx
        .fetch_pattern_e_batch(instr.expand_eids.iter().map(String::as_str));
      (pattern_vs, pattern_es)
    };

    // core logic: incremental load new edges
    a_bucket
      .incremental_load_new_edges(pattern_es, pattern_vs, self.storage_adapter.clone())
      .await;

    // update the `block` and `extended data vid set`
    self.ctx.update_a_block(&instr.target_var, a_bucket);

    Some(())
  }
}
