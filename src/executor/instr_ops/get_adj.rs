use super::resolve_var;
use crate::{
  matching_ctx::{MatchingCtx, buckets::ABucket},
  schemas::Instruction,
  storage::AdvancedStorageAdapter,
};
use parking_lot::Mutex;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct GetAdjOperator<S: AdvancedStorageAdapter> {
  pub(crate) storage_adapter: Arc<S>,
  pub(crate) ctx: Arc<Mutex<MatchingCtx>>,
}

impl<S: AdvancedStorageAdapter> GetAdjOperator<S> {
  pub async fn execute(&mut self, instr: &Instruction) -> Option<()> {
    println!("{instr:#?}\n");

    // to resolve current `pattern_vid`
    let (_, curr_pat_vid) = resolve_var(instr.single_op.as_ref().unwrap());

    let f_bucket = { self.ctx.lock() }.pop_from_f_block(instr.single_op.as_ref().unwrap())?;
    let mut a_bucket = ABucket::from_f_bucket(f_bucket, curr_pat_vid);

    let (pattern_vs, pattern_es) = {
      let ctx = self.ctx.lock();
      let pattern_vs = ctx.pattern_vs().clone();
      let pattern_es = ctx.fetch_pattern_e_batch(instr.expand_eids.iter().map(String::as_str));
      (pattern_vs, pattern_es)
    };

    // core logic: incremental load new edges
    let connected_data_vids = a_bucket
      .incremental_load_new_edges(pattern_es, &pattern_vs, self.storage_adapter.as_ref())
      .await;

    // update the `block` and `extended data vid set`
    {
      let mut ctx = self.ctx.lock();
      ctx.update_a_block(&instr.target_var, a_bucket);
      ctx.update_formalized_data_vids(connected_data_vids);
    }

    Some(())
  }
}
