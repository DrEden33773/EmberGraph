use super::resolve_var;
use crate::{
  matching_ctx::{MatchingCtx, buckets::ABucket},
  schemas::Instruction,
  storage::StorageAdapter,
};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct GetAdjOperator<S: StorageAdapter> {
  pub(crate) storage_adapter: Arc<S>,
  pub(crate) ctx: Arc<Mutex<MatchingCtx>>,
}

impl<S: StorageAdapter> GetAdjOperator<S> {
  pub async fn execute(&mut self, instr: &Instruction) -> Option<()> {
    println!("{instr:#?}\n");

    // to resolve current `pattern_vid`
    let (_, curr_pat_vid) = resolve_var(instr.single_op.as_ref().unwrap());

    let f_bucket = { self.ctx.lock().await }.pop_from_f_block(instr.single_op.as_ref().unwrap())?;
    let mut a_bucket = ABucket::from_f_bucket(f_bucket, curr_pat_vid);

    let connected_data_vids = {
      let ctx = self.ctx.lock().await;

      let pattern_vs = ctx.pattern_vs();
      let pattern_es = ctx.fetch_pattern_e_batch(instr.expand_eids.iter().map(String::as_str));

      // core logic: incremental load new edges
      a_bucket
        .incremental_load_new_edges(pattern_es, pattern_vs, self.storage_adapter.as_ref())
        .await
    };

    // update the `block` and `extended data vid set`
    {
      let mut ctx = self.ctx.lock().await;
      ctx.update_a_block(&instr.target_var, a_bucket);
      ctx.update_extended_data_vids(connected_data_vids);
    }

    Some(())
  }
}
