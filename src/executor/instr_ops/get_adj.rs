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
  pub(crate) storage_adapter: Arc<Mutex<S>>,
  pub(crate) ctx: Arc<Mutex<MatchingCtx>>,
}

impl<S: StorageAdapter> GetAdjOperator<S> {
  pub async fn execute(&mut self, instr: &Instruction) {
    let instr_json = serde_json::to_string_pretty(instr).unwrap();
    println!("{instr_json}\n");

    let (_, curr_pat_vid) = resolve_var(instr.single_op.as_ref().unwrap());

    let pattern_es =
      { self.ctx.lock().await }.fetch_pattern_e_batch(instr.expand_eids.iter().map(String::as_str));
    let pattern_vs = { self.ctx.lock().await }.pattern_vs().to_owned();

    let f_bucket = { self.ctx.lock().await }.pop_from_f_block(instr.single_op.as_ref().unwrap());
    if f_bucket.is_none() {
      println!(
        "No 'f_bucket' found for '{}'\n",
        instr.single_op.as_ref().unwrap()
      );
      return;
    }
    let mut a_bucket = ABucket::from_f_bucket(f_bucket.unwrap(), curr_pat_vid);

    { self.ctx.lock().await }.init_a_block(&instr.target_var);

    let connected_data_vids = {
      let storage_adapter = self.storage_adapter.lock().await;
      a_bucket
        .incremental_load_new_edges(pattern_es, &pattern_vs, &*storage_adapter)
        .await
    };

    { self.ctx.lock().await }.update_a_block(&instr.target_var, a_bucket);
    { self.ctx.lock().await }.update_extended_data_vids(connected_data_vids);
  }
}
