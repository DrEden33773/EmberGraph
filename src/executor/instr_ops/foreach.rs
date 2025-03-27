use crate::{
  matching_ctx::{MatchingCtx, buckets::FBucket},
  schemas::Instruction,
};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct ForeachOperator {
  pub(crate) ctx: Arc<Mutex<MatchingCtx>>,
}

impl ForeachOperator {
  pub async fn execute(&mut self, instr: Instruction) {
    { self.ctx.lock().await }.init_f_block(&instr.target_var);

    let c_bucket = { self.ctx.lock().await }.pop_from_c_block(instr.single_op.as_ref().unwrap());
    if c_bucket.is_none() {
      return;
    }

    let f_bucket = FBucket::from_c_bucket(c_bucket.unwrap()).await;
    { self.ctx.lock().await }.update_f_block(&instr.target_var, f_bucket);
  }
}
