use crate::{
  matching_ctx::{MatchingCtx, buckets::FBucket},
  schemas::Instruction,
};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ForeachOperator {
  pub(crate) ctx: Arc<MatchingCtx>,
}

impl ForeachOperator {
  pub async fn execute(&mut self, instr: &Instruction) -> Option<()> {
    println!("\t{instr}");

    let c_bucket = self
      .ctx
      .pop_from_c_block(instr.single_op.as_ref().unwrap())?;

    let f_bucket = FBucket::from_c_bucket(c_bucket).await;

    self.ctx.update_f_block(&instr.target_var, f_bucket);

    Some(())
  }
}
