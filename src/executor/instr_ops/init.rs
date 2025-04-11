use crate::{
  matching_ctx::MatchingCtx,
  schemas::{DataEdge, DataVertex, Instruction},
  storage::StorageAdapter,
  utils::{dyn_graph::DynGraph, parallel},
};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct InitOperator<S: StorageAdapter> {
  pub(crate) storage_adapter: Arc<S>,
  pub(crate) ctx: Arc<MatchingCtx>,
}

impl<S: StorageAdapter> InitOperator<S> {
  pub async fn execute(&mut self, instr: &Instruction) -> Option<()> {
    println!("{instr:#?}\n");

    let pattern_v = self.ctx.get_pattern_v(&instr.vid)?.clone();

    let label = pattern_v.label.as_str();
    let attr = pattern_v.attr.as_ref();

    // load vertices
    let matched_vs = self.storage_adapter.load_v(label, attr).await;

    #[cfg(feature = "trace_init")]
    {
      use crate::schemas::VBase;
      use colored::Colorize;

      println!(
        "✨  Found {} vertices that match: ({})\n",
        matched_vs.len().to_string().yellow(),
        pattern_v.vid().cyan()
      );
    }

    // prepare for updating the block
    let pattern: Arc<str> = pattern_v.vid.as_str().into();
    let target_var: Arc<str> = instr.target_var.as_str().into();
    let pre = parallel::spawn_blocking(move || {
      matched_vs
        .into_par_iter()
        .map(|data_v| {
          let frontier_vid = data_v.vid.clone();
          let mut matched_dg = DynGraph::<DataVertex, DataEdge>::default();
          matched_dg.update_v(data_v, pattern.clone());

          (target_var.clone(), matched_dg, frontier_vid)
        })
        .collect_vec_list()
    })
    .await;

    // update f_block
    for (target_var, matched_dg, frontier_vid) in pre.into_iter().flatten() {
      self
        .ctx
        .append_to_f_block(target_var, matched_dg, &frontier_vid);
    }

    Some(())
  }
}
