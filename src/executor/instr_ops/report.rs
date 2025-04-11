use crate::{
  matching_ctx::MatchingCtx,
  schemas::Instruction,
  utils::{dyn_graph::DynGraph, parallel},
};
use hashbrown::HashMap;
use itertools::Itertools;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct ReportOperator {
  pub(crate) ctx: Arc<MatchingCtx>,
}

impl ReportOperator {
  fn is_subset_of_pattern(
    graph: &DynGraph,
    plan_v_pat_cnt: Arc<HashMap<String, usize>>,
    plan_e_pat_cnt: Arc<HashMap<String, usize>>,
  ) -> bool {
    let graph_v_pat_cnt = graph
      .pattern_2_vids
      .iter()
      .map(|(v_pat, vids)| {
        let cnt = vids.len();
        (v_pat.clone(), cnt)
      })
      .collect::<HashMap<_, usize>>();
    let graph_e_pat_cnt = graph
      .pattern_2_eids
      .iter()
      .map(|(e_pat, eids)| {
        let cnt = eids.len();
        (e_pat.clone(), cnt)
      })
      .collect::<HashMap<_, usize>>();

    for (v_pat, cnt) in graph_v_pat_cnt {
      if let Some(&plan_cnt) = plan_v_pat_cnt.get(&v_pat) {
        if cnt > plan_cnt {
          return false;
        }
      } else {
        return false;
      }
    }
    for (e_pat, cnt) in graph_e_pat_cnt {
      if let Some(&plan_cnt) = plan_e_pat_cnt.get(&e_pat) {
        if cnt > plan_cnt {
          return false;
        }
      } else {
        return false;
      }
    }

    true
  }

  pub async fn execute(&mut self, instr: &Instruction) -> Option<()> {
    println!("{instr:#?}\n");

    let (plan_v_pat_cnt, plan_e_pat_cnt) = {
      let plan_v_pat_cnt = self
        .ctx
        .plan_data
        .pattern_vs()
        .keys()
        .map(|v_pat| (v_pat.clone(), 1))
        .collect::<HashMap<_, usize>>();
      let plan_e_pat_cnt = self
        .ctx
        .plan_data
        .pattern_es()
        .keys()
        .map(|e_pat| (e_pat.clone(), 1))
        .collect::<HashMap<_, usize>>();
      (Arc::new(plan_v_pat_cnt), Arc::new(plan_e_pat_cnt))
    };

    let f_buckets = self.ctx.f_block.iter().map(|a| a.clone()).collect_vec();

    let mut filtered_groups = Vec::new();

    // Actually, graph in f_bucket <= pattern graph (is subset of).
    // They will be merged in later `merge` step.
    //
    // So, we can filter out the graph that is real-superset of pattern graph.
    for f_bucket in f_buckets {
      let curr_group = f_bucket.all_matched.into_iter().collect_vec();

      let plan_v_pat_cnt = plan_v_pat_cnt.clone();
      let plan_e_pat_cnt = plan_e_pat_cnt.clone();

      let filtered_group = parallel::spawn_blocking(move || {
        curr_group
          .into_par_iter()
          .filter(|g| Self::is_subset_of_pattern(g, plan_v_pat_cnt.clone(), plan_e_pat_cnt.clone()))
          .collect::<Vec<_>>()
      })
      .await;

      filtered_groups.push(filtered_group);
    }

    // Now, we can update the `grouped_partial_matches` in ctx.
    for curr_group in filtered_groups {
      self.ctx.grouped_partial_matches.push(curr_group);
    }

    Some(())
  }
}
