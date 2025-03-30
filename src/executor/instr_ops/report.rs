use crate::{matching_ctx::MatchingCtx, schemas::Instruction, utils::dyn_graph::DynGraph};
use hashbrown::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct ReportOperator {
  pub(crate) ctx: Arc<Mutex<MatchingCtx>>,
}

impl ReportOperator {
  pub async fn execute(&mut self, instr: &Instruction) -> Option<()> {
    println!("{instr:#?}\n");

    let mut ctx = self.ctx.lock().await;

    let plan_v_pat_cnt = ctx
      .plan_data
      .pattern_vs()
      .keys()
      .map(|v_pat| (v_pat.to_owned(), 1))
      .collect::<HashMap<_, usize>>();
    let plan_e_pat_cnt = ctx
      .plan_data
      .pattern_es()
      .keys()
      .map(|e_pat| (e_pat.to_owned(), 1))
      .collect::<HashMap<_, usize>>();

    let is_subset_of_pattern = |graph: &DynGraph| -> bool {
      let graph_v_pat_cnt = graph
        .pattern_2_vids
        .iter()
        .map(|(v_pat, vids)| {
          let cnt = vids.len();
          (v_pat.to_owned(), cnt)
        })
        .collect::<HashMap<_, usize>>();
      let graph_e_pat_cnt = graph
        .pattern_2_eids
        .iter()
        .map(|(e_pat, eids)| {
          let cnt = eids.len();
          (e_pat.to_owned(), cnt)
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
    };

    let f_buckets: Vec<_> = ctx.f_block.drain().collect();

    let mut filtered_groups = Vec::new();

    // Actually, graph in f_bucket <= pattern graph (is subset of).
    // They will be merged in later `merge` step.
    //
    // So, we can filter out the graph that is real-superset of pattern graph.
    for (_, f_bucket) in f_buckets {
      let curr_group = f_bucket.all_matched.into_iter().collect::<Vec<_>>();

      let filtered_group = curr_group
        .into_iter()
        .filter(&is_subset_of_pattern)
        .collect::<Vec<_>>();

      filtered_groups.push(filtered_group);
    }

    // Now, we can update the `grouped_partial_matches` in ctx.
    for curr_group in filtered_groups {
      ctx.grouped_partial_matches.push(curr_group);
    }

    Some(())
  }
}
