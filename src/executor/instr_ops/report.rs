use crate::{matching_ctx::MatchingCtx, schemas::Instruction, utils::dyn_graph::DynGraph};
use futures::future;
use hashbrown::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct ReportOperator {
  pub(crate) ctx: Arc<Mutex<MatchingCtx>>,
}

impl ReportOperator {
  pub async fn execute(&mut self, instr: &Instruction) {
    let instr_json = serde_json::to_string_pretty(instr).unwrap();
    println!("{instr_json}\n");

    let could_match_partial_pattern = async |graph: &DynGraph| -> bool {
      let plan_v_pat_cnt = { self.ctx.lock().await }
        .plan_data
        .pattern_vs()
        .keys()
        .map(|v_pat| (v_pat.to_owned(), 1))
        .collect::<HashMap<_, usize>>();
      let plan_e_pat_cnt = { self.ctx.lock().await }
        .plan_data
        .pattern_es()
        .keys()
        .map(|e_pat| (e_pat.to_owned(), 1))
        .collect::<HashMap<_, usize>>();

      let mut graph_v_pat_cnt = HashMap::new();
      let mut graph_e_pat_cnt = HashMap::new();

      for pat in graph.v_patterns.values().cloned() {
        let cnt = graph_v_pat_cnt.entry(pat).or_insert(0);
        *cnt += 1;
      }
      for pat in graph.e_patterns.values().cloned() {
        let cnt = graph_e_pat_cnt.entry(pat).or_insert(0);
        *cnt += 1;
      }

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

    let f_buckets: Vec<_> = {
      let mut ctx = self.ctx.lock().await;
      ctx.f_block.drain().collect()
    };

    let mut filtered_groups = Vec::new();
    for (_, f_bucket) in f_buckets {
      let curr_group = f_bucket.all_matched.into_iter().collect::<Vec<_>>();

      let filtered_future = curr_group.into_iter().map(|g| async {
        let satisfies = could_match_partial_pattern(&g).await;
        if satisfies { Some(g) } else { None }
      });

      let filtered_group = future::join_all(filtered_future)
        .await
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

      filtered_groups.push(filtered_group);
    }

    {
      let mut ctx = self.ctx.lock().await;
      for curr_group in filtered_groups {
        ctx.grouped_partial_matches.push(curr_group);
      }
    }
  }
}
