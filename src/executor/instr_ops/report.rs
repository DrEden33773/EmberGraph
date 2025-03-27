use crate::{matching_ctx::MatchingCtx, utils::dyn_graph::DynGraph};
use ahash::AHashMap;
use futures::future;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Debug, Clone)]
pub struct ReportOperator {
  pub(crate) ctx: Arc<Mutex<MatchingCtx>>,
}

impl ReportOperator {
  pub async fn execute(&mut self) {
    let could_match_partial_pattern = async |graph: &DynGraph| -> bool {
      if graph.v_entities.len() > { self.ctx.lock().await }.plan_data.pattern_vs().len() {
        return false;
      }
      if graph.e_entities.len() > { self.ctx.lock().await }.plan_data.pattern_es().len() {
        return false;
      }

      let plan_v_pat_cnt = { self.ctx.lock().await }
        .plan_data
        .pattern_vs()
        .keys()
        .map(|v_pat| (v_pat.to_owned(), 1))
        .collect::<AHashMap<_, usize>>();
      let plan_e_pat_cnt = { self.ctx.lock().await }
        .plan_data
        .pattern_es()
        .keys()
        .map(|e_pat| (e_pat.to_owned(), 1))
        .collect::<AHashMap<_, usize>>();

      let graph_v_pat_cnt = graph
        .v_patterns
        .values()
        .map(|v_pat| {
          (
            v_pat.to_owned(),
            graph.v_patterns.values().filter(|v| *v == v_pat).count(),
          )
        })
        .collect::<AHashMap<_, usize>>();
      let graph_e_pat_cnt = graph
        .e_patterns
        .values()
        .map(|e_pat| {
          (
            e_pat.to_owned(),
            graph.e_patterns.values().filter(|v| *v == e_pat).count(),
          )
        })
        .collect::<AHashMap<_, usize>>();

      for (v_pat, cnt) in graph_v_pat_cnt {
        if let Some(plan_cnt) = plan_v_pat_cnt.get(&v_pat) {
          if cnt > *plan_cnt {
            return false;
          }
        } else {
          return false;
        }
      }
      for (e_pat, cnt) in graph_e_pat_cnt {
        if let Some(plan_cnt) = plan_e_pat_cnt.get(&e_pat) {
          if cnt > *plan_cnt {
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
