use crate::{
  matching_ctx::MatchingCtx, schemas::*, storage::StorageAdapter, utils::dyn_graph::DynGraph,
};
use futures::future;
use hashbrown::HashMap;
use instr_ops::InstrOperatorFactory;
use itertools::Itertools;
use std::{collections::VecDeque, sync::Arc};
use tokio::sync::Mutex;

pub mod instr_ops;

#[derive(Clone)]
pub struct ExecEngine<S: StorageAdapter> {
  pub(crate) plan_data: PlanData,
  pub(crate) matching_ctx: Arc<Mutex<MatchingCtx>>,
  pub(crate) storage_adapter: Arc<Mutex<S>>,
}

impl<S: StorageAdapter> ExecEngine<S> {
  pub async fn build_from_json(plan_json_content: &str) -> Self {
    let plan_data: PlanData = serde_json::from_str(plan_json_content).unwrap();
    let matching_ctx = Arc::new(Mutex::new(MatchingCtx::new(&plan_data)));
    let storage_adapter = Arc::new(Mutex::new(S::async_default().await));
    Self {
      plan_data,
      matching_ctx,
      storage_adapter,
    }
  }

  pub async fn exec_without_final_merge(&mut self) -> Vec<Vec<DynGraph>> {
    let mut operators = Vec::with_capacity(self.plan_data.instructions.len());
    for instr in self.plan_data.instructions.iter() {
      let operator = InstrOperatorFactory::create(
        instr,
        self.storage_adapter.clone(),
        self.matching_ctx.clone(),
      );
      operators.push(operator);
    }

    for (operator, instr) in operators.iter_mut().zip(self.plan_data.instructions.iter()) {
      operator.execute(instr).await;
    }

    self
      .matching_ctx
      .lock()
      .await
      .grouped_partial_matches
      .drain(0..)
      .collect()
  }

  pub async fn exec(&mut self) -> Vec<DynGraph> {
    let unmerged_results = self.exec_without_final_merge().await;
    if unmerged_results.is_empty() {
      return vec![];
    }

    let mut result = vec![];

    let plan_v_pat_cnt = self
      .plan_data
      .pattern_vs()
      .keys()
      .map(|v_pat| (v_pat.to_owned(), 1))
      .collect::<HashMap<_, usize>>();
    let plan_e_pat_cnt = self
      .plan_data
      .pattern_es()
      .keys()
      .map(|e_pat| (e_pat.to_owned(), 1))
      .collect::<HashMap<_, usize>>();

    let could_match_the_whole_pattern = async |graph: &DynGraph| -> bool {
      if graph.v_entities.len() != self.plan_data.pattern_vs().len() {
        return false;
      }
      if graph.e_entities.len() != self.plan_data.pattern_es().len() {
        return false;
      }

      let graph_v_pat_cnt = graph
        .v_patterns
        .values()
        .map(|v_pat| {
          (
            v_pat.to_owned(),
            graph.v_patterns.values().filter(|v| *v == v_pat).count(),
          )
        })
        .collect::<HashMap<_, usize>>();
      let graph_e_pat_cnt = graph
        .e_patterns
        .values()
        .map(|e_pat| {
          (
            e_pat.to_owned(),
            graph.e_patterns.values().filter(|v| *v == e_pat).count(),
          )
        })
        .collect::<HashMap<_, usize>>();

      graph_v_pat_cnt == plan_v_pat_cnt && graph_e_pat_cnt == plan_e_pat_cnt
    };

    for mut combination in unmerged_results.into_iter().multi_cartesian_product() {
      let mut successors = combination.drain(1..).collect::<VecDeque<_>>();
      let mut curr = combination.pop().unwrap();
      while let Some(next) = successors.pop_front() {
        let new = curr | next;
        curr = new;
      }
      result.push(curr);
    }

    let result_future = result.into_iter().map(|graph| async {
      let satisfies = could_match_the_whole_pattern(&graph).await;
      if satisfies { Some(graph) } else { None }
    });

    future::join_all(result_future)
      .await
      .into_iter()
      .flatten()
      .collect::<Vec<_>>()
  }
}
