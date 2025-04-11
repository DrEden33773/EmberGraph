use crate::{
  matching_ctx::MatchingCtx,
  schemas::*,
  storage::{AdvancedStorageAdapter, TestOnlyStorageAdapter},
  utils::{dyn_graph::DynGraph, parallel},
};
use hashbrown::HashMap;
use instr_ops::InstrOperatorFactory;
use itertools::Itertools;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::sync::Arc;

pub mod instr_ops;

#[derive(Clone)]
pub struct ExecEngine<S: AdvancedStorageAdapter> {
  pub(crate) plan_data: Arc<PlanData>,
  pub(crate) storage_adapter: Arc<S>,
  pub(crate) matching_ctx: Arc<MatchingCtx>,
}

impl<S: TestOnlyStorageAdapter> ExecEngine<S> {
  pub async fn build_test_only_from_json(plan_json_content: &str) -> Self {
    let plan_data: PlanData = serde_json::from_str(plan_json_content).unwrap();
    let plan_data = Arc::new(plan_data);
    let storage_adapter = Arc::new(S::async_init_test_only().await);
    let matching_ctx = Arc::new(MatchingCtx::new(plan_data.clone()));
    Self {
      plan_data,
      storage_adapter,
      matching_ctx,
    }
  }
}

impl<S: AdvancedStorageAdapter + 'static> ExecEngine<S> {
  pub async fn build_from_json(plan_json_content: &str) -> Self {
    let plan_data: PlanData = serde_json::from_str(plan_json_content).unwrap();
    let plan_data = Arc::new(plan_data);
    let storage_adapter = Arc::new(S::async_default().await);
    let matching_ctx = Arc::new(MatchingCtx::new(plan_data.clone()));
    Self {
      plan_data,
      storage_adapter,
      matching_ctx,
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

    let mut result = Vec::with_capacity(self.matching_ctx.grouped_partial_matches.len());
    while let Some(matched_graphs) = self.matching_ctx.grouped_partial_matches.pop() {
      result.push(matched_graphs);
    }

    result
  }

  fn is_equivalent_to_pattern(
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

    graph_v_pat_cnt == *plan_v_pat_cnt && graph_e_pat_cnt == *plan_e_pat_cnt
  }

  pub async fn exec(&mut self) -> Vec<DynGraph> {
    let unmerged_results = self
      .exec_without_final_merge()
      .await
      .into_iter()
      .filter(|v| !v.is_empty())
      .collect_vec();

    fn preview_scale(unmerged: &[Vec<DynGraph>]) {
      let len_vec = unmerged.iter().map(|v| v.len()).collect_vec();
      println!("✨  Scale(unmerged_results) = {len_vec:?}\n");
    }

    preview_scale(&unmerged_results);

    if unmerged_results.is_empty() {
      return vec![];
    }

    let plan_v_pat_cnt = self
      .plan_data
      .pattern_vs()
      .keys()
      .map(|v_pat| (v_pat.clone(), 1))
      .collect::<HashMap<_, usize>>();
    let plan_e_pat_cnt = self
      .plan_data
      .pattern_es()
      .keys()
      .map(|e_pat| (e_pat.clone(), 1))
      .collect::<HashMap<_, usize>>();
    let plan_v_pat_cnt = Arc::new(plan_v_pat_cnt);
    let plan_e_pat_cnt = Arc::new(plan_e_pat_cnt);

    // get all combinations of unmerged results
    let combinations = unmerged_results
      .into_iter()
      .multi_cartesian_product()
      .collect_vec();

    parallel::spawn_blocking(move || {
      combinations
        .into_par_iter()
        .map(|combination| {
          combination
            .into_iter()
            .reduce(|acc, curr| acc | curr)
            .unwrap()
        })
        .filter(|graph| {
          Self::is_equivalent_to_pattern(graph, plan_v_pat_cnt.clone(), plan_e_pat_cnt.clone())
        })
        .collect::<Vec<_>>()
    })
    .await
  }
}
