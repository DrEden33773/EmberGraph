use crate::{
  matching_ctx::MatchingCtx,
  result_dump::ResultDumper,
  schemas::*,
  storage::{AdvancedStorageAdapter, CachedStorageAdapter, TestOnlyStorageAdapter},
  utils::{dyn_graph::DynGraph, parallel},
};
use colored::Colorize;
use hashbrown::HashMap;
use instr_ops::InstrOperatorFactory;
use itertools::Itertools;
use project_root::get_project_root;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use std::{
  collections::VecDeque,
  path::PathBuf,
  sync::{Arc, LazyLock},
};
use tokio::{fs, io};

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

  async fn exec_helper(&mut self, unmerged_results: Vec<Vec<DynGraph>>) -> Vec<DynGraph> {
    fn preview_scale(unmerged: &[Vec<DynGraph>]) {
      let len_vec = unmerged.iter().map(|v| v.len()).collect_vec();
      println!();
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

  pub async fn exec(&mut self) -> Vec<DynGraph> {
    let unmerged_results = self
      .exec_without_final_merge()
      .await
      .into_iter()
      .filter(|v| !v.is_empty())
      .collect_vec();

    self.exec_helper(unmerged_results).await
  }
}

impl<S: AdvancedStorageAdapter + 'static> ExecEngine<S> {
  pub async fn parallel_exec(&mut self) -> Vec<DynGraph> {
    let unmerged_results = self
      .parallel_exec_without_final_merge()
      .await
      .into_iter()
      .filter(|v| !v.is_empty())
      .collect_vec();

    self.exec_helper(unmerged_results).await
  }

  pub async fn parallel_exec_without_final_merge(&mut self) -> Vec<Vec<DynGraph>> {
    let layers = self.build_dependency_layers();

    if layers.is_none() {
      return self.exec_without_final_merge().await;
    }

    let layers = layers.unwrap();

    // execute the instructions in parallel (by layer)
    for (layer_idx, layer) in layers.iter().enumerate() {
      if layer_idx != 0 {
        println!();
      }

      println!(
        "🚀  Executing {}-{}: {}",
        "layer".yellow(),
        layer_idx.to_string().yellow(),
        format!("{:?}", layer).blue()
      );

      let mut handles = Vec::with_capacity(layer.len());

      for &instr_idx in layer {
        let instr = self.plan_data.instructions[instr_idx].clone();
        let matching_ctx = self.matching_ctx.clone();
        let storage_adapter = self.storage_adapter.clone();

        let handle = tokio::spawn(async move {
          let mut operator = InstrOperatorFactory::create(&instr, storage_adapter, matching_ctx);
          operator.execute(&instr).await;
        });

        handles.push(handle);
      }

      for handle in handles {
        if let Err(e) = handle.await {
          eprintln!("❌  Error executing instruction: {}", e);
        }
      }
    }

    let mut result = Vec::with_capacity(self.matching_ctx.grouped_partial_matches.len());
    while let Some(matched_graphs) = self.matching_ctx.grouped_partial_matches.pop() {
      result.push(matched_graphs);
    }

    result
  }

  fn build_dependency_layers(&self) -> Option<Vec<Vec<usize>>> {
    let instructions = &self.plan_data.instructions;
    let n = instructions.len();

    // build dependency graph
    let mut incoming_edges = vec![0; n];
    let mut graph = vec![vec![]; n];

    // build `target_var -> index` map
    let mut var_to_idx = HashMap::new();
    for (idx, instr) in instructions.iter().enumerate() {
      var_to_idx.insert(instr.target_var.clone(), idx);
    }

    // fill the dependency graph
    for (idx, instr) in instructions.iter().enumerate() {
      for dep_var in &instr.depend_on {
        if let Some(&dep_idx) = var_to_idx.get(dep_var) {
          graph[dep_idx].push(idx); // `dep_idx` is dependent on `idx`
          incoming_edges[idx] += 1; // increase the incoming edge count for `idx`
        }
      }
    }

    // Kahn's algorithm for topological sorting
    let mut layers = vec![];
    let mut queue = VecDeque::new();

    // init the queue with all nodes on which incoming edges = 0
    for (i, &incoming_edge) in incoming_edges.iter().enumerate().take(n) {
      if incoming_edge == 0 {
        queue.push_back(i);
      }
    }

    while !queue.is_empty() {
      let mut current_layer = vec![];
      let layer_size = queue.len();

      // iterate over each node of current_layer
      for _ in 0..layer_size {
        let node = queue.pop_front().unwrap();
        current_layer.push(node);

        // decrease the incoming edge count for all dependent nodes
        for &dependent in &graph[node] {
          incoming_edges[dependent] -= 1;

          // if incoming_edges[dependent] == 0, then add to queue
          if incoming_edges[dependent] == 0 {
            queue.push_back(dependent);
          }
        }
      }

      // add current_layer to layers
      if !current_layer.is_empty() {
        layers.push(current_layer);
      }
    }

    // check for cycles
    if layers.iter().map(|layer| layer.len()).sum::<usize>() < n {
      eprintln!("⚠️  The plan contains a cycle. Fallback to sequential execution.");
      return None;
    }

    Some(layers)
  }
}

static PLAN_ROOT: LazyLock<PathBuf> =
  LazyLock::new(|| get_project_root().unwrap().join("resources").join("plan"));

pub async fn query<S: AdvancedStorageAdapter + 'static>(plan_filename: &str) -> io::Result<()> {
  let mut path = PLAN_ROOT.clone();
  path.push(plan_filename);
  let plan_json_content = fs::read_to_string(path).await?;

  let result = ExecEngine::<CachedStorageAdapter<S>>::build_from_json(&plan_json_content)
    .await
    .parallel_exec()
    .await;

  println!("✨  Count(result) = {}", result.len());

  if let Some(df) = ResultDumper::new(result).to_simplified_df() {
    println!("{}", df);
  }

  Ok(())
}
