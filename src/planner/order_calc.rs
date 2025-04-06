use crate::{
  schemas::{Label, Op, PatternEdge, PatternVertex, Vid},
  utils::dyn_graph::DynGraph,
};
use hashbrown::HashMap;
use ordered_float::OrderedFloat;
use project_root::get_project_root;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fs::File, path::PathBuf, sync::LazyLock};

static STAT_FILEPATH: LazyLock<PathBuf> = LazyLock::new(|| {
  get_project_root()
    .unwrap()
    .join("resources")
    .join("statistics")
    .join("label_statistics.json")
});

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Statistics {
  v_cnt: usize,
  e_cnt: usize,
  v_label_cnt: HashMap<Label, usize>,
  e_label_cnt: HashMap<Label, usize>,
}

#[derive(Debug, Clone)]
pub struct OrderCalculator {
  statistics: Statistics,
  pattern_graph: DynGraph<PatternVertex, PatternEdge>,
  cost_2_vids: BTreeMap<usize, Vec<Vid>>,
  order: Vec<Vid>,
  override_v_cost: HashMap<Vid, usize>,
}

#[derive(Debug, Clone)]
pub struct PlanGenInput {
  pub(crate) pattern_graph: DynGraph<PatternVertex, PatternEdge>,
  pub(crate) optimal_order: Vec<Vid>,
}

impl OrderCalculator {
  pub fn new(pattern_graph: DynGraph<PatternVertex, PatternEdge>) -> Self {
    let statistics: Statistics = serde_json::from_reader(
      File::open(STAT_FILEPATH.as_path()).expect("❌  Failed to open statistics file"),
    )
    .expect("❌  Failed to parse statistics file");

    let raw_order = pattern_graph
      .view_vids()
      .into_iter()
      .map(String::from)
      .collect::<Vec<_>>();

    Self {
      statistics,
      pattern_graph,
      cost_2_vids: BTreeMap::default(),
      order: raw_order,
      override_v_cost: HashMap::default(),
    }
  }

  /// Core logic: Rule-based estimation
  fn rule_based_estimation(&mut self) {
    let mut v_scores = HashMap::new();

    for (vid, v) in self.pattern_graph.v_entities.iter() {
      let mut score = 0.0;

      // 1. selectivity (only if `attr` is not None)
      if let Some(ref attr) = v.attr {
        // Eq is tent to select the least number of vertices
        score += if attr.op == Op::Eq {
          // don't forget to update `override_v_cost`
          self.override_v_cost.insert(vid.clone(), 1);
          100.0
        }
        // Ne is tent to select the most number of vertices
        else if attr.op == Op::Ne {
          10.0
        }
        // Range could also be quite selective
        else {
          50.0
        }
      }

      // 2. edge connectivity
      let in_degree = self.pattern_graph.get_in_degree(vid);
      let out_degree = self.pattern_graph.get_out_degree(vid);
      // the less the degree is, the faster the query could be
      score -= (in_degree + out_degree) as f64;

      // 3. vertex connectivity
      let num_of_neighbors = self.pattern_graph.get_adj_vids(vid).len();
      // the less the number of neighbors is, the faster the query could be
      //
      // and this one could be more affective than the `edge connectivity`
      score -= num_of_neighbors as f64 * 5.0;

      let score = OrderedFloat::from(score);

      v_scores.insert(vid, score);
    }

    // DESC of `score`
    self.order.sort_unstable_by_key(|vid| -v_scores[vid]);
  }

  /// Core logic: Heuristic-based cost estimation & adjustment
  ///
  /// To keep `worst-case optimal`, we assume that each step is in the worst case.
  fn cost_based_adjustment(&mut self) {
    let vs = self.pattern_graph.view_v_entities();

    vs.into_par_iter()
      .map(|v| {
        // Get current vertex's cost (=> v_label_cnt)
        let mut v_cost = self
          .statistics
          .v_label_cnt
          .get(&v.label)
          .copied()
          .unwrap_or(0);

        if let Some(&cost) = self.override_v_cost.get(&v.vid) {
          v_cost = cost;
        }

        let grouped_adj_eids = self.pattern_graph.view_adj_es_grouped_by_target_vid(&v.vid);
        let mut grouped_e_costs = Vec::with_capacity(grouped_adj_eids.len());

        // In the worst case, each vertex could match.
        // Then v_cost should be multiplied by `group count` of `grouped_adj_es`.
        //
        // However, we still the original v_cost (before cloned) for further estimation.
        let original_v_cost = v_cost;
        v_cost *= grouped_adj_eids.len();

        for adj_eids in grouped_adj_eids.into_values() {
          // We assume that in the original data graph,
          // the patterns of edges between two vertices will never overlap.
          let curr_group_adj_es_costs: usize = adj_eids
            .into_iter()
            .map(|eid| {
              // est <=> estimation
              let e_est = self
                .statistics
                .e_label_cnt
                .get(&eid.label)
                .copied()
                .unwrap_or(0);
              // notice that, e_cost should be the minimum value of `e_est` and `original_v_cost`
              e_est.min(original_v_cost)
            })
            .sum();

          grouped_e_costs.push(curr_group_adj_es_costs);
        }

        // Now we have the cost of `v`
        let cost = v_cost + grouped_e_costs.into_iter().sum::<usize>();

        (v.vid.clone(), cost)
      })
      .collect_vec_list()
      .into_iter()
      .flatten()
      .for_each(|(vid, cost)| {
        // Update
        self.cost_2_vids.entry(cost).or_default().push(vid);
      });

    let mut v_costs = HashMap::with_capacity(self.cost_2_vids.len());
    while let Some((cost, vids)) = self.cost_2_vids.pop_first() {
      for vid in vids {
        v_costs.insert(vid, cost);
      }
    }

    // use `stable_sort` to keep the order of `vids` with the same cost
    self.order.sort_by_key(|vid| v_costs[vid]);
  }

  pub fn compute_optimal_order(mut self) -> PlanGenInput {
    self.rule_based_estimation();
    self.cost_based_adjustment();

    PlanGenInput {
      pattern_graph: self.pattern_graph,
      optimal_order: self.order,
    }
  }
}
