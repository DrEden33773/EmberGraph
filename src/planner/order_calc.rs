use crate::{
  schemas::{Label, PatternEdge, PatternVertex, Vid},
  utils::dyn_graph::DynGraph,
};
use hashbrown::HashMap;
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
}

#[derive(Debug, Clone)]
pub struct PlanGenInput {
  pub(crate) pattern_graph: DynGraph<PatternVertex, PatternEdge>,
  pub(crate) optimal_order: Vec<(Vid, usize)>,
}

impl OrderCalculator {
  pub fn new(pattern_graph: DynGraph<PatternVertex, PatternEdge>) -> Self {
    let statistics: Statistics = serde_json::from_reader(
      File::open(STAT_FILEPATH.as_path()).expect("⚠️  Failed to open statistics file"),
    )
    .expect("⚠️  Failed to parse statistics file");

    Self {
      statistics,
      pattern_graph,
      cost_2_vids: BTreeMap::default(),
    }
  }

  /// Core logic: Heuristic-based cost estimation
  ///
  /// To keep `worst-case optimal`, we assume that each step is in the worst case.
  pub fn compute_optimal_order(mut self) -> PlanGenInput {
    let vs = self.pattern_graph.get_v_entities();

    vs.into_par_iter()
      .map(|v| {
        // Get current vertex's cost (=> v_label_cnt)
        let mut v_cost = self
          .statistics
          .v_label_cnt
          .get(&v.label)
          .copied()
          .unwrap_or(0);

        let grouped_adj_eids = self.pattern_graph.get_adj_es_grouped_by_target_vid(&v.vid);
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

        (v.vid, cost)
      })
      .collect_vec_list()
      .into_iter()
      .flatten()
      .for_each(|(vid, cost)| {
        // Update
        self.cost_2_vids.entry(cost).or_default().push(vid);
      });

    let mut optimal_order = Vec::with_capacity(self.cost_2_vids.len());
    for (cost, vids) in self.cost_2_vids {
      for vid in vids {
        optimal_order.push((vid, cost));
      }
    }

    PlanGenInput {
      pattern_graph: self.pattern_graph,
      optimal_order,
    }
  }
}
