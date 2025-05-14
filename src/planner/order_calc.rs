use crate::{
  schemas::{Label, Op, PatternEdge, PatternVertex, Vid},
  utils::dyn_graph::DynGraph,
};
use hashbrown::HashMap;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use project_root::get_project_root;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use std::{fs::File, path::PathBuf, sync::LazyLock};

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
  cost_2_vids: HashMap<usize, Vec<Vid>>,

  order: Vec<Vid>,

  eq_vids: Vec<Vid>,
  range_vids: Vec<Vid>,
  ne_vids: Vec<Vid>,
  plain_vids: Vec<Vid>,
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
      .collect_vec();
    let max_cap = raw_order.len();

    Self {
      statistics,
      pattern_graph,
      cost_2_vids: HashMap::with_capacity(max_cap),
      order: Vec::with_capacity(max_cap),
      eq_vids: Vec::with_capacity(max_cap),
      range_vids: Vec::with_capacity(max_cap),
      ne_vids: Vec::with_capacity(max_cap),
      plain_vids: Vec::with_capacity(max_cap),
    }
  }

  fn group_vids_by_attr_op(&mut self) {
    for (vid, v) in self.pattern_graph.v_entities.iter() {
      if let Some(ref attr) = v.attr {
        match attr.op {
          Op::Eq => self.eq_vids.push(vid.clone()),
          Op::Ne => self.ne_vids.push(vid.clone()),
          _ => self.range_vids.push(vid.clone()),
        }
      } else {
        self.plain_vids.push(vid.clone());
      }
    }
  }

  /// Core logic: Rule-based optimization
  fn rule_based_optimization(&mut self) {
    let mut v_prices = HashMap::new();

    for vid in self.pattern_graph.v_entities.keys() {
      let mut price = 0.0;

      // 1. edge connectivity
      let in_degree = self.pattern_graph.get_in_degree(vid);
      let out_degree = self.pattern_graph.get_out_degree(vid);
      // vertex with more edges should be more expensive
      price += (in_degree + out_degree) as f64;

      // 2. vertex connectivity
      let num_of_neighbors = self.pattern_graph.get_adj_vids(vid).len();
      // vertex with more neighbors should be more expensive
      // (and should be much more expensive than edge connectivity)
      price += num_of_neighbors as f64 * 5.0;

      let price = OrderedFloat::from(price);

      v_prices.insert(vid, price);
    }

    // for each bucket
    for bucket in [
      &mut self.eq_vids,
      &mut self.range_vids,
      &mut self.ne_vids,
      &mut self.plain_vids,
    ] {
      // Sort by price (ASC)
      bucket.sort_unstable_by_key(|vid| v_prices[vid]);
    }
  }

  /// Core logic: Heuristic cost based optimization
  ///
  /// To keep `worst-case optimal`, we assume that each step is in the worst case.
  fn cost_based_optimization(&mut self) {
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

        let dst_grouped_adj_eids = self.pattern_graph.view_adj_es_grouped_by_target_vid(&v.vid);
        let mut dst_grouped_e_costs = Vec::with_capacity(dst_grouped_adj_eids.len());

        // In the worst case, each vertex could match.
        // Then v_cost should be multiplied by `group count` of `dst_grouped_adj_eids`.
        //
        // However, we still need the original v_cost (before cloned) for further estimation.
        let original_v_cost = v_cost;
        v_cost *= dst_grouped_adj_eids.len();

        for adj_eids in dst_grouped_adj_eids.into_values() {
          // We assume that in the original data graph,
          // the patterns of edges between two vertices will never overlap.
          let curr_group_adj_es_costs: usize = adj_eids
            .into_iter()
            .map(|eid| {
              let e_estimation = self
                .statistics
                .e_label_cnt
                .get(&eid.label)
                .copied()
                .unwrap_or(0);
              // notice that, e_cost should be the minimum value of `e_est` and `original_v_cost`
              e_estimation.min(original_v_cost)
            })
            .sum();

          dst_grouped_e_costs.push(curr_group_adj_es_costs);
        }

        // Now we have the cost of `v`
        let cost = v_cost + dst_grouped_e_costs.into_iter().sum::<usize>();

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
    for (cost, vids) in self.cost_2_vids.drain() {
      for vid in vids {
        v_costs.insert(vid, cost);
      }
    }

    // for each bucket
    for bucket in [
      &mut self.eq_vids,
      &mut self.range_vids,
      &mut self.ne_vids,
      &mut self.plain_vids,
    ] {
      // Sort by cost (ASC)
      bucket.sort_by_key(|vid| v_costs[vid]);
    }
  }

  fn concat_final_optimal_order(&mut self) {
    // 1. eq_vids (=)
    self.order.append(&mut self.eq_vids);

    // 2. range_vids (<, <=, >, >=)
    self.order.append(&mut self.range_vids);

    // 3. ne_vids (!=)
    self.order.append(&mut self.ne_vids);

    // 4. plain_vids (no attr)
    self.order.append(&mut self.plain_vids);
  }

  pub fn compute_optimal_order(mut self) -> PlanGenInput {
    self.group_vids_by_attr_op();
    self.rule_based_optimization();
    self.cost_based_optimization();
    self.concat_final_optimal_order();

    PlanGenInput {
      pattern_graph: self.pattern_graph,
      optimal_order: self.order,
    }
  }
}
