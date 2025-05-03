use crate::{
  schemas::{AttrValue, Label, Op, PatternEdge, PatternVertex, Vid},
  utils::dyn_graph::DynGraph,
};
use hashbrown::HashMap;
use ordered_float::OrderedFloat;
use project_root::get_project_root;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fs::File, path::PathBuf, sync::LazyLock};

use super::order_calc::PlanGenInput;

static STAT_FILEPATH: LazyLock<PathBuf> = LazyLock::new(|| {
  get_project_root()
    .unwrap()
    .join("resources")
    .join("statistics")
    .join("advanced_statistics.json")
});

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OperatorSelectivity {
  eq: f64,
  ne: f64,
  gt: HashMap<String, f64>,
  ge: HashMap<String, f64>,
  lt: HashMap<String, f64>,
  le: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AttributeHistogram {
  bins: Vec<f64>,
  counts: Vec<usize>,
  value_counts: HashMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AttributeStats {
  count: usize,
  null_count: usize,
  distinct_count: usize,
  // Use serde_json::Value for flexibility in parsing min/max
  min_value: Option<Value>,
  max_value: Option<Value>,
  histogram: AttributeHistogram,
  selectivity: OperatorSelectivity,
  #[serde(rename = "type")]
  type_: String, // "Int", "Float", "String"
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Statistics {
  v_cnt: usize,
  e_cnt: usize,
  v_label_cnt: HashMap<Label, usize>,
  e_label_cnt: HashMap<Label, usize>,
  /// {v_label: {v_attr_name: stats}}
  v_attr_stats: HashMap<String, HashMap<String, AttributeStats>>,
  /// {e_label: {e_attr_name: stats}}
  e_attr_stats: HashMap<String, HashMap<String, AttributeStats>>,
}

#[derive(Debug, Clone)]
pub struct AdvancedOrderCalculator {
  statistics: Statistics,
  pattern_graph: DynGraph<PatternVertex, PatternEdge>,
  // Store calculated costs for sorting
  vertex_costs: HashMap<Vid, f64>,

  order: Vec<Vid>,

  eq_vids: Vec<Vid>,
  range_vids: Vec<Vid>,
  ne_vids: Vec<Vid>,
  plain_vids: Vec<Vid>,
}

impl AdvancedOrderCalculator {
  pub fn new(pattern_graph: DynGraph<PatternVertex, PatternEdge>) -> Self {
    println!(
      "📖  Loading advanced statistics from {:?}...",
      STAT_FILEPATH.as_path()
    );
    let statistics: Statistics = serde_json::from_reader(
      File::open(STAT_FILEPATH.as_path()).expect("❌  Failed to open advanced statistics file"),
    )
    .expect("❌  Failed to parse advanced statistics file");
    println!("✅  Advanced statistics loaded successfully.");

    let max_cap = pattern_graph.view_vids().len();

    Self {
      statistics,
      pattern_graph,
      vertex_costs: HashMap::with_capacity(max_cap),
      order: Vec::with_capacity(max_cap),
      eq_vids: Vec::with_capacity(max_cap),
      range_vids: Vec::with_capacity(max_cap),
      ne_vids: Vec::with_capacity(max_cap),
      plain_vids: Vec::with_capacity(max_cap),
    }
  }

  // --- Helper function to get attribute stats ---
  fn get_v_attr_stats(&self, v_label: &Label, attr_key: &str) -> Option<&AttributeStats> {
    self
      .statistics
      .v_attr_stats
      .get(v_label)
      .and_then(|label_stats| label_stats.get(attr_key))
  }

  // --- Helper function to estimate selectivity from histogram for range queries ---
  // Note: This is a simplified estimation. More sophisticated methods exist.
  fn estimate_range_selectivity(
    &self,
    op: &Op,
    value: &AttrValue,
    stats: &AttributeStats,
  ) -> Option<f64> {
    if stats.count == 0 {
      return Some(0.0); // No data points
    }
    if stats.type_ == "String" {
      // Range queries on strings are not directly supported by this histogram model
      return None; // Fallback or handle based on specific string semantics if needed
    }

    // Try parsing the comparison value to f64
    let Some(cmp_val) = (match value {
      AttrValue::Int(n) => Some(*n as f64),
      AttrValue::Float(n) => Some(*n),
      _ => None,
    }) else {
      eprintln!(
        "⚠️  Could not parse comparison value {:?} to f64 for range selectivity.",
        value
      );
      return None; // Cannot compare
    };

    let histogram = &stats.histogram;
    let bins = &histogram.bins;
    let counts = &histogram.counts;
    let total_count = stats.count as f64; // Use total count including nulls? Or non-null count? Let's use total count for now.

    if bins.is_empty() || counts.is_empty() || bins.len() != counts.len() + 1 {
      eprintln!(
        "⚠️  Invalid histogram for attribute (label: ?, key: ?): bins {:?}, counts {:?}",
        bins, counts
      ); // Need label/key context if possible
      return None; // Invalid histogram data
    }

    let mut satisfying_count = 0.0;

    for i in 0..counts.len() {
      let bin_start = bins[i];
      let bin_end = bins[i + 1];
      let bin_count = counts[i] as f64;

      // Estimate the fraction of the bin that satisfies the condition
      // Assume uniform distribution within the bin for simplicity
      let fraction = match op {
        Op::Lt => {
          if cmp_val <= bin_start {
            0.0
          } else if cmp_val >= bin_end {
            1.0
          } else {
            (cmp_val - bin_start) / (bin_end - bin_start)
          }
        }
        Op::Le => {
          if cmp_val < bin_start {
            0.0
          } else if cmp_val >= bin_end {
            1.0
          } else {
            (cmp_val - bin_start) / (bin_end - bin_start)
          }
        }
        Op::Gt => {
          if cmp_val >= bin_end {
            0.0
          } else if cmp_val <= bin_start {
            1.0
          } else {
            (bin_end - cmp_val) / (bin_end - bin_start)
          }
        }
        Op::Ge => {
          if cmp_val > bin_end {
            0.0
          } else if cmp_val <= bin_start {
            1.0
          } else {
            (bin_end - cmp_val) / (bin_end - bin_start)
          }
        }
        _ => 0.0, // Should not happen for range ops
      };

      satisfying_count += bin_count * fraction.clamp(0.0, 1.0); // Clamp fraction [0, 1]
    }

    Some(satisfying_count / total_count)
  }

  /// Placeholder for helper to estimate selectivity for Eq/Ne on strings
  fn estimate_string_eq_selectivity(&self, value_str: &str, stats: &AttributeStats) -> Option<f64> {
    if stats.count == 0 {
      return Some(0.0);
    }
    stats
      .histogram
      .value_counts
      .get(value_str)
      .map(|&count| count as f64 / stats.count as f64)
      .or(Some(0.0)) // If value not found, assume 0 selectivity for Eq
  }

  /// Group vertices by attribute operation
  fn group_vids_by_attr_op(&mut self) {
    for (vid, v) in self.pattern_graph.v_entities.iter() {
      if let Some(ref attr) = v.attr {
        match attr.op {
          Op::Eq => self.eq_vids.push(vid.clone()),
          Op::Ne => self.ne_vids.push(vid.clone()),
          _ => self.range_vids.push(vid.clone()), // Gt, Ge, Lt, Le
        }
      } else {
        self.plain_vids.push(vid.clone());
      }
    }
    println!("🔹  Vertices grouped by attribute op:");
    println!("   - Eq: {}", self.eq_vids.len());
    println!("   - Range: {}", self.range_vids.len());
    println!("   - Ne: {}", self.ne_vids.len());
    println!("   - Plain: {}", self.plain_vids.len());
  }

  /// Rule-Based Optimization (Connectivity)
  fn rule_based_optimization(&mut self) {
    let mut v_prices = HashMap::new();

    for vid in self.pattern_graph.v_entities.keys() {
      let mut price = 0.0;
      let in_degree = self.pattern_graph.get_in_degree(vid);
      let out_degree = self.pattern_graph.get_out_degree(vid);
      price += (in_degree + out_degree) as f64; // Edge connectivity

      let num_of_neighbors = self.pattern_graph.get_adj_vids(vid).len();
      price += num_of_neighbors as f64 * 5.0; // Vertex connectivity (weighted higher)

      v_prices.insert(vid.clone(), OrderedFloat::from(price));
    }

    // Sort each bucket by connectivity price (ASC - cheaper first)
    println!("🔹 Applying rule-based optimization (connectivity) ...");
    for bucket in [
      &mut self.eq_vids,
      &mut self.range_vids,
      &mut self.ne_vids,
      &mut self.plain_vids,
    ] {
      bucket.sort_unstable_by_key(|vid| v_prices[vid]);
    }
    println!("✅  Rule-based optimization complete.");
  }

  /// Cost-Based Optimization (Selectivity & Cardinality)
  fn cost_based_optimization(&mut self) {
    println!("🔹  Applying cost-based optimization (selectivity & cardinality) ...");
    let calculated_costs: Vec<(Vid, f64)> = self
      .pattern_graph
      .view_v_entities()
      .into_par_iter() // Parallel calculation
      .map(|v| {
        let vid = &v.vid;
        let v_label = &v.label;

        // 1. Initial cost based on label cardinality
        let initial_cardinality = self
          .statistics
          .v_label_cnt
          .get(v_label)
          .copied()
          .unwrap_or(1) // Avoid 0 cost, assume at least 1 match if label unknown
          as f64;

        // 2. Factor in attribute filter selectivity
        let mut selectivity = 1.0;
        if let Some(ref attr) = v.attr {
          if let Some(stats) = self.get_v_attr_stats(v_label, &attr.key) {
            let estimated_selectivity = match attr.op {
              Op::Eq => {
                if stats.type_ == "String" {
                  if let AttrValue::String(val_str) = &attr.value {
                    self.estimate_string_eq_selectivity(val_str, stats)
                  } else {
                    None // Type mismatch
                  }
                } else {
                  // For numeric Eq, use pre-calculated selectivity
                  Some(stats.selectivity.eq)
                }
              }
              Op::Ne => Some(stats.selectivity.ne),
              Op::Gt | Op::Ge | Op::Lt | Op::Le => {
                self.estimate_range_selectivity(&attr.op, &attr.value, stats)
              }
            };

            // Use estimated selectivity if available, otherwise keep 1.0
            if let Some(est) = estimated_selectivity {
              // Clamp selectivity to avoid negative or overly large values
              selectivity = est.clamp(0.0, 1.0);
              // Handle edge case: if selectivity is near zero, use a small floor
              // to prevent costs from becoming exactly zero, which might disrupt sorting.
              if selectivity < 1e-9 {
                selectivity = 1e-9;
              }
            } else {
              eprintln!(
                "⚠️  Could not estimate selectivity for {:?}({}), label: {}, key: {}, op: {:?}, value: {:?}. Using default 1.0",
                vid, v_label, v_label, attr.key, attr.op, attr.value
              );
              selectivity = 1.0; // Fallback if estimation fails
            }
          } else {
            // No stats for this attribute, cannot estimate selectivity accurately
            // Could default to a higher cost or keep selectivity = 1.0
            eprintln!(
              "⚠️  No statistics found for attribute {:?}({}), label: {}, key: {}. Using default selectivity 1.0",
              vid, v_label, v_label, attr.key
            );
            selectivity = 1.0;
          }
        }

        let vertex_estimated_cardinality = initial_cardinality * selectivity;

        // 3. Factor in edge costs (Similar to original OrderCalculator)
        // Consider outgoing edges grouped by target vertex
        let dst_grouped_adj_eids = self
          .pattern_graph
          .view_adj_es_grouped_by_target_vid(vid);
        let mut total_edge_cost_contribution = 0.0;

        for adj_eids in dst_grouped_adj_eids.into_values() {
          // Cost for edges connecting to *one* specific target pattern vertex
          let curr_group_edge_cost: f64 = adj_eids
            .into_iter()
            .map(|eid| {
              let e_label = &eid.label;
              let e_cardinality_estimation = self
                .statistics
                .e_label_cnt
                .get(e_label)
                .copied()
                .unwrap_or(1) // Avoid 0 cost
                as f64;

              // Cost contribution of this edge type is limited by the cardinality
              // of the source vertex (after filtering) and the edge cardinality itself.
              e_cardinality_estimation.min(vertex_estimated_cardinality)
            })
            .sum();
          total_edge_cost_contribution += curr_group_edge_cost;
        }

        // Final cost: Estimated vertex cardinality + edge contributions
        // We multiply vertex cardinality by the number of distinct *pattern* neighbors
        // as a heuristic similar to the original paper (worst-case joins).
        let num_distinct_pattern_neighbors = self.pattern_graph.view_adj_es_grouped_by_target_vid(vid).len();
        let final_cost = vertex_estimated_cardinality * (num_distinct_pattern_neighbors.max(1) as f64) + total_edge_cost_contribution;

        (vid.clone(), final_cost)
      })
      .collect::<Vec<(Vid, f64)>>();

    // Store calculated costs
    self.vertex_costs = calculated_costs.into_iter().collect();

    // Sort each bucket by the calculated cost (ASC - cheaper first)
    for bucket in [
      &mut self.eq_vids,
      &mut self.range_vids,
      &mut self.ne_vids,
      &mut self.plain_vids,
    ] {
      // Use f64::total_cmp for robust comparison, handle potential NaN/Infinities
      // Default to a very high cost if a vertex's cost wasn't calculated (shouldn't happen)
      bucket.sort_unstable_by(|a, b| {
        let cost_a = self.vertex_costs.get(a).copied().unwrap_or(f64::INFINITY);
        let cost_b = self.vertex_costs.get(b).copied().unwrap_or(f64::INFINITY);
        cost_a.total_cmp(&cost_b)
      });
    }
    println!("✅  Cost-based optimization complete.");
    // Optional: Print top N cheapest vertices per category for debugging
    // println!("   - Cheapest Eq: {:?}", self.eq_vids.iter().take(3).map(|vid| (vid, self.vertex_costs.get(vid))).collect_vec());
    // println!("   - Cheapest Range: {:?}", self.range_vids.iter().take(3).map(|vid| (vid, self.vertex_costs.get(vid))).collect_vec());
    // println!("   - Cheapest Ne: {:?}", self.ne_vids.iter().take(3).map(|vid| (vid, self.vertex_costs.get(vid))).collect_vec());
    // println!("   - Cheapest Plain: {:?}", self.plain_vids.iter().take(3).map(|vid| (vid, self.vertex_costs.get(vid))).collect_vec());
  }

  /// Concatenate the final optimal order
  fn concat_final_optimal_order(&mut self) {
    println!("🔹  Concatenating final order...");
    // Order: Eq -> Range -> Ne -> Plain
    self.order.append(&mut self.eq_vids);
    self.order.append(&mut self.range_vids);
    self.order.append(&mut self.ne_vids);
    self.order.append(&mut self.plain_vids);
    println!(
      "✅  Final order concatenated. Total vertices: {}",
      self.order.len()
    );
  }

  /// Main computation function
  pub fn compute_optimal_order(mut self) -> PlanGenInput {
    println!("🚀  Starting advanced optimal order computation...");
    self.group_vids_by_attr_op();
    self.rule_based_optimization(); // Initial sort by connectivity
    self.cost_based_optimization(); // Refined sort by estimated cost/cardinality
    self.concat_final_optimal_order();

    println!("✅  Advanced optimal order computation finished.");
    PlanGenInput {
      pattern_graph: self.pattern_graph, // Return the original graph
      optimal_order: self.order,
    }
  }
}
