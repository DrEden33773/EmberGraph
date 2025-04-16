use crate::{parser::PatternParser, schemas::PlanData};
use itertools::Itertools;
use order_calc::{OrderCalculator, PlanGenInput};
use plan_dump::PlanDumper;
use plan_gen::PlanGenerator;
use plan_opt::PlanOptimizer;
use std::{fs, path::Path};

pub mod order_calc;
pub mod plan_dump;
pub mod plan_gen;
pub mod plan_opt;

pub fn generate_optimal_plan(query_path: &Path) -> PlanData {
  let query_src = fs::read_to_string(query_path).expect("❌  Failed to read query file.");

  // Parse the query source
  let mut parser = PatternParser::new(query_src);
  parser.parse();
  let pattern_graph = parser.take_as_pattern_graph();

  // Compute the optimal matching order
  let order_calc = OrderCalculator::new(pattern_graph);
  let plan_gen_input = order_calc.compute_optimal_order();

  // Generate the raw plan
  let mut plan_gen = PlanGenerator::from(plan_gen_input);
  plan_gen.generate_raw_plan();

  // Optimize the plan
  let mut plan_optimizer = PlanOptimizer::from(plan_gen);
  plan_optimizer.apply_optimization();

  // Dump the plan
  let plan_dumper = PlanDumper::from(plan_optimizer);
  plan_dumper.to_plan_data()
}

pub fn generate_plan_with_given_order(query_path: &Path, given_order: &[&str]) -> PlanData {
  let query_src = fs::read_to_string(query_path).expect("❌  Failed to read query file.");

  // Parse the query source
  let mut parser = PatternParser::new(query_src);
  parser.parse();
  let pattern_graph = parser.take_as_pattern_graph();

  #[cfg(debug_assertions)]
  assert!(
    pattern_graph.has_all_vids(given_order) && pattern_graph.get_v_count() == given_order.len(),
    "❌  The `given order` != `pattern graph vertices`."
  );

  let given_order = given_order.iter().map(|s| s.to_string()).collect_vec();
  let plan_gen_input = PlanGenInput {
    pattern_graph,
    optimal_order: given_order,
  };

  // Generate the raw plan
  let mut plan_gen = PlanGenerator::from(plan_gen_input);
  plan_gen.generate_raw_plan();

  // Optimize the plan
  let mut plan_optimizer = PlanOptimizer::from(plan_gen);
  plan_optimizer.apply_optimization();

  // Dump the plan
  let plan_dumper = PlanDumper::from(plan_optimizer);
  plan_dumper.to_plan_data()
}
