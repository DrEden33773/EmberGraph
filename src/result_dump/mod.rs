use crate::{schemas::PlanData, utils::dyn_graph::DynGraph};
use itertools::Itertools;
use polars::prelude::*;
use std::collections::BTreeMap;

pub struct EmptyResultDumper<'plan> {
  plan_data: &'plan PlanData,
}

impl<'plan> EmptyResultDumper<'plan> {
  pub fn new(plan_data: &'plan PlanData) -> Self {
    Self { plan_data }
  }

  pub fn dump_patterns_to_polars_df(self) -> DataFrame {
    let patterns = self
      .plan_data
      .pattern_vs
      .keys()
      .chain(self.plan_data.pattern_es.keys());

    let columns = patterns
      .map(|pattern| Column::new(pattern.as_str().into(), Vec::<String>::new()))
      .collect_vec();

    DataFrame::new(columns).unwrap()
  }
}

pub struct ResultDumper {
  results: Vec<DynGraph>,
}

impl ResultDumper {
  pub fn new(results: Vec<DynGraph>) -> Self {
    Self { results }
  }

  pub fn dump_to_polars_df(self) -> Option<DataFrame> {
    if self.results.is_empty() {
      return None;
    }

    unsafe { std::env::set_var("POLARS_FMT_STR_LEN", "10000") };

    let all_pre_dumped = self.results.into_iter().map(|g| g.pre_dump()).collect_vec();
    let mut ordered_columns = BTreeMap::new();

    for pre_dump in all_pre_dumped {
      for (pattern, repr) in pre_dump {
        ordered_columns
          .entry(pattern)
          .or_insert_with(Vec::new)
          .extend(repr);
      }
    }

    let columns = ordered_columns
      .into_iter()
      .map(|(pattern, repr)| Column::new(pattern.as_str().into(), repr))
      .collect_vec();

    let df = DataFrame::new(columns).unwrap();

    Some(df)
  }
}
