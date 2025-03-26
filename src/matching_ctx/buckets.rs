use crate::{
  schemas::Vid,
  utils::{dyn_graph::DynGraph, expand_graph::ExpandGraph},
};
use ahash::AHashMap;

#[derive(Debug, Clone, Default)]
pub struct FBucket {
  pub(crate) all_matched: Vec<DynGraph>,
  pub(crate) matched_with_pivots: AHashMap<usize, Vec<Vid>>,
}
#[derive(Debug, Clone)]
pub struct ABucket {
  pub(crate) curr_pat_vid: Vid,
  pub(crate) all_matched: Vec<DynGraph>,
  pub(crate) matched_with_pivots: AHashMap<usize, Vec<Vid>>,
  pub(crate) next_pat_grouped_expanding: AHashMap<Vid, Vec<ExpandGraph>>,
}
#[derive(Debug, Clone, Default)]
pub struct CBucket {
  pub(crate) all_expanded: Vec<ExpandGraph>,
  pub(crate) expanded_with_pivots: AHashMap<usize, Vec<Vid>>,
}
#[derive(Debug, Clone)]
pub struct TBucket {
  pub(crate) target_pat_vid: Vid,
  pub(crate) expanding_graphs: Vec<ExpandGraph>,
}
