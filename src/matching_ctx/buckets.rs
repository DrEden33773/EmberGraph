use crate::{
  schemas::{DataEdge, Vid},
  utils::{dyn_graph::DynGraph, expand_graph::ExpandGraph},
};
use ahash::{AHashMap, AHashSet};

#[derive(Debug, Clone, Default)]
pub struct FBucket {
  pub(crate) all_matched: Vec<DynGraph>,
}
#[derive(Debug, Clone)]
pub struct ABucket {
  pub(crate) curr_pat_vid: Vid,
  pub(crate) all_matched: Vec<DynGraph>,
  pub(crate) next_pat_grouped_es: AHashMap<Vid, AHashSet<DataEdge>>,
  pub(crate) next_pat_grouped_expansions: AHashMap<Vid, AHashSet<DataEdge>>,
}
#[derive(Debug, Clone, Default)]
pub struct CBucket {
  pub(crate) all_expanded: Vec<ExpandGraph>,
}
#[derive(Debug, Clone)]
pub struct TBucket {
  pub(crate) target_pat_vid: Vid,
  pub(crate) expanding_graphs: Vec<ExpandGraph>,
}
