use crate::{
  schemas::{PatternEdge, PatternVertex, PlanData, STR_TUPLE_SPLITTER, Vid, VidRef},
  utils::{dyn_graph::DynGraph, expand_graph::ExpandGraph},
};
use ahash::{AHashMap, AHashSet};
use buckets::{ABucket, CBucket, FBucket, TBucket};

pub mod buckets;
pub mod buckets_impl;

#[inline]
fn resolve_var_name(target_var: &str) -> &str {
  target_var.split(STR_TUPLE_SPLITTER).nth(1).unwrap()
}

#[derive(Debug, Clone, Default)]
pub struct MatchingCtx {
  pub(crate) plan_data: PlanData,
  pub(crate) empty_matched_set_appeared: bool,
  pub(crate) expanded_data_vids: AHashSet<Vid>,

  pub(crate) f_block: AHashMap<Vid, FBucket>,
  pub(crate) a_block: AHashMap<Vid, ABucket>,
  pub(crate) c_block: AHashMap<Vid, CBucket>,
  pub(crate) t_block: AHashMap<Vid, TBucket>,

  pub(crate) grouped_partial_matches: Vec<Vec<DynGraph>>,
}

impl MatchingCtx {
  #[inline]
  pub fn update_extended_data_vids(&mut self, vid: AHashSet<Vid>) {
    self.expanded_data_vids.extend(vid);
  }

  pub fn pattern_vs(&self) -> &AHashMap<Vid, PatternVertex> {
    &self.plan_data.pattern_vs
  }

  pub fn pattern_es(&self) -> &AHashMap<Vid, PatternEdge> {
    &self.plan_data.pattern_es
  }
}

impl MatchingCtx {
  #[inline]
  pub fn get_pattern_v(&self, vid: VidRef) -> Option<&PatternVertex> {
    self.plan_data.pattern_vs.get(vid)
  }

  #[inline]
  pub fn fetch_pattern_v_batch<'a>(
    &'a self,
    vid: impl IntoIterator<Item = VidRef<'a>>,
  ) -> Vec<PatternVertex> {
    vid
      .into_iter()
      .filter_map(|v| self.plan_data.pattern_vs.get(v))
      .cloned()
      .collect()
  }

  #[inline]
  pub fn get_pattern_e(&self, eid: VidRef) -> Option<&PatternEdge> {
    self.plan_data.pattern_es.get(eid)
  }

  #[inline]
  pub fn fetch_pattern_e_batch<'a>(
    &'a self,
    eid: impl IntoIterator<Item = VidRef<'a>>,
  ) -> Vec<PatternEdge> {
    eid
      .into_iter()
      .filter_map(|e| self.plan_data.pattern_es.get(e))
      .cloned()
      .collect()
  }
}

impl MatchingCtx {
  /// `Foreach`: Initialize `f_block`
  pub fn init_f_block(&mut self, target_var: &str) {
    let key = resolve_var_name(target_var);
    self.f_block.insert(key.to_string(), FBucket::default());
  }

  /// `Init`: Update `f_block` with matched_graph and frontier_vid
  pub fn append_to_f_block(
    &mut self,
    target_var: &str,
    matched_graph: DynGraph,
    frontier_vid: VidRef,
  ) {
    let key = resolve_var_name(target_var);
    let next_idx = self.f_block.len();
    let f_bucket = self.f_block.entry(key.to_string()).or_default();
    f_bucket.all_matched.push(matched_graph);
    f_bucket
      .matched_with_frontiers
      .entry(next_idx)
      .or_default()
      .push(frontier_vid.to_string());
  }

  /// `Foreach`: Update `f_block` with `f_bucket`
  pub fn update_f_block(&mut self, target_var: &str, f_bucket: FBucket) {
    let key = resolve_var_name(target_var);
    self.f_block.insert(key.to_string(), f_bucket);
  }

  /// `GetAdj`: Get `f_block` with `target_var` (pop it out)
  pub fn pop_from_f_block(&mut self, single_op: &str) -> Option<FBucket> {
    let key = resolve_var_name(single_op);
    self.f_block.remove(key)
  }
}

impl MatchingCtx {
  /// `GetAdj`: Initialize `a_block` with `target_var`
  pub fn init_a_block(&mut self, target_var: &str) {
    let key = resolve_var_name(target_var);
    self
      .a_block
      .insert(key.to_string(), ABucket::new(key.to_string()));
  }

  /// `GetAdj`: Update `a_block` with `a_bucket`
  pub fn update_a_block(&mut self, target_var: &str, a_bucket: ABucket) {
    let key = resolve_var_name(target_var);
    self.a_block.insert(key.to_string(), a_bucket);
  }

  /// `Intersect(Ai)`: Get the `group` from `single_op` represented `a_bucket` by `pattern_str`
  ///
  /// (pop it out)
  pub fn pop_group_by_pat_from_a_block(
    &mut self,
    single_op: &str,
    curr_pat_str: &str,
  ) -> Option<Vec<ExpandGraph>> {
    let key = resolve_var_name(single_op);
    self
      .a_block
      .get_mut(key)?
      .next_pat_grouped_expanding
      .remove(curr_pat_str)
  }
}

impl MatchingCtx {
  /// `Intersect`: Initialize `c_block` with `target_var`
  pub fn init_c_block(&mut self, target_var: &str) {
    let key = resolve_var_name(target_var);
    self.c_block.insert(key.to_string(), CBucket::default());
  }

  /// `Intersect`: Update `c_block` with `c_bucket`
  pub fn update_c_block(&mut self, target_var: &str, c_bucket: CBucket) {
    let key = resolve_var_name(target_var);
    self.c_block.insert(key.to_string(), c_bucket);
  }

  /// `Foreach`: Get `c_block` with `single_op` (pop it out)
  pub fn pop_from_c_block(&mut self, single_op: &str) -> Option<CBucket> {
    let key = resolve_var_name(single_op);
    self.c_block.remove(key)
  }
}

impl MatchingCtx {
  /// `Intersect`: Initialize `t_block` with `target_var`
  pub fn init_t_block(&mut self, target_var: &str) {
    let key = resolve_var_name(target_var);
    self
      .t_block
      .insert(key.to_string(), TBucket::new(key.to_string()));
  }

  /// `Intersect`: Update `t_block` with `t_bucket`
  pub fn update_t_block(&mut self, target_var: &str, t_bucket: TBucket) {
    let key = resolve_var_name(target_var);
    self.t_block.insert(key.to_string(), t_bucket);
  }

  /// `Intersect(Tx)`: Get `t_block` with `single_op` (pop it out)
  pub fn pop_from_t_block(&mut self, single_op: &str) -> Option<TBucket> {
    let key = resolve_var_name(single_op);
    self.t_block.remove(key)
  }
}
