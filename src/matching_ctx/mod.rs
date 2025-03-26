use ahash::{AHashMap, AHashSet};
use buckets::{ABucket, CBucket, FBucket, TBucket};

use crate::schemas::{PlanData, STR_TUPLE_SPLITTER, Vid};

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
}

impl MatchingCtx {
  pub fn update_extended_data_vids(&mut self, vid: AHashSet<Vid>) {
    self.expanded_data_vids.extend(vid);
  }
}
