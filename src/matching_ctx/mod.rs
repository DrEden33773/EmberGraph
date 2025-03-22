use ahash::{AHashMap, AHashSet};
use buckets::{ABucket, CBucket, FBucket, TBucket};

use crate::schemas::{PlanData, STR_TUPLE_SPLITTER, Vid};

pub mod buckets;

#[inline]
fn resolve_var_name(target_var: &str) -> &str {
  target_var.split(STR_TUPLE_SPLITTER).nth(1).unwrap()
}

#[derive(Debug, Clone, Default)]
pub struct MatchingCtx {
  pub(crate) plan_data: PlanData,
  pub(crate) empty_matched_set_appeared: bool,
  pub(crate) expanded_data_vids: AHashSet<Vid>,

  pub(crate) f_pool: AHashMap<Vid, FBucket>,
  pub(crate) a_pool: AHashMap<Vid, ABucket>,
  pub(crate) c_pool: AHashMap<Vid, CBucket>,
  pub(crate) t_pool: AHashMap<Vid, TBucket>,
}
