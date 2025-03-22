use crate::{
  schemas::{DataEdge, PatternVertex, Vid, VidRef},
  storage::StorageAdapter,
  utils::{dyn_graph::DynGraph, expand_graph::ExpandGraph},
};
use ahash::{AHashMap, AHashSet};

async fn does_data_v_satisfy_pattern(
  dg_vid: VidRef<'_>,
  pat_vid: VidRef<'_>,
  pat_v_entities: &AHashMap<Vid, PatternVertex>,
  storage_adapter: &impl StorageAdapter,
) -> bool {
  let pat_v = pat_v_entities.get(pat_vid).unwrap();
  let data_v = storage_adapter.get_v(dg_vid).await.unwrap();

  if pat_v.label != data_v.label {
    return false;
  }

  if pat_v.attr.is_none() {
    true
  } else if data_v.attrs.is_empty() {
    false
  } else {
    let pat_attr = pat_v.attr.as_ref().unwrap();
    if !data_v.attrs.contains_key(pat_attr.key.as_str()) {
      return false;
    }
    let data_value = data_v.attrs.get(pat_attr.key.as_str()).unwrap();
    pat_attr.op.operate_on(data_value, &pat_attr.value)
  }
}

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
