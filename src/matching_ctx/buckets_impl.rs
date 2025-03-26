use super::buckets::{ABucket, CBucket, FBucket, TBucket};
use crate::{
  schemas::{DataVertex, PatternVertex, Vid, VidRef},
  storage::StorageAdapter,
};
use ahash::AHashMap;

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

impl FBucket {
  pub async fn from_c_bucket(c_bucket: &CBucket) -> Self {
    let all_matched = c_bucket
      .all_expanded
      .iter()
      .map(|g| g.clone().into())
      .collect();
    let matched_with_pivots = c_bucket
      .all_expanded
      .iter()
      .enumerate()
      .map(|(idx, _)| (idx, c_bucket.expanded_with_pivots[&idx].clone()))
      .collect();

    Self {
      all_matched,
      matched_with_pivots,
    }
  }
}

impl ABucket {
  pub async fn from_f_bucket(f_bucket: &FBucket, curr_pat_vid: VidRef<'_>) -> Self {
    Self {
      curr_pat_vid: curr_pat_vid.to_owned(),
      all_matched: f_bucket.all_matched.clone(),
      matched_with_pivots: f_bucket.matched_with_pivots.clone(),
      next_pat_grouped_expanding: AHashMap::new(),
    }
  }
}

impl CBucket {
  pub async fn build_from_a(
    a_bucket: &mut ABucket,
    curr_pat_vid: VidRef<'_>,
    loaded_v_pat_pairs: impl IntoIterator<Item = (DataVertex, String)>,
  ) -> Self {
    let loaded_v_pat_pairs = loaded_v_pat_pairs.into_iter().collect::<Vec<_>>();
    let mut all_expanded = vec![];
    let mut expanded_with_pivots = AHashMap::new();

    let curr_group = a_bucket
      .next_pat_grouped_expanding
      .remove(curr_pat_vid)
      .unwrap_or_default();

    for (idx, mut expanding) in curr_group.into_iter().enumerate() {
      let valid_targets = expanding.update_valid_target_vertices(loaded_v_pat_pairs.clone());
      all_expanded.push(expanding);
      expanded_with_pivots
        .entry(idx)
        .or_insert_with(Vec::new)
        .extend(valid_targets);
    }

    Self {
      all_expanded,
      expanded_with_pivots,
    }
  }

  pub async fn build_from_t(
    t_bucket: &mut TBucket,
    loaded_v_pat_pairs: impl IntoIterator<Item = (DataVertex, String)>,
  ) -> Self {
    let loaded_v_pat_pairs = loaded_v_pat_pairs.into_iter().collect::<Vec<_>>();
    let mut all_expanded = vec![];
    let mut expanded_with_pivots = AHashMap::new();

    for (idx, expanding) in t_bucket.expanding_graphs.iter_mut().enumerate() {
      let valid_targets = expanding.update_valid_target_vertices(loaded_v_pat_pairs.clone());
      all_expanded.push(expanding.clone());
      expanded_with_pivots
        .entry(idx)
        .or_insert_with(Vec::new)
        .extend(valid_targets);
    }

    Self {
      all_expanded,
      expanded_with_pivots,
    }
  }
}
