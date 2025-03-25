use crate::{
  schemas::{PatternVertex, Vid, VidRef},
  storage::StorageAdapter,
};
use ahash::AHashMap;

use super::buckets::FBucket;

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

impl FBucket {}
