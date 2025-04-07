use crate::schemas::*;

pub mod cached;
pub mod neo4j;
pub mod sqlite;

pub use cached::*;
pub use neo4j::*;
pub use sqlite::*;

pub trait AsyncDefault: Send + Sync {
  fn async_default() -> impl Future<Output = Self> + Send;
}

pub trait StorageAdapter: Clone + AsyncDefault {
  fn get_v(&self, vid: VidRef<'_>) -> impl Future<Output = Option<DataVertex>> + Send;

  fn load_v(
    &self,
    v_label: LabelRef<'_>,
    v_attr: Option<&PatternAttr>,
  ) -> impl Future<Output = Vec<DataVertex>> + Send;

  fn load_e(
    &self,
    e_label: LabelRef<'_>,
    e_attr: Option<&PatternAttr>,
  ) -> impl Future<Output = Vec<DataEdge>> + Send;

  fn load_e_with_src(
    &self,
    src_vid: VidRef<'_>,
    e_label: LabelRef<'_>,
    e_attr: Option<&PatternAttr>,
  ) -> impl Future<Output = Vec<DataEdge>> + Send;

  fn load_e_with_dst(
    &self,
    dst_vid: VidRef<'_>,
    e_label: LabelRef<'_>,
    e_attr: Option<&PatternAttr>,
  ) -> impl Future<Output = Vec<DataEdge>> + Send;
}

pub trait AdvancedStorageAdapter: StorageAdapter {
  fn load_e_with_src_and_dst_filter(
    &self,
    src_vid: VidRef<'_>,
    e_label: LabelRef<'_>,
    e_attr: Option<&PatternAttr>,
    dst_v_label: LabelRef<'_>,
    dst_v_attr: Option<&PatternAttr>,
  ) -> impl Future<Output = Vec<DataEdge>> + Send;

  fn load_e_with_dst_and_src_filter(
    &self,
    dst_vid: VidRef<'_>,
    e_label: LabelRef<'_>,
    e_attr: Option<&PatternAttr>,
    src_v_label: LabelRef<'_>,
    src_v_attr: Option<&PatternAttr>,
  ) -> impl Future<Output = Vec<DataEdge>> + Send;
}

pub trait WritableStorageAdapter: AdvancedStorageAdapter {
  fn add_v(
    &self,
    v: DataVertex,
  ) -> impl Future<Output = Result<(), Box<dyn std::error::Error>>> + Send;

  fn add_e(
    &self,
    e: DataEdge,
  ) -> impl Future<Output = Result<(), Box<dyn std::error::Error>>> + Send;
}

pub trait TestOnlyStorageAdapter: WritableStorageAdapter {
  fn async_init_test_only() -> impl Future<Output = Self> + Send;
}
