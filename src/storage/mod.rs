use crate::schemas::*;

pub mod neo4j;
pub mod sqlite;

pub use neo4j::*;
// pub use sqlite::*;

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
