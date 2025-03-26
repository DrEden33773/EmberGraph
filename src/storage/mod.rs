use crate::schemas::{DataEdge, DataVertex, LabelRef, PatternAttr, VidRef};

pub trait StorageAdapter: Clone {
  fn get_v(&self, vid: VidRef) -> impl Future<Output = Option<DataVertex>> + Send;

  fn load_v(
    &self,
    v_label: LabelRef,
    v_attr: Option<&PatternAttr>,
  ) -> impl Future<Output = Vec<DataVertex>> + Send;

  fn load_e(
    &self,
    e_label: LabelRef,
    e_attr: Option<&PatternAttr>,
  ) -> impl Future<Output = Vec<DataEdge>> + Send;

  fn load_e_with_src(
    &self,
    src_vid: VidRef,
    e_label: LabelRef,
    e_attr: Option<&PatternAttr>,
  ) -> impl Future<Output = Vec<DataEdge>> + Send;

  fn load_e_with_dst(
    &self,
    dst_vid: VidRef,
    e_label: LabelRef,
    e_attr: Option<&PatternAttr>,
  ) -> impl Future<Output = Vec<DataEdge>> + Send;
}
