use crate::schemas::{DataEdge, DataVertex, LabelRef, PatternAttr, VidRef};

pub trait StorageAdapter {
  fn get_v(&self, vid: VidRef) -> impl Future<Output = Option<DataVertex>> + Send;

  fn load_v(&self, v_label: LabelRef) -> impl Future<Output = Vec<DataVertex>> + Send;

  fn load_v_with_attr(
    &self,
    v_label: LabelRef,
    v_attr: PatternAttr,
  ) -> impl Future<Output = Vec<DataVertex>> + Send;

  fn load_e(&self, e_label: LabelRef) -> impl Future<Output = Vec<DataEdge>> + Send;

  fn load_e_with_attr(
    &self,
    e_label: LabelRef,
    e_attr: PatternAttr,
  ) -> impl Future<Output = Vec<DataEdge>> + Send;
}
