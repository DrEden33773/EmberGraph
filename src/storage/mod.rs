use crate::schemas::{DataEdge, DataVertex, LabelRef, PatternAttr, VidRef};

pub(crate) trait StorageAdapter {
  async fn get_v(&self, vid: VidRef) -> Option<DataVertex>;
  async fn load_v(&self, v_label: LabelRef) -> Vec<DataVertex>;
  async fn load_v_with_attr(&self, v_label: LabelRef, v_attr: PatternAttr) -> Vec<DataVertex>;
  async fn load_e(&self, e_label: LabelRef) -> Vec<DataEdge>;
  async fn load_e_with_attr(&self, e_label: LabelRef, e_attr: PatternAttr) -> Vec<DataEdge>;
}
