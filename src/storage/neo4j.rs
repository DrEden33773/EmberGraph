use super::{AdvancedStorageAdapter, AsyncDefault, StorageAdapter};
use crate::{schemas::*, utils::time_async_with_desc};
use neo4rs::*;
use std::env;

#[derive(Clone)]
pub struct Neo4jStorageAdapter {
  graph: Graph,
}

impl AsyncDefault for Neo4jStorageAdapter {
  async fn async_default() -> Self {
    let uri = env::var("NEO4J_URI").unwrap();
    let username = env::var("NEO4J_USERNAME").unwrap();
    let password = env::var("NEO4J_PASSWORD").unwrap();
    let db_name = env::var("NEO4J_DATABASE").unwrap();
    let config = ConfigBuilder::default()
      .uri(uri)
      .user(username)
      .password(password)
      .db(db_name)
      .fetch_size(10000)
      .max_connections(num_cpus::get())
      .build()
      .unwrap();

    let graph = time_async_with_desc(
      Graph::connect(config),
      "Connecting to Neo4j database ...".to_string(),
    )
    .await
    .expect("❌  Failed to connect to Neo4j database");

    Self { graph }
  }
}

impl From<(Row, LabelRef<'_>)> for DataEdge {
  fn from((row, e_label): (Row, LabelRef)) -> Self {
    let eid = row.get("eid").unwrap();
    let src_vid = row.get("src_vid").unwrap();
    let dst_vid = row.get("dst_vid").unwrap();
    let label = e_label.to_string();
    let attrs = row.get("props").unwrap();

    DataEdge {
      eid,
      src_vid,
      dst_vid,
      label,
      attrs,
    }
  }
}

impl StorageAdapter for Neo4jStorageAdapter {
  async fn get_v(&self, vid: VidRef<'_>) -> Option<DataVertex> {
    let mut query_str = "\n\t\tMATCH (v)\n".to_string();
    query_str += &format!("\t\tWHERE elementId(v) = '{vid}'\n");
    query_str += "
      RETURN
        properties(v) as props,
        labels(v) as v_label"
      .trim_start_matches('\n');

    let mut result = time_async_with_desc(self.graph.execute(query(&query_str)), query_str)
      .await
      .unwrap();

    let row = result.next().await.unwrap()?;

    let vid = vid.to_string();
    let labels: Vec<String> = row.get("v_label").unwrap();
    let label = labels[0].clone();
    let attrs = row.get("props").unwrap();

    Some(DataVertex { vid, label, attrs })
  }

  async fn load_v(&self, v_label: LabelRef<'_>, v_attr: Option<&PatternAttr>) -> Vec<DataVertex> {
    let mut query_str = format!("\n\t\tMATCH (v: {v_label})\n");
    if let Some(attr) = v_attr {
      let constraint = attr.to_neo4j_constraint("v");
      query_str += &format!("\t\tWHERE {constraint}\n");
    }
    query_str += "
      RETURN
        properties(v) as props,
        elementId(v) as vid"
      .trim_start_matches('\n');

    let mut result = time_async_with_desc(self.graph.execute(query(&query_str)), query_str)
      .await
      .unwrap();

    let mut ret = vec![];

    while let Some(row) = result.next().await.unwrap() {
      let vid = row.get("vid").unwrap();
      let label = v_label.to_string();
      let attrs = row.get("props").unwrap();

      ret.push(DataVertex { vid, label, attrs });
    }

    ret
  }

  async fn load_e(&self, e_label: LabelRef<'_>, e_attr: Option<&PatternAttr>) -> Vec<DataEdge> {
    let mut query_str = format!("\n\t\tMATCH (src)-[e: {e_label}]->(dst)\n");
    if let Some(attr) = e_attr {
      let constraint = attr.to_neo4j_constraint("e");
      query_str += &format!("\t\tWHERE {constraint}\n");
    }
    query_str += "
      RETURN
        elementId(e) AS eid,
        properties(e) AS props,
        elementId(src) AS src_vid,
        elementId(dst) AS dst_vid"
      .trim_start_matches('\n');

    let mut result = time_async_with_desc(self.graph.execute(query(&query_str)), query_str)
      .await
      .unwrap();

    let mut ret = vec![];

    while let Some(row) = result.next().await.unwrap() {
      ret.push(DataEdge::from((row, e_label)));
    }

    ret
  }

  async fn load_e_with_src(
    &self,
    src_vid: VidRef<'_>,
    e_label: LabelRef<'_>,
    e_attr: Option<&PatternAttr>,
  ) -> Vec<DataEdge> {
    let mut query_str = format!("\n\t\tMATCH (src)-[e: {e_label}]->(dst)\n");
    query_str += &format!("\t\tWHERE elementId(src) = '{src_vid}'\n");
    if let Some(attr) = e_attr {
      let constraint = attr.to_neo4j_constraint("e");
      query_str += &format!("\t\tAND {constraint}\n");
    }
    query_str += "
      RETURN
        elementId(e) AS eid,
        properties(e) AS props,
        elementId(src) AS src_vid,
        elementId(dst) AS dst_vid"
      .trim_start_matches('\n');

    let mut result = time_async_with_desc(self.graph.execute(query(&query_str)), query_str)
      .await
      .unwrap();

    let mut ret = vec![];

    while let Some(row) = result.next().await.unwrap() {
      ret.push(DataEdge::from((row, e_label)));
    }

    ret
  }

  async fn load_e_with_dst(
    &self,
    dst_vid: VidRef<'_>,
    e_label: LabelRef<'_>,
    e_attr: Option<&PatternAttr>,
  ) -> Vec<DataEdge> {
    let mut query_str = format!("\n\t\tMATCH (src)-[e: {e_label}]->(dst)\n");
    query_str += &format!("\t\tWHERE elementId(dst) = '{dst_vid}'\n");
    if let Some(attr) = e_attr {
      let constraint = attr.to_neo4j_constraint("e");
      query_str += &format!("\t\tAND {constraint}\n");
    }
    query_str += "
      RETURN
        elementId(e) AS eid,
        properties(e) AS props,
        elementId(src) AS src_vid,
        elementId(dst) AS dst_vid"
      .trim_start_matches('\n');

    let mut result = time_async_with_desc(self.graph.execute(query(&query_str)), query_str)
      .await
      .unwrap();

    let mut ret = vec![];

    while let Some(row) = result.next().await.unwrap() {
      ret.push(DataEdge::from((row, e_label)));
    }

    ret
  }
}

impl AdvancedStorageAdapter for Neo4jStorageAdapter {
  async fn load_e_with_src_and_dst_filter(
    &self,
    src_vid: VidRef<'_>,
    e_label: LabelRef<'_>,
    e_attr: Option<&PatternAttr>,
    dst_v_label: LabelRef<'_>,
    dst_v_attr: Option<&PatternAttr>,
  ) -> Vec<DataEdge> {
    let mut query_str = format!("\n\t\tMATCH (src)-[e: {e_label}]->(dst: {dst_v_label})\n");
    query_str += &format!("\t\tWHERE elementId(src) = '{src_vid}'\n");
    if let Some(attr) = e_attr {
      let constraint = attr.to_neo4j_constraint("e");
      query_str += &format!("\t\tAND {constraint}\n");
    }
    if let Some(attr) = dst_v_attr {
      let constraint = attr.to_neo4j_constraint("dst");
      query_str += &format!("\t\tAND {constraint}\n");
    }
    query_str += "
      RETURN
        elementId(e) AS eid,
        properties(e) AS props,
        elementId(src) AS src_vid,
        elementId(dst) AS dst_vid"
      .trim_start_matches('\n');

    let mut result = time_async_with_desc(self.graph.execute(query(&query_str)), query_str)
      .await
      .unwrap();

    let mut ret = vec![];
    while let Some(row) = result.next().await.unwrap() {
      ret.push(DataEdge::from((row, e_label)));
    }

    ret
  }

  async fn load_e_with_dst_and_src_filter(
    &self,
    dst_vid: VidRef<'_>,
    e_label: LabelRef<'_>,
    e_attr: Option<&PatternAttr>,
    src_v_label: LabelRef<'_>,
    src_v_attr: Option<&PatternAttr>,
  ) -> Vec<DataEdge> {
    let mut query_str = format!("\n\t\tMATCH (src: {src_v_label})-[e: {e_label}]->(dst)\n");
    query_str += &format!("\t\tWHERE elementId(dst) = '{dst_vid}'\n");
    if let Some(attr) = e_attr {
      let constraint = attr.to_neo4j_constraint("e");
      query_str += &format!("\t\tAND {constraint}\n");
    }
    if let Some(attr) = src_v_attr {
      let constraint = attr.to_neo4j_constraint("src");
      query_str += &format!("\t\tAND {constraint}\n");
    }
    query_str += "
      RETURN
        elementId(e) AS eid,
        properties(e) AS props,
        elementId(src) AS src_vid,
        elementId(dst) AS dst_vid"
      .trim_start_matches('\n');

    let mut result = time_async_with_desc(self.graph.execute(query(&query_str)), query_str)
      .await
      .unwrap();

    let mut ret = vec![];
    while let Some(row) = result.next().await.unwrap() {
      ret.push(DataEdge::from((row, e_label)));
    }

    ret
  }
}
