use super::{AdvancedStorageAdapter, AsyncDefault, StorageAdapter};
use crate::{
  schemas::{AttrType, AttrValue, DataEdge, DataVertex, LabelRef, PatternAttr, VidRef},
  utils::time_async_with_desc,
};
use hashbrown::HashMap;
use project_root::get_project_root;
use sqlx::{Execute, Row, SqlitePool, sqlite::SqliteRow};
use std::env;

#[derive(Clone)]
pub struct SqliteStorageAdapter {
  pool: SqlitePool,
}

impl AsyncDefault for SqliteStorageAdapter {
  async fn async_default() -> Self {
    let db_name = env::var("SQLITE_DB_PATH").unwrap();
    let root = get_project_root().unwrap();
    let db_path = root.join(db_name);
    let url = format!("sqlite://{}", db_path.display());

    let pool = sqlx::SqlitePool::connect(&url)
      .await
      .expect("❌  Failed to connect to SQLite database");

    // create schema if it doesn't exist
    Self::init_schema(&pool).await;

    Self { pool }
  }
}

impl SqliteStorageAdapter {
  async fn init_schema(pool: &SqlitePool) {
    // create db_vertex table
    sqlx::query(
      r#"
      CREATE TABLE IF NOT EXISTS db_vertex (
        vid TEXT PRIMARY KEY,
        label TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_vertex_label ON db_vertex(label);
      "#,
    )
    .execute(pool)
    .await
    .expect("❌  Failed to create `db_vertex` table");

    // create db_edge table
    sqlx::query(
      r#"
      CREATE TABLE IF NOT EXISTS db_edge (
        eid TEXT PRIMARY KEY,
        label TEXT NOT NULL,
        src_vid TEXT NOT NULL,
        dst_vid TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_edge_label ON db_edge(label);
      CREATE INDEX IF NOT EXISTS idx_edge_src_vid ON db_edge(src_vid);
      CREATE INDEX IF NOT EXISTS idx_edge_dst_vid ON db_edge(dst_vid);
      "#,
    )
    .execute(pool)
    .await
    .expect("❌  Failed to create `db_edge` table");

    // create vertex_attribute table
    sqlx::query(
      r#"
      CREATE TABLE IF NOT EXISTS vertex_attribute (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vid TEXT NOT NULL,
        key TEXT NOT NULL,
        value TEXT NOT NULL,
        type TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_vertex_attr_vid ON vertex_attribute(vid);
      CREATE INDEX IF NOT EXISTS idx_vertex_attr_key ON vertex_attribute(key);
      "#,
    )
    .execute(pool)
    .await
    .expect("❌  Failed to create `vertex_attribute` table");

    // create edge_attribute table
    sqlx::query(
      r#"
      CREATE TABLE IF NOT EXISTS edge_attribute (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        eid TEXT NOT NULL,
        key TEXT NOT NULL,
        value TEXT NOT NULL,
        type TEXT NOT NULL
      );
      CREATE INDEX IF NOT EXISTS idx_edge_attr_eid ON edge_attribute(eid);
      CREATE INDEX IF NOT EXISTS idx_edge_attr_key ON edge_attribute(key);
      "#,
    )
    .execute(pool)
    .await
    .expect("❌  Failed to create edge_attribute table");
  }

  async fn query_edge_with_attr_then_collect(
    &self,
    e_attr: Option<&PatternAttr>,
    mut query_str: String,
    mut params: Vec<String>,
  ) -> Vec<DataEdge> {
    // add attribute filter
    if let Some(attr) = e_attr {
      query_str.push_str(
        r#"
      AND EXISTS (
        SELECT * FROM edge_attribute 
        WHERE eid = e.eid AND key = ?
      "#,
      );
      params.push(attr.key.clone());
      add_attr_filter(attr, &mut query_str, &mut params);
    }

    // execute query
    let mut query = sqlx::query(&query_str);
    for param in params {
      query = query.bind(param);
    }

    // collect rows
    let sql = query.sql().trim();
    let rows = match time_async_with_desc(query.fetch_all(&self.pool), sql.to_string()).await {
      Ok(rows) => rows,
      Err(_) => return vec![],
    };

    collect_edges(rows).into_values().collect()
  }

  async fn query_vertex_with_attr_then_collect(
    &self,
    v_attr: Option<&PatternAttr>,
    mut query_str: String,
    mut params: Vec<String>,
  ) -> Vec<DataVertex> {
    // add attribute filter
    if let Some(attr) = v_attr {
      query_str.push_str(
        r#"
      AND EXISTS (
        SELECT * FROM vertex_attribute 
        WHERE vid = v.vid AND key = ?
      "#,
      );
      params.push(attr.key.clone());
      add_attr_filter(attr, &mut query_str, &mut params);
    }

    // execute query
    let mut query = sqlx::query(&query_str);
    for param in params {
      query = query.bind(param);
    }

    // collect rows
    let sql = query.sql().trim();
    let rows = match time_async_with_desc(query.fetch_all(&self.pool), sql.to_string()).await {
      Ok(rows) => rows,
      Err(_) => return vec![],
    };

    collect_vertices(rows).into_values().collect()
  }
}

fn add_attr_filter(attr: &PatternAttr, query_str: &mut String, params: &mut Vec<String>) {
  match &attr.value {
    AttrValue::Int(val) => {
      query_str.push_str(&format!(
        "  AND type = '{}' AND CAST(value AS INTEGER) ",
        AttrType::Int
      ));
      query_str.push_str(attr.op.to_neo4j_sqlite_repr());
      query_str.push_str(" ?");
      query_str.push_str(
        "
      )",
      );
      params.push(val.to_string());
    }
    AttrValue::Float(val) => {
      query_str.push_str(&format!(
        "  AND type = '{}' AND CAST(value AS REAL) ",
        AttrType::Float
      ));
      query_str.push_str(attr.op.to_neo4j_sqlite_repr());
      query_str.push_str(" ?");
      query_str.push_str(
        "
      )",
      );
      params.push(val.to_string());
    }
    AttrValue::String(val) => {
      query_str.push_str(&format!("  AND type = '{}' AND value ", AttrType::String));
      query_str.push_str(attr.op.to_neo4j_sqlite_repr());
      query_str.push_str(" ?");
      query_str.push_str(
        "
      )",
      );
      params.push(val.clone());
    }
  }
}

fn collect_vertices(rows: Vec<SqliteRow>) -> HashMap<String, DataVertex> {
  let mut vertices = HashMap::new();

  rows.into_iter().for_each(|row| {
    let vid: String = row.get("vid");

    // init never-seen vertex
    if !vertices.contains_key(&vid) {
      let label: String = row.get("label");
      vertices.insert(
        vid.clone(),
        DataVertex {
          vid: vid.clone(),
          label,
          attrs: HashMap::new(),
        },
      );
    }

    // add attribute if it exists in current row
    if let Ok(key) = row.try_get::<String, _>("key") {
      let value: String = row.get("value");
      let type_: String = row.get("type");
      let typed_value = get_typed_value(&type_, value);

      vertices
        .get_mut(&vid)
        .unwrap()
        .attrs
        .insert(key, typed_value);
    }
  });

  vertices
}

fn collect_edges(rows: Vec<SqliteRow>) -> HashMap<String, DataEdge> {
  let mut edges = HashMap::new();

  rows.into_iter().for_each(|row| {
    let eid: String = row.get("eid");

    // init never-seen edge
    if !edges.contains_key(&eid) {
      let label: String = row.get("label");
      let src_vid: String = row.get("src_vid");
      let dst_vid: String = row.get("dst_vid");

      edges.insert(
        eid.clone(),
        DataEdge {
          eid: eid.clone(),
          label,
          src_vid,
          dst_vid,
          attrs: HashMap::new(),
        },
      );
    }

    // add attribute if it exists in current row
    if let Ok(key) = row.try_get::<String, _>("key") {
      let value: String = row.get("value");
      let type_: String = row.get("type");
      let typed_value = get_typed_value(&type_, value);

      edges.get_mut(&eid).unwrap().attrs.insert(key, typed_value);
    }
  });

  edges
}

fn get_typed_value(type_: &str, value: String) -> AttrValue {
  match type_ {
    "int" => AttrValue::Int(value.parse().unwrap_or(0)),
    "float" => AttrValue::Float(value.parse().unwrap_or(0.0)),
    _ => AttrValue::String(value),
  }
}

impl StorageAdapter for SqliteStorageAdapter {
  async fn get_v(&self, vid: VidRef<'_>) -> Option<DataVertex> {
    let query = sqlx::query(
      r#"
      SELECT v.vid, v.label, a.key, a.value, a.type
      FROM db_vertex v
      LEFT JOIN vertex_attribute a ON v.vid = a.vid
      WHERE v.vid = ?
      "#,
    )
    .bind(vid);
    let sql = query.sql().trim();

    let rows = time_async_with_desc(query.fetch_all(&self.pool), sql.to_string())
      .await
      .ok()?;
    if rows.is_empty() {
      return None;
    }

    let vid: String = rows[0].get("vid");
    let label: String = rows[0].get("label");
    let mut attrs = HashMap::new();

    // collect all attrs
    for row in rows {
      if let Ok(key) = row.try_get::<String, _>("key") {
        let value: String = row.get("value");
        let type_: String = row.get("type");

        let typed_value = match type_.as_str() {
          "int" => AttrValue::Int(value.parse().unwrap_or(0)),
          "float" => AttrValue::Float(value.parse().unwrap_or(0.0)),
          _ => AttrValue::String(value),
        };

        attrs.insert(key, typed_value);
      }
    }

    Some(DataVertex { vid, label, attrs })
  }

  async fn load_v(&self, v_label: LabelRef<'_>, v_attr: Option<&PatternAttr>) -> Vec<DataVertex> {
    let query_str = String::from(
      r#"
      SELECT v.vid, v.label, a.key, a.value, a.type
      FROM db_vertex v
      LEFT JOIN vertex_attribute a ON v.vid = a.vid
      WHERE v.label = ?"#,
    );
    let params = vec![v_label.to_string()];

    self
      .query_vertex_with_attr_then_collect(v_attr, query_str, params)
      .await
  }

  async fn load_e(&self, e_label: LabelRef<'_>, e_attr: Option<&PatternAttr>) -> Vec<DataEdge> {
    let query_str = String::from(
      r#"
      SELECT e.eid, e.label, e.src_vid, e.dst_vid, a.key, a.value, a.type
      FROM db_edge e
      LEFT JOIN edge_attribute a ON e.eid = a.eid
      WHERE e.label = ?"#,
    );
    let params = vec![e_label.to_string()];

    self
      .query_edge_with_attr_then_collect(e_attr, query_str, params)
      .await
  }

  async fn load_e_with_src(
    &self,
    src_vid: VidRef<'_>,
    e_label: LabelRef<'_>,
    e_attr: Option<&PatternAttr>,
  ) -> Vec<DataEdge> {
    let query_str = String::from(
      r#"
      SELECT e.eid, e.label, e.src_vid, e.dst_vid, a.key, a.value, a.type
      FROM db_edge e
      LEFT JOIN edge_attribute a ON e.eid = a.eid
      WHERE e.src_vid = ? AND e.label = ?"#,
    );
    let params = vec![src_vid.to_string(), e_label.to_string()];

    self
      .query_edge_with_attr_then_collect(e_attr, query_str, params)
      .await
  }

  async fn load_e_with_dst(
    &self,
    dst_vid: VidRef<'_>,
    e_label: LabelRef<'_>,
    e_attr: Option<&PatternAttr>,
  ) -> Vec<DataEdge> {
    let query_str = String::from(
      r#"
      SELECT e.eid, e.label, e.src_vid, e.dst_vid, a.key, a.value, a.type
      FROM db_edge e
      LEFT JOIN edge_attribute a ON e.eid = a.eid
      WHERE e.dst_vid = ? AND e.label = ?"#,
    );
    let params = vec![dst_vid.to_string(), e_label.to_string()];

    self
      .query_edge_with_attr_then_collect(e_attr, query_str, params)
      .await
  }
}

impl SqliteStorageAdapter {
  async fn query_edge_with_attr_and_next_v_attr_then_collect(
    &self,
    e_attr: Option<&PatternAttr>,
    next_v_attr: Option<&PatternAttr>,
    mut query_str: String,
    mut params: Vec<String>,
  ) -> Vec<DataEdge> {
    // add e_attr filter
    if let Some(e_attr) = e_attr {
      query_str.push_str(
        r#"
      AND EXISTS (
        SELECT * FROM edge_attribute 
        WHERE eid = e.eid AND key = ?
      "#,
      );
      params.push(e_attr.key.clone());
      add_attr_filter(e_attr, &mut query_str, &mut params);
    }

    // add next_v_attr filter
    if let Some(v_attr) = next_v_attr {
      query_str.push_str(
        r#"
      AND EXISTS (
        SELECT * FROM vertex_attribute 
        WHERE vid = v.vid AND key = ?
      "#,
      );
      params.push(v_attr.key.clone());
      add_attr_filter(v_attr, &mut query_str, &mut params);
    }

    // execute query
    let mut query = sqlx::query(&query_str);
    for param in params {
      query = query.bind(param);
    }

    // collect rows
    let sql = query.sql().trim();
    let rows = match time_async_with_desc(query.fetch_all(&self.pool), sql.to_string()).await {
      Ok(rows) => rows,
      Err(_) => return vec![],
    };

    collect_edges(rows).into_values().collect()
  }
}

impl AdvancedStorageAdapter for SqliteStorageAdapter {
  async fn load_e_with_src_and_dst_filter(
    &self,
    src_vid: VidRef<'_>,
    e_label: LabelRef<'_>,
    e_attr: Option<&PatternAttr>,
    dst_v_label: LabelRef<'_>,
    dst_v_attr: Option<&PatternAttr>,
  ) -> Vec<DataEdge> {
    let query_str = String::from(
      r#"
      SELECT e.eid, e.label, e.src_vid, e.dst_vid, ea.key, ea.value, ea.type
      FROM db_edge e
      LEFT JOIN edge_attribute ea ON e.eid = ea.eid
      JOIN db_vertex v ON e.dst_vid = v.vid
      WHERE e.src_vid = ? AND e.label = ? AND v.label = ?"#,
    );
    let params = vec![
      src_vid.to_string(),
      e_label.to_string(),
      dst_v_label.to_string(),
    ];

    self
      .query_edge_with_attr_and_next_v_attr_then_collect(e_attr, dst_v_attr, query_str, params)
      .await
  }

  async fn load_e_with_dst_and_src_filter(
    &self,
    dst_vid: VidRef<'_>,
    e_label: LabelRef<'_>,
    e_attr: Option<&PatternAttr>,
    src_v_label: LabelRef<'_>,
    src_v_attr: Option<&PatternAttr>,
  ) -> Vec<DataEdge> {
    let query_str = String::from(
      r#"
      SELECT e.eid, e.label, e.src_vid, e.dst_vid, ea.key, ea.value, ea.type
      FROM db_edge e
      LEFT JOIN edge_attribute ea ON e.eid = ea.eid
      JOIN db_vertex v ON e.src_vid = v.vid
      WHERE e.dst_vid = ? AND e.label = ? AND v.label = ?"#,
    );
    let params = vec![
      dst_vid.to_string(),
      e_label.to_string(),
      src_v_label.to_string(),
    ];

    self
      .query_edge_with_attr_and_next_v_attr_then_collect(e_attr, src_v_attr, query_str, params)
      .await
  }
}
