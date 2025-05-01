use super::{AdvancedStorageAdapter, AsyncDefault, StorageAdapter};
use crate::schemas::{AttrType, AttrValue, DataEdge, DataVertex, LabelRef, PatternAttr, VidRef};
use hashbrown::HashMap;
use project_root::get_project_root;
use r2d2::{Pool, PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, params_from_iter};
use std::{env, sync::Arc};
use tokio::task;

type SqlitePool = Pool<SqliteConnectionManager>;
type SqliteConnection = PooledConnection<SqliteConnectionManager>;

#[derive(Clone)]
pub struct SqliteStorageAdapter {
  pool: Arc<SqlitePool>,
}

impl AsyncDefault for SqliteStorageAdapter {
  async fn async_default() -> Self {
    let db_name = env::var("SQLITE_DB_PATH").unwrap();
    let root = get_project_root().unwrap();
    let db_path = root.join(db_name);

    // create connection manager and connection pool
    let manager = SqliteConnectionManager::file(&db_path).with_flags(
      rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_URI,
    );

    let pool = task::spawn_blocking(move || {
      let pool = Pool::builder()
        .max_size(num_cpus::get() as u32 * 4)
        .min_idle(Some(4))
        .idle_timeout(Some(std::time::Duration::from_secs(300)))
        .max_lifetime(Some(std::time::Duration::from_secs(600)))
        .build(manager)
        .expect("❌  Failed to create SQLite connection pool");

      // apply performance optimization configs
      let conn = pool.get().expect("❌  Failed to get connection from pool");
      conn
        .execute_batch(
          "
        PRAGMA journal_mode = OFF;
        PRAGMA synchronous = 0;
        PRAGMA cache_size = 1000000;
        PRAGMA locking_mode = EXCLUSIVE;
        PRAGMA temp_store = MEMORY;
        PRAGMA mmap_size = 30000000000;
        PRAGMA page_size = 4096;
      ",
        )
        .expect("❌  Failed to set SQLite PRAGMA options");

      pool
    })
    .await
    .expect("❌  Failed to create connection pool");

    // initialize database schema
    let pool_clone = Arc::new(pool);
    task::spawn_blocking({
      let pool = pool_clone.clone();
      move || {
        let conn = pool.get().expect("❌  Failed to get connection from pool");
        Self::init_schema(&conn);
      }
    })
    .await
    .expect("❌  Failed to initialize schema");

    Self { pool: pool_clone }
  }
}

impl SqliteStorageAdapter {
  #[allow(dead_code)]
  fn clear_tables(conn: &SqliteConnection) {
    let queries = vec![
      "\n\t\tDELETE FROM db_vertex",
      "\n\t\tDELETE FROM db_edge",
      "\n\t\tDELETE FROM vertex_attribute",
      "\n\t\tDELETE FROM edge_attribute",
      // delete `auto_increment` values
      "\n\t\tDELETE FROM sqlite_sequence WHERE name = 'vertex_attribute'",
      "\n\t\tDELETE FROM sqlite_sequence WHERE name = 'edge_attribute'",
    ];

    for query in queries {
      conn.execute(query, []).unwrap();
    }

    // reset auto increment values
    let reset_queries = vec![
      "\n\t\tUPDATE sqlite_sequence SET seq = 0 WHERE name = 'vertex_attribute'",
      "\n\t\tUPDATE sqlite_sequence SET seq = 0 WHERE name = 'edge_attribute'",
    ];

    for query in reset_queries {
      conn.execute(query, []).unwrap();
    }
  }

  fn init_schema(conn: &SqliteConnection) {
    // create db_vertex table
    conn
      .execute_batch(
        r#"
      CREATE TABLE IF NOT EXISTS db_vertex (
        vid VARCHAR PRIMARY KEY,
        label VARCHAR NOT NULL
      );
      CREATE INDEX IF NOT EXISTS ix_db_vertex_label ON db_vertex(label);
      "#,
      )
      .expect("❌  Failed to create `db_vertex` table");

    // create db_edge table
    conn
      .execute_batch(
        r#"
      CREATE TABLE IF NOT EXISTS db_edge (
        eid VARCHAR PRIMARY KEY,
        label VARCHAR NOT NULL,
        src_vid VARCHAR NOT NULL,
        dst_vid VARCHAR NOT NULL
      );
      CREATE INDEX IF NOT EXISTS ix_db_edge_label ON db_edge(label);
      CREATE INDEX IF NOT EXISTS ix_db_edge_src_vid ON db_edge(src_vid);
      CREATE INDEX IF NOT EXISTS ix_db_edge_dst_vid ON db_edge(dst_vid);
      "#,
      )
      .expect("❌  Failed to create `db_edge` table");

    // create vertex_attribute table
    conn
      .execute_batch(
        r#"
      CREATE TABLE IF NOT EXISTS vertex_attribute (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        vid VARCHAR NOT NULL,
        key VARCHAR NOT NULL,
        value VARCHAR NOT NULL,
        type VARCHAR NOT NULL
      );
      CREATE INDEX IF NOT EXISTS ix_vertex_attribute_vid ON vertex_attribute(vid);
      CREATE INDEX IF NOT EXISTS ix_vertex_attribute_key ON vertex_attribute(key);
      "#,
      )
      .expect("❌  Failed to create `vertex_attribute` table");

    // create edge_attribute table
    conn
      .execute_batch(
        r#"
      CREATE TABLE IF NOT EXISTS edge_attribute (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        eid VARCHAR NOT NULL,
        key VARCHAR NOT NULL,
        value VARCHAR NOT NULL,
        type VARCHAR NOT NULL
      );
      CREATE INDEX IF NOT EXISTS ix_edge_attribute_eid ON edge_attribute(eid);
      CREATE INDEX IF NOT EXISTS ix_edge_attribute_key ON edge_attribute(key);
      "#,
      )
      .expect("❌  Failed to create edge_attribute table");
  }

  async fn query_edge_with_attr_then_collect(
    &self,
    e_attr: Option<&PatternAttr>,
    mut query_str: String,
    params: Vec<String>,
  ) -> Vec<DataEdge> {
    let pool = self.pool.clone();
    let e_attr_cloned = e_attr.cloned();

    task::spawn_blocking(move || {
      let conn = match pool.get() {
        Ok(conn) => conn,
        Err(e) => {
          eprintln!("❌  Error getting connection from pool: {}", e);
          return Vec::new();
        }
      };

      // add attr filter
      let mut all_params = params.clone();
      if let Some(attr) = e_attr_cloned.as_ref() {
        query_str.push_str(
          r#"
        AND EXISTS (
          SELECT * FROM edge_attribute 
          WHERE eid = e.eid AND key = ?
        "#,
        );
        all_params.push(attr.key.clone());
        add_attr_filter(attr, &mut query_str, &mut all_params);
      }

      let mut stmt = match conn.prepare_cached(&query_str) {
        Ok(stmt) => stmt,
        Err(e) => {
          eprintln!("❌  Error preparing query: {}", e);
          return Vec::new();
        }
      };

      let rows = match stmt.query(params_from_iter(all_params.iter())) {
        Ok(rows) => rows,
        Err(e) => {
          eprintln!("❌  Error executing query: {}", e);
          return Vec::new();
        }
      };

      let edges = collect_edges(rows);
      edges.into_values().collect()
    })
    .await
    .unwrap_or_else(|e| {
      eprintln!("❌  Task failed: {}", e);
      Vec::new()
    })
  }

  async fn query_vertex_with_attr_then_collect(
    &self,
    v_attr: Option<&PatternAttr>,
    mut query_str: String,
    params: Vec<String>,
  ) -> Vec<DataVertex> {
    let pool = self.pool.clone();
    let v_attr_cloned = v_attr.cloned();

    task::spawn_blocking(move || {
      let conn = match pool.get() {
        Ok(conn) => conn,
        Err(e) => {
          eprintln!("❌  Error getting connection from pool: {}", e);
          return Vec::new();
        }
      };

      // add attr filter
      let mut all_params = params.clone();
      if let Some(attr) = v_attr_cloned.as_ref() {
        query_str.push_str(
          r#"
        AND EXISTS (
          SELECT * FROM vertex_attribute 
          WHERE vid = v.vid AND key = ?
        "#,
        );
        all_params.push(attr.key.clone());
        add_attr_filter(attr, &mut query_str, &mut all_params);
      }

      let mut stmt = match conn.prepare_cached(&query_str) {
        Ok(stmt) => stmt,
        Err(e) => {
          eprintln!("❌  Error preparing query: {}", e);
          return Vec::new();
        }
      };

      let rows = match stmt.query(params_from_iter(all_params.iter())) {
        Ok(rows) => rows,
        Err(e) => {
          eprintln!("❌  Error executing query: {}", e);
          return Vec::new();
        }
      };

      let vertices = collect_vertices(rows);
      vertices.into_values().collect()
    })
    .await
    .unwrap_or_else(|e| {
      eprintln!("❌  Task failed: {}", e);
      Vec::new()
    })
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

fn collect_vertices(mut rows: rusqlite::Rows) -> HashMap<String, DataVertex> {
  let mut vertices = HashMap::new();

  while let Ok(Some(row)) = rows.next() {
    let vid: String = row.get(0).unwrap();

    // init unseen vertex
    if !vertices.contains_key(&vid) {
      let label: String = row.get(1).unwrap();
      vertices.insert(
        vid.clone(),
        DataVertex {
          vid: vid.clone(),
          label,
          attrs: HashMap::new(),
        },
      );
    }

    // if current row has attr, add it
    if let (Ok(key), Ok(value), Ok(type_)) = (
      row.get::<_, String>(2),
      row.get::<_, String>(3),
      row.get::<_, String>(4),
    ) {
      let typed_value = get_typed_value(&type_, value);

      vertices
        .get_mut(&vid)
        .unwrap()
        .attrs
        .insert(key, typed_value);
    }
  }

  vertices
}

fn collect_edges(mut rows: rusqlite::Rows) -> HashMap<String, DataEdge> {
  let mut edges = HashMap::new();

  while let Ok(Some(row)) = rows.next() {
    let eid: String = row.get(0).unwrap();

    // init unseen edge
    if !edges.contains_key(&eid) {
      let label: String = row.get(1).unwrap();
      let src_vid: String = row.get(2).unwrap();
      let dst_vid: String = row.get(3).unwrap();

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

    // if current row has attr, add it
    if let (Ok(key), Ok(value), Ok(type_)) = (
      row.get::<_, String>(4),
      row.get::<_, String>(5),
      row.get::<_, String>(6),
    ) {
      let typed_value = get_typed_value(&type_, value);

      edges.get_mut(&eid).unwrap().attrs.insert(key, typed_value);
    }
  }

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
    let pool = self.pool.clone();
    let vid_string = vid.to_string();

    task::spawn_blocking(move || {
      let conn = match pool.get() {
        Ok(conn) => conn,
        Err(e) => {
          eprintln!("Error getting connection from pool: {}", e);
          return None;
        }
      };

      let query = r#"
        SELECT v.vid, v.label, a.key, a.value, a.type
        FROM db_vertex v
        LEFT JOIN vertex_attribute a ON v.vid = a.vid
        WHERE v.vid = ?
      "#;

      let mut stmt = match conn.prepare_cached(query) {
        Ok(stmt) => stmt,
        Err(e) => {
          eprintln!("❌  Error preparing statement: {}", e);
          return None;
        }
      };

      let rows_result = match stmt
        .query_map(params![vid_string], |row| {
          Ok((
            row.get::<_, String>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, Option<String>>(2)?,
            row.get::<_, Option<String>>(3)?,
            row.get::<_, Option<String>>(4)?,
          ))
        })
        .and_then(|mapped_rows| mapped_rows.collect::<Result<Vec<_>, _>>())
      {
        Ok(rows) => rows,
        Err(e) => {
          eprintln!("❌  Error executing query: {}", e);
          return None;
        }
      };

      if rows_result.is_empty() {
        return None;
      }

      let vid = rows_result[0].0.clone();
      let label = rows_result[0].1.clone();
      let mut attrs = HashMap::new();

      // collect all attrs
      for row in rows_result {
        if let (Some(key), Some(value), Some(type_)) = (row.2, row.3, row.4) {
          let typed_value = match type_.as_str() {
            "int" => AttrValue::Int(value.parse().unwrap_or(0)),
            "float" => AttrValue::Float(value.parse().unwrap_or(0.0)),
            _ => AttrValue::String(value),
          };

          attrs.insert(key, typed_value);
        }
      }

      Some(DataVertex { vid, label, attrs })
    })
    .await
    .unwrap_or_else(|e| {
      eprintln!("❌  Task failed: {}", e);
      None
    })
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
    params: Vec<String>,
  ) -> Vec<DataEdge> {
    let pool = self.pool.clone();
    let e_attr_cloned = e_attr.cloned();
    let next_v_attr_cloned = next_v_attr.cloned();

    task::spawn_blocking(move || {
      let conn = match pool.get() {
        Ok(conn) => conn,
        Err(e) => {
          eprintln!("❌  Error getting connection from pool: {}", e);
          return Vec::new();
        }
      };

      // add e_attr filter
      let mut all_params = params.clone();
      if let Some(e_attr) = e_attr_cloned.as_ref() {
        query_str.push_str(
          r#"
        AND EXISTS (
          SELECT * FROM edge_attribute 
          WHERE eid = e.eid AND key = ?
        "#,
        );
        all_params.push(e_attr.key.clone());
        add_attr_filter(e_attr, &mut query_str, &mut all_params);
      }

      // add next_v_attr filter
      if let Some(v_attr) = next_v_attr_cloned.as_ref() {
        query_str.push_str(
          r#"
        AND EXISTS (
          SELECT * FROM vertex_attribute 
          WHERE vid = v.vid AND key = ?
        "#,
        );
        all_params.push(v_attr.key.clone());
        add_attr_filter(v_attr, &mut query_str, &mut all_params);
      }

      let mut stmt = match conn.prepare_cached(&query_str) {
        Ok(stmt) => stmt,
        Err(e) => {
          eprintln!("❌  Error preparing query: {}", e);
          return Vec::new();
        }
      };

      let rows = match stmt.query(params_from_iter(all_params.iter())) {
        Ok(rows) => rows,
        Err(e) => {
          eprintln!("❌  Error executing query: {}", e);
          return Vec::new();
        }
      };

      let edges = collect_edges(rows);
      edges.into_values().collect()
    })
    .await
    .unwrap_or_else(|e| {
      eprintln!("❌  Task failed: {}", e);
      Vec::new()
    })
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
