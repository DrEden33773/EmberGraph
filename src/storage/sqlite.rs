use super::{
  AdvancedStorageAdapter, AsyncDefault, StorageAdapter, TestOnlyStorageAdapter,
  WritableStorageAdapter,
};
use crate::{
  schemas::{AttrType, AttrValue, DataEdge, DataVertex, LabelRef, PatternAttr, VidRef},
  utils::time_async_with_desc,
};
use hashbrown::HashMap;
use project_root::get_project_root;
use sqlx::{
  Execute, Row, SqlitePool,
  sqlite::{
    SqliteConnectOptions, SqliteJournalMode, SqliteLockingMode, SqliteRow, SqliteSynchronous,
  },
};
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

    let pool = sqlx::SqlitePool::connect_with(
      SqliteConnectOptions::new()
        .filename(&db_path)
        .create_if_missing(true)
        .immutable(true)
        .read_only(true)
        .row_buffer_size(4096)
        .page_size(4096)
        .locking_mode(SqliteLockingMode::Exclusive)
        .journal_mode(SqliteJournalMode::Off)
        .synchronous(SqliteSynchronous::Off)
        .statement_cache_capacity(1_000_000)
        .pragma("temp_store", "MEMORY")
        .pragma("mmap_size", "30000000000"),
    )
    .await
    .expect("❌ Failed to connect to SQLite database");

    Self::init_schema(&pool).await;

    Self { pool }
  }
}

impl SqliteStorageAdapter {
  async fn clear_tables(pool: &SqlitePool) {
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
      let q = sqlx::query(query);
      time_async_with_desc(q.execute(pool), query.to_string())
        .await
        .unwrap();
    }

    // reset auto increment values
    let reset_queries = vec![
      "\n\t\tUPDATE sqlite_sequence SET seq = 0 WHERE name = 'vertex_attribute'",
      "\n\t\tUPDATE sqlite_sequence SET seq = 0 WHERE name = 'edge_attribute'",
    ];

    for query in reset_queries {
      let q = sqlx::query(query);
      time_async_with_desc(q.execute(pool), query.to_string())
        .await
        .unwrap();
    }
  }

  async fn init_schema(pool: &SqlitePool) {
    // create db_vertex table
    sqlx::query(
      r#"
      CREATE TABLE IF NOT EXISTS db_vertex (
        vid VARCHAR PRIMARY KEY,
        label VARCHAR NOT NULL
      );
      CREATE INDEX IF NOT EXISTS ix_db_vertex_label ON db_vertex(label);
      "#,
    )
    .execute(pool)
    .await
    .expect("❌  Failed to create `db_vertex` table");

    // create db_edge table
    sqlx::query(
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
    .execute(pool)
    .await
    .expect("❌  Failed to create `db_edge` table");

    // create vertex_attribute table
    sqlx::query(
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
    .execute(pool)
    .await
    .expect("❌  Failed to create `vertex_attribute` table");

    // create edge_attribute table
    sqlx::query(
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
    let sql = query.sql();
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
    let sql = query.sql();
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
    let sql = query.sql();

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
    let sql = query.sql();
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

impl WritableStorageAdapter for SqliteStorageAdapter {
  async fn add_v(&self, v: DataVertex) -> Result<(), Box<dyn std::error::Error>> {
    let mut tx = self.pool.begin().await?;

    let query_str = "
      INSERT INTO db_vertex (vid, label) VALUES (?, ?)";
    let query = sqlx::query(query_str).bind(&v.vid).bind(&v.label);
    let sql = query.sql();

    match time_async_with_desc(query.execute(&mut *tx), sql.to_string()).await {
      Ok(result) => println!("💾  Inserted {} vertex", result.rows_affected()),
      Err(e) => {
        eprintln!("❌  Error inserting vertex: {}", e);
        return Err(Box::new(e));
      }
    };

    for (key, value) in &v.attrs {
      let attr_query = sqlx::query(
        "
      INSERT INTO vertex_attribute (vid, key, value, type) VALUES (?, ?, ?, ?)",
      )
      .bind(&v.vid)
      .bind(key)
      .bind(value.to_string())
      .bind(value.to_type().to_string());
      let sql = attr_query.sql();

      match time_async_with_desc(attr_query.execute(&mut *tx), sql.to_string()).await {
        Ok(result) => println!("💾  Inserted {} vertex_attribute", result.rows_affected()),
        Err(e) => {
          eprintln!("❌  Error inserting vertex attribute: {}", e);
          return Err(Box::new(e));
        }
      }
    }

    tx.commit().await?;

    Ok(())
  }

  async fn add_e(&self, e: DataEdge) -> Result<(), Box<dyn std::error::Error>> {
    let mut tx = self.pool.begin().await?;

    let query_str = "
      INSERT INTO db_edge (eid, label, src_vid, dst_vid) VALUES (?, ?, ?, ?)";
    let query = sqlx::query(query_str)
      .bind(&e.eid)
      .bind(&e.label)
      .bind(&e.src_vid)
      .bind(&e.dst_vid);
    let sql = query.sql();

    match time_async_with_desc(query.execute(&mut *tx), sql.to_string()).await {
      Ok(result) => println!("💾  Inserted {} edge", result.rows_affected()),
      Err(e) => {
        eprintln!("❌  Error inserting edge: {}", e);
        return Err(Box::new(e));
      }
    };

    for (key, value) in &e.attrs {
      let attr_query = sqlx::query(
        "
      INSERT INTO edge_attribute (eid, key, value, type) VALUES (?, ?, ?, ?)",
      )
      .bind(&e.eid)
      .bind(key)
      .bind(value.to_string())
      .bind(value.to_type().to_string());
      let sql = attr_query.sql();

      match time_async_with_desc(attr_query.execute(&mut *tx), sql.to_string()).await {
        Ok(result) => println!("💾  Inserted {} edge_attribute", result.rows_affected()),
        Err(e) => {
          eprintln!("❌  Error inserting edge attribute: {}", e);
          return Err(Box::new(e));
        }
      }
    }

    tx.commit().await?;

    Ok(())
  }
}

impl TestOnlyStorageAdapter for SqliteStorageAdapter {
  async fn async_init_test_only() -> Self {
    let db_name = env::var("TEST_ONLY_SQLITE_DB_PATH").unwrap();
    // Create an absolute path to the current directory
    let root = get_project_root().unwrap();
    let db_path = root.join(db_name);
    println!("🔍  Using database at: {}\n", db_path.display());

    // Delete existing file if it exists to ensure we start fresh
    if db_path.exists() {
      std::fs::remove_file(&db_path).expect("Failed to remove existing database file");
      println!("🗑️  Removed existing database file\n");
    }

    let pool = sqlx::SqlitePool::connect_with(
      SqliteConnectOptions::new()
        .filename(&db_path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Truncate)
        .synchronous(SqliteSynchronous::Off),
    )
    .await
    .expect("❌ Failed to connect to SQLite database");

    Self::init_schema(&pool).await;
    Self::clear_tables(&pool).await;

    Self { pool }
  }
}

impl SqliteStorageAdapter {
  pub async fn count_v(&self) -> usize {
    let query = sqlx::query(
      "
      SELECT COUNT(*) FROM db_vertex",
    );
    let sql = query.sql();

    let row = time_async_with_desc(query.fetch_one(&self.pool), sql.to_string())
      .await
      .unwrap();

    row.get::<i64, _>(0) as usize
  }

  pub async fn count_e(&self) -> usize {
    let query = sqlx::query(
      "
      SELECT COUNT(*) FROM db_edge",
    );
    let sql = query.sql();

    let row = time_async_with_desc(query.fetch_one(&self.pool), sql.to_string())
      .await
      .unwrap();

    row.get::<i64, _>(0) as usize
  }
}
