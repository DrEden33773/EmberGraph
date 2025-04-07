use crate::{executor::ExecEngine, storage::*};
use project_root::get_project_root;
use std::{path::PathBuf, sync::LazyLock};
use tokio::{fs, io};

static PLAN_ROOT: LazyLock<PathBuf> =
  LazyLock::new(|| get_project_root().unwrap().join("resources").join("plan"));

async fn exec(plan_filename: &str) -> io::Result<()> {
  let mut path = PLAN_ROOT.clone();
  path.push(plan_filename);
  let plan_json_content = fs::read_to_string(path).await?;

  let result =
    ExecEngine::<CachedStorageAdapter<Neo4jStorageAdapter>>::build_from_json(&plan_json_content)
      .await
      .exec()
      .await;

  println!("✨  Count(result) = {}\n", result.len());
  Ok(())
}

/// ☑️
pub async fn bi_2_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-2' on 'SF0.1' ...\n");
  exec("ldbc-bi-2.json").await
}

/// ☑️ ⚠️ (Not that fast)
/// - `SqliteAdapter` is faster
pub async fn bi_3_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-3' on 'SF0.1' ...\n");
  exec("ldbc-bi-3.json").await
}

/// ☑️
pub async fn bi_4_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-4' on 'SF0.1' ...\n");
  exec("ldbc-bi-4.json").await
}

/// ☑️
pub async fn bi_5_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-5' on 'SF0.1' ...\n");
  exec("ldbc-bi-5.json").await
}

/// ☑️
pub async fn bi_6_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-6' on 'SF0.1' ...\n");
  exec("ldbc-bi-6.json").await
}

/// ☑️
pub async fn bi_7_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-7' on 'SF0.1' ...\n");
  exec("ldbc-bi-7.json").await
}

/// ☑️
pub async fn bi_8_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-8' on 'SF0.1' ...\n");
  exec("ldbc-bi-8.json").await
}

/// ☑️
pub async fn bi_9_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-9' on 'SF0.1' ...\n");
  exec("ldbc-bi-9.json").await
}

/// ☑️ ⚠️  Slow query: `GetAdj("f^otherTag")`
/// - Memory usage is normal, computation process is too slow
/// - `Neo4jStorageAdapter` is `slower` than `SqliteStorageAdapter`
pub async fn bi_10_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-10' on 'SF0.1' ...\n");
  exec("ldbc-bi-10.json").await
}

/// ☑️
pub async fn bi_11_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-11' on 'SF0.1' ...\n");
  exec("ldbc-bi-11.json").await
}

/// ☑️
pub async fn bi_12_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-12' on 'SF0.1' ...\n");
  exec("ldbc-bi-12.json").await
}

/// ☑️
pub async fn bi_13_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-13' on 'SF0.1' ...\n");
  exec("ldbc-bi-13.json").await
}

/// ☑️
pub async fn bi_14_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-14' on 'SF0.1' ...\n");
  exec("ldbc-bi-14.json").await
}

pub async fn bi_17_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-17' on 'SF0.1' ...\n");
  exec("ldbc-bi-17.json").await
}
