use crate::{executor::ExecEngine, result_dump::ResultDumper, storage::*};
use project_root::get_project_root;
use std::{path::PathBuf, sync::LazyLock};
use tokio::{fs, io};

static PLAN_ROOT: LazyLock<PathBuf> =
  LazyLock::new(|| get_project_root().unwrap().join("resources").join("plan"));

async fn query<S: AdvancedStorageAdapter + 'static>(plan_filename: &str) -> io::Result<()> {
  let mut path = PLAN_ROOT.clone();
  path.push(plan_filename);
  let plan_json_content = fs::read_to_string(path).await?;

  let result = ExecEngine::<CachedStorageAdapter<S>>::build_from_json(&plan_json_content)
    .await
    .parallel_exec()
    .await;

  println!("✨  Count(result) = {}", result.len());

  if let Some(df) = ResultDumper::new(result).to_simplified_df(false) {
    println!("{}", df);
  }

  Ok(())
}

/// ✅
pub async fn bi_1_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-1' on 'SF0.1' ...\n");
  query::<Neo4jStorageAdapter>("ldbc-bi-1.json").await
}

/// ✅
pub async fn bi_2_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-2' on 'SF0.1' ...\n");
  query::<Neo4jStorageAdapter>("ldbc-bi-2.json").await
}

/// ✅ ⚠️ (Not that fast)
/// - `SqliteAdapter` is faster
pub async fn bi_3_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-3' on 'SF0.1' ...\n");
  query::<SqliteStorageAdapter>("ldbc-bi-3.json").await
}

/// ✅
pub async fn bi_4_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-4' on 'SF0.1' ...\n");
  query::<Neo4jStorageAdapter>("ldbc-bi-4.json").await
}

/// ✅
pub async fn bi_5_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-5' on 'SF0.1' ...\n");
  query::<Neo4jStorageAdapter>("ldbc-bi-5.json").await
}

/// ✅
pub async fn bi_6_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-6' on 'SF0.1' ...\n");
  query::<SqliteStorageAdapter>("ldbc-bi-6.json").await
}

/// ✅
pub async fn bi_7_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-7' on 'SF0.1' ...\n");
  query::<Neo4jStorageAdapter>("ldbc-bi-7.json").await
}

/// ✅
pub async fn bi_8_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-8' on 'SF0.1' ...\n");
  query::<Neo4jStorageAdapter>("ldbc-bi-8.json").await
}

/// ✅
pub async fn bi_9_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-9' on 'SF0.1' ...\n");
  query::<Neo4jStorageAdapter>("ldbc-bi-9.json").await
}

/// ✅ ⚠️  Slow query: `GetAdj("f^otherTag")`
/// - Memory usage is normal, computation process is too slow
/// - `Neo4jStorageAdapter` is `slower` than `SqliteStorageAdapter`
pub async fn bi_10_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-10' on 'SF0.1' ...\n");
  query::<SqliteStorageAdapter>("ldbc-bi-10.json").await
}

/// ✅
pub async fn bi_11_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-11' on 'SF0.1' ...\n");
  query::<Neo4jStorageAdapter>("ldbc-bi-11.json").await
}

/// ✅
pub async fn bi_12_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-12' on 'SF0.1' ...\n");
  query::<Neo4jStorageAdapter>("ldbc-bi-12.json").await
}

/// ✅
pub async fn bi_13_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-13' on 'SF0.1' ...\n");
  query::<Neo4jStorageAdapter>("ldbc-bi-13.json").await
}

/// ✅
pub async fn bi_14_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-14' on 'SF0.1' ...\n");
  query::<Neo4jStorageAdapter>("ldbc-bi-14.json").await
}

/// ✅
pub async fn bi_15_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-15' on 'SF0.1' ...\n");
  query::<Neo4jStorageAdapter>("ldbc-bi-15.json").await
}

/// ✅
pub async fn bi_16_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-16' on 'SF0.1' ...\n");
  query::<Neo4jStorageAdapter>("ldbc-bi-16.json").await
}

/// ✅
pub async fn bi_17_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-17' on 'SF0.1' ...\n");
  query::<Neo4jStorageAdapter>("ldbc-bi-17.json").await
}

/// ✅
pub async fn bi_18_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-18' on 'SF0.1' ...\n");
  query::<Neo4jStorageAdapter>("ldbc-bi-18.json").await
}

/// ✅
pub async fn bi_19_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-19' on 'SF0.1' ...\n");
  query::<Neo4jStorageAdapter>("ldbc-bi-19.json").await
}

/// ✅
pub async fn bi_20_on_sf_01() -> io::Result<()> {
  println!("Querying 'BI-20' on 'SF0.1' ...\n");
  query::<Neo4jStorageAdapter>("ldbc-bi-20.json").await
}
