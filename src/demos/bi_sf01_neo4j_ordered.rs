use crate::{executor::ExecEngine, result_dump::ResultDumper, storage::*};
use colored::Colorize;
use project_root::get_project_root;
use std::{path::PathBuf, sync::LazyLock};
use tokio::{fs, io};

static PLAN_ROOT: LazyLock<PathBuf> = LazyLock::new(|| {
  get_project_root()
    .unwrap()
    .join("resources")
    .join("plan")
    .join("neo4j_ordered")
});

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
pub async fn bi_3_on_sf_01() -> io::Result<()> {
  println!(
    "Querying 'BI-3' {} on 'SF0.1' ...\n",
    "with neo4j_matching_order".purple()
  );
  query::<SqliteStorageAdapter>("ldbc-bi-3.json").await
}

/// ✅
pub async fn bi_7_on_sf_01() -> io::Result<()> {
  println!(
    "Querying 'BI-7' {} on 'SF0.1' ...\n",
    "with neo4j_matching_order".purple()
  );
  query::<SqliteStorageAdapter>("ldbc-bi-7.json").await
}

/// ✅
pub async fn bi_11_on_sf_01() -> io::Result<()> {
  println!(
    "Querying 'BI-11' {} on 'SF0.1' ...\n",
    "with neo4j_matching_order".purple()
  );
  query::<SqliteStorageAdapter>("ldbc-bi-11.json").await
}

/// ✅
pub async fn bi_17_on_sf_01() -> io::Result<()> {
  println!(
    "Querying 'BI-17' {} on 'SF0.1' ...\n",
    "with neo4j_matching_order".purple()
  );
  query::<SqliteStorageAdapter>("ldbc-bi-17.json").await
}
