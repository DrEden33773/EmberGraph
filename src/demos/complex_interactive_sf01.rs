use crate::{executor::ExecEngine, result_dump::ResultDumper, storage::*, utils::time_async};
use colored::Colorize;
use project_root::get_project_root;
use std::{path::PathBuf, sync::LazyLock};
use tokio::{fs, io};

static PLAN_ROOT: LazyLock<PathBuf> =
  LazyLock::new(|| get_project_root().unwrap().join("resources").join("plan"));

async fn exec(plan_filename: &str) -> io::Result<()> {
  let mut path = PLAN_ROOT.clone();
  path.push(plan_filename);
  let plan_json_content = fs::read_to_string(path).await?;

  let (result, elapsed) = time_async(
    ExecEngine::<CachedStorageAdapter<Neo4jStorageAdapter>>::build_from_json(&plan_json_content)
      .await
      .parallel_exec(),
  )
  .await;

  let len = result.len();

  if let Some(df) = ResultDumper::new(result).to_simplified_df(false) {
    println!("{df}");
  }

  println!(
    "✨  Get {} results in {} ms",
    len.to_string().green(),
    format!("{elapsed:.2}").yellow()
  );

  Ok(())
}

/// ✅
pub async fn ic_1_on_sf_01() -> io::Result<()> {
  println!("Querying 'IC-1' on 'SF0.1' ...\n");
  exec("ldbc-ic-1-single-directed-knows.json").await
}

/// ✅
pub async fn ic_4_on_sf_01() -> io::Result<()> {
  println!("Querying 'IC-4' on 'SF0.1' ...\n");
  exec("ldbc-ic-4-single-directed-knows.json").await
}

/// ✅
pub async fn ic_5_on_sf_01() -> io::Result<()> {
  println!("Querying 'IC-5' on 'SF0.1' ...\n");
  exec("ldbc-ic-5-single-directed-knows.json").await
}

/// ✅
pub async fn ic_6_on_sf_01() -> io::Result<()> {
  println!("Querying 'IC-6' on 'SF0.1' ...\n");
  exec("ldbc-ic-6-single-directed-knows.json").await
}

/// ✅
pub async fn ic_11_on_sf_01() -> io::Result<()> {
  println!("Querying 'IC-11' on 'SF0.1' ...\n");
  exec("ldbc-ic-11-single-directed-knows.json").await
}

/// ✅
pub async fn ic_12_on_sf_01() -> io::Result<()> {
  println!("Querying 'IC-12' on 'SF0.1' ...\n");
  exec("ldbc-ic-12-single-directed-knows.json").await
}
