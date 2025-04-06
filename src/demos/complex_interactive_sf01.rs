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

  println!("🔍  Count(result) = {}\n", result.len());
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
