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
    ExecEngine::<CachedStorageAdapter<SqliteStorageAdapter>>::build_from_json(&plan_json_content)
      .await
      .exec()
      .await;

  println!("🔍  Count(result) = {}\n", result.len());
  Ok(())
}

/// ✅
pub async fn is_1_on_sf_01() -> io::Result<()> {
  println!("Querying 'IS-1' on 'SF0.1' ...\n");
  exec("ldbc-is-1.json").await
}

/// ✅
pub async fn is_3_on_sf_01() -> io::Result<()> {
  println!("Querying 'IS-3' on 'SF0.1' ...\n");
  exec("ldbc-is-3-single-directed-knows.json").await
}

/// ✅
pub async fn is_3_double_directed_knows_on_sf_01() -> io::Result<()> {
  println!("Querying 'IS-3' on 'SF0.1' ...\n");
  exec("ldbc-is-3-double-directed-knows.json").await
}

/// ✅
pub async fn is_3_reversed_directed_knows_on_sf_01() -> io::Result<()> {
  println!("Querying 'IS-3' on 'SF0.1' ...\n");
  exec("ldbc-is-3-reversed-directed-knows.json").await
}
