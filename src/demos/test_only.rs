use crate::{demos::sqlite_test_only_db_builder::BI6Builder, executor::ExecEngine, storage::*};
use project_root::get_project_root;
use std::{path::PathBuf, sync::LazyLock};
use tokio::{fs, io};

static PLAN_ROOT: LazyLock<PathBuf> =
  LazyLock::new(|| get_project_root().unwrap().join("resources").join("plan"));

async fn exec(plan_filename: &str) -> io::Result<()> {
  let mut path = PLAN_ROOT.clone();
  path.push(plan_filename);
  let plan_json_content = fs::read_to_string(path).await?;

  let result = ExecEngine::<SqliteStorageAdapter>::build_test_only_from_json(&plan_json_content)
    .await
    .exec()
    .await;

  println!("🔍  Count(result) = {}\n", result.len());
  Ok(())
}

pub async fn bi_6_minimized() -> io::Result<()> {
  println!("Building 'BI-6' (Minimized Dataset) ...\n");
  BI6Builder::build().await;

  println!("Querying 'BI-6' on 'Minimized Dataset' ...\n");
  exec("ldbc-bi-6.json").await
}
