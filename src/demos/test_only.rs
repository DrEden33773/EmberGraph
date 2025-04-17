use crate::{
  demos::sqlite_test_only_db_builder::BI6Builder, executor::ExecEngine, result_dump::ResultDumper,
  storage::*, utils::time_async,
};
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
    ExecEngine::<CachedStorageAdapter<SqliteStorageAdapter>>::build_from_json(&plan_json_content)
      .await
      .parallel_exec(),
  )
  .await;

  println!(
    "✨  Get {} results in {} ms",
    result.len().to_string().green(),
    format!("{elapsed:.2}").yellow()
  );

  if let Some(df) = ResultDumper::new(result).to_simplified_df(false) {
    println!("{}", df);
  }

  Ok(())
}

pub async fn bi_6_minimized() -> io::Result<()> {
  println!("Building 'BI-6' (Minimized Dataset) ...\n");
  BI6Builder::build().await;

  println!("Querying 'BI-6' on 'Minimized Dataset' ...\n");
  exec("ldbc-bi-6.json").await
}
