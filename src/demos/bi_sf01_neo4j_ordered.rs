use crate::{executor::ExecEngine, result_dump::ResultDumper, storage::*, utils::time_async};
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

pub async fn query<S: AdvancedStorageAdapter + 'static>(plan_filename: &str) -> io::Result<()> {
  let mut path = PLAN_ROOT.clone();
  path.push(plan_filename);
  let plan_json_content = fs::read_to_string(path).await?;

  let (result, elapsed) = time_async(
    ExecEngine::<CachedStorageAdapter<S>>::build_from_json(&plan_json_content)
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
