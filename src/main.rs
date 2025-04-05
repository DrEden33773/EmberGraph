use dotenv::dotenv;
use tokio::io::{self};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
  dotenv().ok();

  #[cfg(feature = "use_tracing")]
  let _guard = ember_graph::init_log::init_log().await?;

  // plan_gen().await?;
  run_demo().await?;

  Ok(())
}

#[allow(dead_code)]
async fn run_demo() -> io::Result<()> {
  #[allow(unused_imports)]
  use ember_graph::demos::{bi_sf01::*, complex_interactive_sf01::*, simple_interactive_sf01::*};

  // bi_6_on_sf_01().await?;
  // bi_2_on_sf_01().await?;
  bi_14_on_sf_01().await
}

#[allow(dead_code)]
async fn plan_gen() -> io::Result<()> {
  use colored::Colorize;
  use ember_graph::planner::generate_optimal_plan;
  use project_root::get_project_root;

  let queries = get_project_root()?.join("resources").join("queries");
  let plans = get_project_root()?.join("resources").join("plan");

  let mut handles = vec![];

  // iterate over all files in the directory
  for entry in std::fs::read_dir(queries)? {
    let entry = entry?;

    let path = entry.path();
    if !path.is_file() {
      eprintln!(
        "⚠️  (Skipped) Not a file: '{}'",
        path.to_str().unwrap().yellow()
      );
      continue;
    }
    if let Some(ext) = path.extension() {
      if ext != "txt" {
        continue;
      }
    }

    let filename = path.file_stem().unwrap().to_str().unwrap().to_string();
    let plans = plans.clone();

    println!(
      "🪄  Generating plan for query: '{}'",
      path.to_str().unwrap().green()
    );
    let handle = tokio::spawn(async move {
      let plan_data = generate_optimal_plan(&path);
      let plan_json = serde_json::to_string_pretty(&plan_data).unwrap();
      let filepath = plans.join(format!("{}.json", filename));

      tokio::fs::write(filepath.clone(), plan_json)
        .await
        .expect("❌  Failed to write plan file");

      println!(
        "✅  Plan file generated: '{}'",
        filepath.to_str().unwrap().green()
      );
    });
    handles.push(handle);
  }

  // wait for all tasks to complete
  for handle in handles {
    if let Err(e) = handle.await {
      eprintln!("⚠️  Task failed: {}", e);
    }
  }

  Ok(())
}
