use colored::Colorize;
use dotenv::dotenv;
use ember_graph::utils::parallel;
#[cfg(windows)]
use mimalloc::MiMalloc;
#[cfg(unix)]
use tikv_jemallocator::Jemalloc;
use tokio::io;

#[cfg(unix)]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(windows)]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

async fn to_run() -> io::Result<()> {
  dotenv().ok();

  println!();

  #[cfg(feature = "use_tracing")]
  #[allow(unused_variables)]
  let guard = ember_graph::init_log::init_log().await?;

  plan_gen().await?;

  println!(
    "⚠️  If you want to query `{}`, use `{}` instead.\n",
    "bi_x".yellow(),
    "cargo run --example bi_x".yellow()
  );

  Ok(())
}

fn main() -> io::Result<()> {
  parallel::config_before_run(to_run())
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
      eprintln!("❌  Task failed: {}", e);
    }
  }

  println!("✅  All plans generated\n");

  Ok(())
}
