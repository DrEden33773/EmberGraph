use dotenv::dotenv;
#[cfg(windows)]
use mimalloc::MiMalloc;
#[cfg(unix)]
use tikv_jemallocator::Jemalloc;
use tokio::{io, runtime};

#[cfg(unix)]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(windows)]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

async fn to_run() -> io::Result<()> {
  dotenv().ok();

  #[cfg(feature = "use_tracing")]
  #[allow(unused_variables)]
  let guard = ember_graph::init_log::init_log().await?;

  // plan_gen().await?;
  run_demo().await?;

  Ok(())
}

fn main() -> io::Result<()> {
  // rayon config
  rayon::ThreadPoolBuilder::new()
    .num_threads(num_cpus::get() / 2)
    .thread_name(|i| format!("rayon-{}", i))
    .build_global()
    .unwrap();

  // tokio config
  runtime::Builder::new_multi_thread()
    .enable_all()
    .worker_threads(num_cpus::get() / 2)
    .thread_name_fn(|| {
      use std::sync::atomic::{AtomicUsize, Ordering};
      static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
      let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
      format!("tokio-{}", id)
    })
    .build()
    .unwrap()
    .block_on(to_run())
}

#[allow(dead_code)]
async fn run_test_only() -> io::Result<()> {
  use ember_graph::demos::test_only::*;

  bi_6_minimized().await?;

  Ok(())
}

#[allow(dead_code)]
async fn run_demo() -> io::Result<()> {
  #[allow(unused_imports)]
  use ember_graph::demos::bi_sf01::*;

  // bi_1_on_sf_01().await?;
  // bi_2_on_sf_01().await?;
  // bi_4_on_sf_01().await?;
  // bi_5_on_sf_01().await?;
  // bi_6_on_sf_01().await?;
  // bi_7_on_sf_01().await?;
  // bi_8_on_sf_01().await?;
  // bi_9_on_sf_01().await?;
  // bi_11_on_sf_01().await?;
  // bi_12_on_sf_01().await?;
  // bi_13_on_sf_01().await?;
  // bi_14_on_sf_01().await?;
  // bi_15_on_sf_01().await?;
  // bi_16_on_sf_01().await?;
  // bi_17_on_sf_01().await?;
  // bi_18_on_sf_01().await?;
  // bi_19_on_sf_01().await?;
  // bi_20_on_sf_01().await?;

  // bi_3_on_sf_01().await?;
  bi_10_on_sf_01().await?;

  Ok(())
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

  Ok(())
}
