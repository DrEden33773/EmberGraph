use dotenv::dotenv;
use mimalloc::MiMalloc;
use std::sync::LazyLock;
use tokio::{io, runtime};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

static NUM_CPUS: LazyLock<usize> = LazyLock::new(num_cpus::get);

fn main() -> io::Result<()> {
  // rayon config
  rayon::ThreadPoolBuilder::new()
    .num_threads(*NUM_CPUS / 2)
    .thread_name(|i| format!("rayon-{}", i))
    .build_global()
    .unwrap();

  // tokio config (multi_threaded)
  #[cfg(not(feature = "single_thread_debug"))]
  {
    use std::sync::atomic::{AtomicUsize, Ordering};

    runtime::Builder::new_multi_thread()
      .enable_all()
      .worker_threads(*NUM_CPUS / 2)
      .thread_name_fn(|| {
        static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
        let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
        format!("tokio-{}", id)
      })
      .build()
      .unwrap()
      .block_on(to_run())
  }

  // tokio config (single_threaded)
  #[cfg(feature = "single_thread_debug")]
  {
    runtime::Builder::new_current_thread()
      .enable_all()
      .build()
      .unwrap()
      .block_on(to_run())
  }
}

async fn to_run() -> io::Result<()> {
  dotenv().ok();

  #[cfg(feature = "use_tracing")]
  #[allow(dead_code)]
  let guard = ember_graph::init_log::init_log().await?;

  // plan_gen().await?;
  run_demo().await?;
  // run_test_only().await?;

  Ok(())
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
  use ember_graph::demos::{bi_sf01::*, complex_interactive_sf01::*, simple_interactive_sf01::*};

  // bi_6_on_sf_01().await?;
  // bi_2_on_sf_01().await?;
  // bi_14_on_sf_01().await?;

  // ic_4_on_sf_01().await?;

  // bi_10_on_sf_01().await?;
  // bi_3_on_sf_01().await?;
  bi_5_on_sf_01().await?;

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
        "☑️   Plan file generated: '{}'",
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
