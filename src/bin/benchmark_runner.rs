use clap::Parser;
use colored::Colorize;
use dotenv::dotenv;
use ember_graph::{
  executor::ExecEngine,
  schemas::PlanData,
  storage::{AsyncDefault, CachedStorageAdapter, Neo4jStorageAdapter, SqliteStorageAdapter},
  utils::parallel,
};
use serde::Serialize;
use std::collections::HashSet;
use std::sync::LazyLock;
use std::{
  fs,
  path::PathBuf,
  process,
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
  },
  thread,
  time::{Duration, Instant, SystemTime},
};
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, ProcessRefreshKind, RefreshKind, System};
use tokio::io;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
  /// Path to the JSON query plan file (ignored if --all-bi-tasks is used).
  #[arg(short, long)]
  query_file: Option<PathBuf>,

  /// Storage adapter type to use (ignored if --all-bi-tasks is used).
  #[arg(long, value_parser = clap::value_parser!(String))]
  storage: Option<String>, // "neo4j" or "sqlite"

  /// Run benchmarks for all ldbc-bi-*.json tasks found in resources/plan.
  #[arg(long, default_value_t = false)]
  all_bi_tasks: bool,

  /// Number of measurement runs.
  #[arg(short, long, default_value_t = 10)]
  runs: usize,

  /// Number of warm-up runs (not measured).
  #[arg(short, long, default_value_t = 1)]
  warmup: usize,

  /// Output file path for results (JSON format). If not specified, prints to stdout.
  #[arg(short, long)]
  output: Option<PathBuf>,

  /// Cache size for the storage adapter.
  #[arg(long, default_value_t = 512)]
  cache_size: usize,
}

#[derive(Serialize, Debug, Clone)]
struct TimingStats {
  min_ms: f64,
  max_ms: f64,
  avg_ms: f64,
  all_runs_ms: Vec<f64>,
}

#[derive(Serialize, Debug, Clone)]
struct ResourceUsage {
  timestamp_ms: u128,
  cpu_usage_percent: f32,
  memory_bytes: u64,
}

#[derive(Serialize, Debug)]
struct PerTaskBenchmarkOutput {
  query_file: String,
  storage_type: String,
  cache_size: usize,
  num_runs: usize,
  num_warmup: usize,
  timing: TimingStats,
}

#[derive(Serialize, Debug)]
struct SingleRunBenchmarkOutput {
  query_file: String,
  storage_type: String,
  cache_size: usize,
  num_runs: usize,
  num_warmup: usize,
  timing: TimingStats,
  resource_usage: Vec<ResourceUsage>,
}

#[cfg(unix)]
static SQLITE_TASKS: LazyLock<HashSet<&str>> =
  LazyLock::new(|| HashSet::from(["3", "6", "7", "10", "11"]));

static BENCHMARK_OUTPUT_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
  let res = project_root::get_project_root()
    .unwrap()
    .join("resources")
    .join("out")
    .join("benchmarks");
  if !res.exists() {
    fs::create_dir_all(&res).unwrap();
  }
  res
});

fn main() -> io::Result<()> {
  parallel::config_before_run(run_benchmark())
}

async fn run_benchmark() -> io::Result<()> {
  dotenv().ok();
  let args = Args::parse();

  // --- Argument Validation ---
  if args.all_bi_tasks {
    if args.query_file.is_some() || args.storage.is_some() {
      eprintln!(
        "{} Cannot use {} or {} with {}.",
        "ERROR:".red(),
        "--query-file".yellow(),
        "--storage".yellow(),
        "--all-bi-tasks".cyan()
      );
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "Conflicting arguments",
      ));
    }
    if args.output.is_some() {
      eprintln!(
        "{} Cannot use {} with {}. Output files will be generated in {}",
        "ERROR:".red(),
        "--output".yellow(),
        "--all-bi-tasks".cyan(),
        BENCHMARK_OUTPUT_DIR.display().to_string().purple()
      );
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "Conflicting arguments",
      ));
    }
  } else if args.query_file.is_none() || args.storage.is_none() {
    eprintln!(
      "{} Must provide {} and {} unless {} is used.",
      "ERROR:".red(),
      "--query-file".yellow(),
      "--storage".yellow(),
      "--all-bi-tasks".cyan()
    );
    return Err(io::Error::new(
      io::ErrorKind::InvalidInput,
      "Missing arguments",
    ));
  }
  // --- End Argument Validation ---

  println!(
    "{} Running benchmark for {} with {} storage (cache size: {})",
    "INFO:".cyan(),
    if args.all_bi_tasks {
      "all BI tasks".cyan()
    } else {
      args
        .query_file
        .as_ref()
        .unwrap()
        .display()
        .to_string()
        .cyan()
    },
    if args.all_bi_tasks {
      "determined storage".cyan()
    } else {
      args.storage.as_ref().unwrap().cyan()
    },
    args.cache_size.to_string().yellow()
  );
  println!(
    "{} Runs: {}, Warm-up: {}",
    "INFO:".cyan(),
    args.runs.to_string().yellow(),
    args.warmup.to_string().yellow()
  );

  // --- Resource Monitoring Setup ---
  let (resource_tx, resource_rx) = mpsc::channel();
  let monitoring_active = Arc::new(AtomicBool::new(false));
  let monitoring_active_clone = monitoring_active.clone();
  let pid = process::id();

  let resource_thread = thread::spawn(move || {
    let mut sys = System::new_with_specifics(
      RefreshKind::new()
        .with_memory(MemoryRefreshKind::everything())
        .with_cpu(CpuRefreshKind::everything())
        .with_processes(ProcessRefreshKind::everything()),
    );
    let mut usage_data = vec![];

    while monitoring_active_clone.load(Ordering::Relaxed) {
      sys.refresh_specifics(
        RefreshKind::new()
          .with_memory(MemoryRefreshKind::everything())
          .with_cpu(CpuRefreshKind::everything())
          .with_processes(ProcessRefreshKind::everything()),
      );

      if let Some(process) = sys.process((pid as usize).into()) {
        let timestamp_ms = SystemTime::now()
          .duration_since(SystemTime::UNIX_EPOCH)
          .unwrap_or_default()
          .as_millis();
        usage_data.push(ResourceUsage {
          timestamp_ms,
          cpu_usage_percent: process.cpu_usage(),
          memory_bytes: process.memory(),
        });
      }
      thread::sleep(Duration::from_millis(100)); // Sample every 100ms
    }
    resource_tx.send(usage_data).ok(); // Send data back when stopping
  });
  // --- End Resource Monitoring Setup ---

  monitoring_active.store(true, Ordering::Relaxed);

  if args.all_bi_tasks {
    // Find and sort plan files
    let plan_dir = project_root::get_project_root()
      .map_err(io::Error::other)?
      .join("resources")
      .join("plan");

    let mut plan_files: Vec<PathBuf> = Vec::new();
    for entry in fs::read_dir(&plan_dir).map_err(io::Error::other)? {
      let entry = entry.map_err(io::Error::other)?;
      let path = entry.path();
      if path.is_file() {
        if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
          if name.starts_with("ldbc-bi-") && name.ends_with(".json") {
            plan_files.push(path);
          }
        }
      }
    }

    // Natural sort
    plan_files.sort_by(|a, b| {
      let num_a = a
        .file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.trim_start_matches("ldbc-bi-"))
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(u32::MAX);
      let num_b = b
        .file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.trim_start_matches("ldbc-bi-"))
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(u32::MAX);
      num_a.cmp(&num_b)
    });

    println!(
      "{} Found {} BI task plan files.",
      "INFO:".cyan(),
      plan_files.len().to_string().yellow()
    );

    for plan_file in plan_files {
      // Extract task number from filename
      let task_num_str = plan_file
        .file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.trim_start_matches("ldbc-bi-"))
        .unwrap_or(""); // Default to empty if parsing fails

      println!(
        "{} Running task: {} (Task Num: {})", // Log extracted task number
        "--->".blue(),
        plan_file.display().to_string().purple(),
        task_num_str.yellow()
      );

      // Determine storage type: Always sqlite based on example analysis
      #[cfg(unix)]
      let storage_type = if SQLITE_TASKS.contains(&task_num_str) {
        "sqlite"
      } else {
        "neo4j"
      };
      #[cfg(windows)]
      let storage_type = "sqlite";
      println!("{} Using storage: {}", "--->".blue(), storage_type.cyan());

      match execute_single_benchmark(&plan_file, storage_type, &args).await {
        Ok(timing_stats) => {
          let output_data = PerTaskBenchmarkOutput {
            query_file: plan_file.display().to_string(),
            storage_type: storage_type.to_string(),
            cache_size: args.cache_size,
            num_runs: args.runs,
            num_warmup: args.warmup,
            timing: timing_stats,
          };

          let output_filename = format!("bi_{}.json", task_num_str);
          let output_path = BENCHMARK_OUTPUT_DIR.join(output_filename);
          let output_json = serde_json::to_string_pretty(&output_data).map_err(io::Error::other)?;

          fs::write(&output_path, output_json)?;
          println!(
            "{} Results saved to: {}",
            "--->".blue(),
            output_path.display().to_string().green()
          );
        }
        Err(e) => eprintln!(
          "{} Failed to execute benchmark for {}: {}",
          "ERROR:".red(),
          plan_file.display().to_string().purple(),
          e
        ),
      }
    }
  } else {
    // Execute single task
    let query_file = args.query_file.as_ref().unwrap();
    let storage_type = args.storage.as_ref().unwrap().as_str();
    match execute_single_benchmark(query_file, storage_type, &args).await {
      Ok(timing_stats) => {
        // Collect resource usage data only AFTER single task execution
        monitoring_active.store(false, Ordering::Relaxed); // Stop monitoring here for single run
        let resource_usage = match resource_thread.join() {
          Ok(_) => {
            // Thread finished without panic, try receiving data
            resource_rx.recv().unwrap_or_else(|e| {
              eprintln!(
                "{} Failed to receive resource data from thread: {}",
                "ERROR:".red(),
                e
              );
              vec![] // Return empty vec on channel error
            })
          }
          Err(_) => {
            eprintln!("{} Resource monitoring thread panicked.", "ERROR:".red());
            vec![] // Return empty vec on panic
          }
        };

        let final_output = SingleRunBenchmarkOutput {
          query_file: query_file.display().to_string(),
          storage_type: storage_type.to_string(),
          cache_size: args.cache_size,
          num_runs: args.runs,
          num_warmup: args.warmup,
          timing: timing_stats,
          resource_usage,
        };

        let output_json = serde_json::to_string_pretty(&final_output).map_err(io::Error::other)?;

        if let Some(output_path) = args.output {
          fs::write(&output_path, output_json)?;
          println!(
            "{} Benchmark results saved to '{}'",
            "INFO:".cyan(),
            output_path.display()
          );
        } else {
          println!("\n--- Benchmark Results ---");
          println!("{}", output_json);
        }
      }
      Err(e) => {
        // Stop monitoring even on error for single run
        monitoring_active.store(false, Ordering::Relaxed);
        // Ensure thread is joined even on error to avoid dangling threads
        let _ = resource_thread.join();
        eprintln!(
          "{} Failed to execute benchmark for {}: {}",
          "ERROR:".red(),
          query_file.display().to_string().purple(),
          e
        );
      }
    }
    // Early exit after single task processing
    return Ok(());
  }

  // If --all-bi-tasks was used, stop monitoring and join thread here
  monitoring_active.store(false, Ordering::Relaxed);
  let _ = resource_thread.join(); // Join thread, ignore result for multi-task run

  println!("{} All benchmark tasks finished.", "INFO:".cyan());

  Ok(())
}

// --- Refactored Benchmark Execution Function ---
async fn execute_single_benchmark(
  query_file: &PathBuf,
  storage_type: &str,
  args: &Args,
) -> io::Result<TimingStats> {
  // Load query plan
  let plan_json = fs::read_to_string(query_file)?; // Correctly use the reference
  let plan: PlanData =
    serde_json::from_str(&plan_json).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
  let plan_arc = Arc::new(plan);

  let mut durations_ms = Vec::with_capacity(args.runs);

  // This match is now inside the function and only handles executor creation
  // It doesn't return different types anymore
  match storage_type.to_lowercase().as_str() {
    "neo4j" => {
      // Create adapter first
      let neo4j_adapter = Neo4jStorageAdapter::async_default().await;
      let cached_adapter =
        CachedStorageAdapter::<Neo4jStorageAdapter>::new(neo4j_adapter, args.cache_size);
      let adapter = Arc::new(cached_adapter);
      let mut executor = ExecEngine::new(plan_arc.clone(), adapter.clone());

      // Warm-up runs
      if args.warmup > 0 {
        for _ in 0..args.warmup {
          adapter.cache_clear().await;
          let _ = executor.exec().await;
        }
      }
      // Measurement runs
      for _ in 0..args.runs {
        adapter.cache_clear().await;
        let start_time = Instant::now();
        executor.exec().await;
        let duration = start_time.elapsed();
        durations_ms.push(duration.as_secs_f64() * 1000.0);
      }
    }
    "sqlite" => {
      // Create adapter first
      let sqlite_adapter = SqliteStorageAdapter::async_default().await;
      let cached_adapter =
        CachedStorageAdapter::<SqliteStorageAdapter>::new(sqlite_adapter, args.cache_size);
      let adapter = Arc::new(cached_adapter);
      let mut executor = ExecEngine::new(plan_arc.clone(), adapter.clone());

      // Warm-up runs
      if args.warmup > 0 {
        for _ in 0..args.warmup {
          adapter.cache_clear().await;
          let _ = executor.exec().await;
        }
      }
      // Measurement runs
      for _ in 0..args.runs {
        adapter.cache_clear().await;
        let start_time = Instant::now();
        executor.exec().await;
        let duration = start_time.elapsed();
        durations_ms.push(duration.as_secs_f64() * 1000.0);
      }
    }
    _ => {
      // This case should ideally be caught by earlier validation
      // but we return an error just in case.
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("Invalid storage type encountered: {}", storage_type),
      ));
    }
  };

  // Calculate timing statistics
  let timing_stats = if durations_ms.len() > 2 {
    let mut sorted_durations = durations_ms.clone();
    sorted_durations.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let sliced = &sorted_durations[1..sorted_durations.len() - 1];
    let sum: f64 = sliced.iter().sum();
    let avg = sum / sliced.len() as f64;
    TimingStats {
      min_ms: *sliced.first().unwrap_or(&0.0),
      max_ms: *sliced.last().unwrap_or(&0.0),
      avg_ms: avg,
      all_runs_ms: durations_ms,
    }
  } else if !durations_ms.is_empty() {
    let sum: f64 = durations_ms.iter().sum();
    let avg = sum / durations_ms.len() as f64;
    TimingStats {
      min_ms: *durations_ms
        .iter()
        .min_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap_or(&0.0),
      max_ms: *durations_ms
        .iter()
        .max_by(|a, b| a.partial_cmp(b).unwrap())
        .unwrap_or(&0.0),
      avg_ms: avg,
      all_runs_ms: durations_ms,
    }
  } else {
    TimingStats {
      min_ms: 0.0,
      max_ms: 0.0,
      avg_ms: 0.0,
      all_runs_ms: durations_ms,
    }
  };

  Ok(timing_stats)
}
