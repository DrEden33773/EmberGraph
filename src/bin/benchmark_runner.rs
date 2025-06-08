use clap::Parser;
use colored::Colorize;
use dotenv::dotenv;
#[cfg(not(feature = "benchmark_with_cache_eviction"))]
use ember_graph::storage::CachedStorageAdapter;
use ember_graph::{
  executor::ExecEngine,
  schemas::PlanData,
  storage::{AsyncDefault, Neo4jStorageAdapter, SqliteStorageAdapter},
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
  time::{Instant, SystemTime},
};
use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};
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
struct BenchmarkOutput {
  query_file: String,
  storage_type: String,
  cache_size: usize,
  num_runs: usize,
  num_warmup: usize,
  timing: TimingStats,
  resource_usage: Vec<ResourceUsage>,
}

#[cfg(unix)]
#[cfg(not(feature = "benchmark_via_sqlite_only"))]
#[cfg(not(feature = "benchmark_via_neo4j_only"))]
static SQLITE_TASKS: LazyLock<HashSet<&str>> = LazyLock::new(|| {
  let res = HashSet::from(["3", "5", "6", "7", "10", "11"]);
  println!(
    "{} SQLite tasks: {}",
    "INFO:".cyan(),
    format!("{:?}", res.iter().collect::<Vec<_>>()).cyan()
  );
  res
});

#[cfg(unix)]
#[cfg(feature = "benchmark_via_neo4j_only")]
#[cfg(not(feature = "benchmark_via_sqlite_only"))]
static SQLITE_TASKS: LazyLock<HashSet<&str>> = LazyLock::new(|| {
  println!("{} {} `SQLite task`", "INFO:".cyan(), "NO".red());
  HashSet::new()
});

#[cfg(unix)]
#[cfg(feature = "benchmark_via_sqlite_only")]
static SQLITE_TASKS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
  println!(
    "{} {} task is registered as `SQLite task`",
    "INFO:".cyan(),
    "EACH".green()
  );
  HashSet::from([
    "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17",
    "18", "19", "20",
  ])
});

static NEO4J_ORDERED_TASKS: LazyLock<HashSet<&str>> = LazyLock::new(|| {
  #[cfg(feature = "use_neo4j_ordered_plan")]
  {
    HashSet::from(["3", "5", "11", "17"])
  }
  #[cfg(not(feature = "use_neo4j_ordered_plan"))]
  {
    HashSet::new()
  }
});

#[cfg(not(feature = "no_optimizations"))]
static BASE_BENCHMARK_OUTPUT_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
  project_root::get_project_root()
    .unwrap()
    .join("resources")
    .join("out")
    .join("benchmarks")
});

#[cfg(feature = "no_optimizations")]
static BASE_BENCHMARK_OUTPUT_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
  project_root::get_project_root()
    .unwrap()
    .join("resources")
    .join("out")
    .join("benchmarks")
    .join("unoptimized")
});

static BENCHMARK_OUTPUT_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
  let mut res = BASE_BENCHMARK_OUTPUT_DIR.clone();

  #[cfg(feature = "benchmark_with_cache_eviction")]
  {
    #[cfg(feature = "benchmark_via_sqlite_only")]
    {
      #[cfg(feature = "benchmark_via_neo4j_only")]
      panic!(
        "CANNOT activate {} and {} at the same time",
        "benchmark_with_cache_eviction".yellow(),
        "benchmark_via_neo4j_only".yellow()
      );
      #[cfg(not(feature = "benchmark_via_neo4j_only"))]
      res.push("sqlite");
    }
    #[cfg(not(feature = "benchmark_via_sqlite_only"))]
    {
      #[cfg(feature = "benchmark_via_neo4j_only")]
      res.push("neo4j");
      #[cfg(not(feature = "benchmark_via_neo4j_only"))]
      res.push("mixed");
    }
  }
  #[cfg(not(feature = "benchmark_with_cache_eviction"))]
  {
    #[cfg(feature = "benchmark_via_sqlite_only")]
    {
      #[cfg(feature = "benchmark_via_neo4j_only")]
      panic!(
        "CANNOT activate {} and {} at the same time",
        "benchmark_with_cache_eviction".yellow(),
        "benchmark_via_neo4j_only".yellow()
      );
      #[cfg(not(feature = "benchmark_via_neo4j_only"))]
      res.push("sqlite_with_cache");
    }
    #[cfg(not(feature = "benchmark_via_sqlite_only"))]
    {
      #[cfg(feature = "benchmark_via_neo4j_only")]
      res.push("neo4j_with_cache");
      #[cfg(not(feature = "benchmark_via_neo4j_only"))]
      res.push("mixed_with_cache");
    }
  }

  if !res.exists() {
    fs::create_dir_all(&res).unwrap();
  }

  println!(
    "{} Benchmark output directory: {}",
    "INFO:".cyan(),
    res.display().to_string().cyan()
  );

  res
});

#[cfg(not(feature = "no_optimizations"))]
static PLAN_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
  project_root::get_project_root()
    .unwrap()
    .join("resources")
    .join("plan")
});

#[cfg(feature = "no_optimizations")]
static PLAN_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
  project_root::get_project_root()
    .unwrap()
    .join("resources")
    .join("plan")
    .join("unoptimized")
});

static NEO4J_ORDERED_PLAN_DIR: LazyLock<PathBuf> =
  LazyLock::new(|| PLAN_DIR.clone().join("neo4j_ordered"));

fn main() -> io::Result<()> {
  parallel::config_before_run(run_benchmark())
}

async fn run_benchmark() -> io::Result<()> {
  dotenv().ok();
  let args = Args::parse();

  arg_validation(&args)?;

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

  if args.all_bi_tasks {
    // Find and sort plan files
    let plan_dir = PLAN_DIR.clone();
    let neo4j_ordered_plan_dir = NEO4J_ORDERED_PLAN_DIR.clone();

    let mut plan_paths: Vec<PathBuf> = vec![];

    for entry in fs::read_dir(&plan_dir).map_err(io::Error::other)? {
      let entry = entry.map_err(io::Error::other)?;
      let path = entry.path();
      if path.is_file()
        && let Some(name) = path.file_name().and_then(|n| n.to_str())
      {
        if !name.starts_with("ldbc-bi-") || !name.ends_with(".json") {
          continue;
        }
        let task_num_str = name
          .trim_start_matches("ldbc-bi-")
          .trim_end_matches(".json");
        if NEO4J_ORDERED_TASKS.contains(&task_num_str) {
          let neo4j_path = neo4j_ordered_plan_dir.join(path.file_name().unwrap());
          plan_paths.push(neo4j_path);
        } else {
          plan_paths.push(path);
        }
      }
    }

    // Natural sort
    plan_paths.sort_by(|a, b| {
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
      plan_paths.len().to_string().yellow()
    );

    for plan_file in plan_paths {
      // Extract task number from filename
      let task_num_str = plan_file
        .file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.trim_start_matches("ldbc-bi-"))
        .unwrap_or(""); // Default to empty if parsing fails

      println!(
        "{} Running task: {} (Task Num: {})",
        "--->".purple(),
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
      println!("{} Using storage: {}", "--->".blue(), storage_type.blue());

      // create independent resource monitoring for each task
      let (task_resource_tx, task_resource_rx) = mpsc::channel();
      let task_monitoring_active = Arc::new(AtomicBool::new(true));
      let task_monitoring_clone = task_monitoring_active.clone();
      let task_pid = process::id();

      let task_resource_thread = thread::spawn(move || {
        let mut sys = System::new_all();
        let mut task_usage_data = vec![];

        while task_monitoring_clone.load(Ordering::Relaxed) {
          // sample interval using system's minimum CPU update interval
          thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);

          sys.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::everything(),
          );

          if let Some(process) = sys.process((task_pid as usize).into()) {
            let timestamp_ms = SystemTime::now()
              .duration_since(SystemTime::UNIX_EPOCH)
              .unwrap_or_default()
              .as_millis();
            task_usage_data.push(ResourceUsage {
              timestamp_ms,
              cpu_usage_percent: process.cpu_usage() / sys.cpus().len() as f32,
              memory_bytes: process.memory(),
            });
          }
        }
        task_resource_tx.send(task_usage_data).ok();
      });

      match execute_single_benchmark(&plan_file, storage_type, &args).await {
        Ok(timing_stats) => {
          // stop current task's resource monitoring
          task_monitoring_active.store(false, Ordering::Relaxed);

          // acquire current task's resource usage data
          let resource_usage = match task_resource_thread.join() {
            Ok(_) => task_resource_rx.recv().unwrap_or_else(|e| {
              eprintln!(
                "{} Failed to receive resource data for task {}: {}",
                "ERROR:".red(),
                task_num_str.yellow(),
                e
              );
              vec![]
            }),
            Err(_) => {
              eprintln!(
                "{} Resource monitoring thread for task {} panicked.",
                "ERROR:".red(),
                task_num_str.yellow()
              );
              vec![]
            }
          };

          let output_data = BenchmarkOutput {
            query_file: plan_file.display().to_string(),
            storage_type: storage_type.to_string(),
            cache_size: args.cache_size,
            num_runs: args.runs,
            num_warmup: args.warmup,
            timing: timing_stats,
            resource_usage,
          };

          let output_filename = format!("bi_{task_num_str}.json");
          let output_path = BENCHMARK_OUTPUT_DIR.join(output_filename);
          let output_json = serde_json::to_string_pretty(&output_data).map_err(io::Error::other)?;

          fs::write(&output_path, output_json)?;
          println!(
            "{} Results saved to: {}",
            "--->".green(),
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

    // create resource monitoring for single task
    let (task_resource_tx, task_resource_rx) = mpsc::channel();
    let task_monitoring_active = Arc::new(AtomicBool::new(true));
    let task_monitoring_clone = task_monitoring_active.clone();
    let task_pid = process::id();

    let task_resource_thread = thread::spawn(move || {
      let mut sys = System::new_all();
      let mut task_usage_data = vec![];

      while task_monitoring_clone.load(Ordering::Relaxed) {
        // sample interval using system's minimum CPU update interval
        thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);

        sys.refresh_processes_specifics(
          ProcessesToUpdate::All,
          true,
          ProcessRefreshKind::everything(),
        );

        if let Some(process) = sys.process((task_pid as usize).into()) {
          let timestamp_ms = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
          task_usage_data.push(ResourceUsage {
            timestamp_ms,
            cpu_usage_percent: process.cpu_usage() / sys.cpus().len() as f32,
            memory_bytes: process.memory(),
          });
        }
      }
      task_resource_tx.send(task_usage_data).ok();
    });

    match execute_single_benchmark(query_file, storage_type, &args).await {
      Ok(timing_stats) => {
        // stop resource monitoring and collect data
        task_monitoring_active.store(false, Ordering::Relaxed);

        // acquire resource usage data
        let resource_usage = match task_resource_thread.join() {
          Ok(_) => {
            task_resource_rx.recv().unwrap_or_else(|e| {
              eprintln!(
                "{} Failed to receive resource data from thread: {}",
                "ERROR:".red(),
                e
              );
              vec![] // return empty array when error
            })
          }
          Err(_) => {
            eprintln!("{} Resource monitoring thread panicked.", "ERROR:".red());
            vec![] // return empty array when thread panics
          }
        };

        let final_output = BenchmarkOutput {
          query_file: query_file.display().to_string(),
          storage_type: storage_type.to_string(),
          cache_size: args.cache_size,
          num_runs: args.runs,
          num_warmup: args.warmup,
          timing: timing_stats,
          resource_usage,
        };

        let output_json = serde_json::to_string_pretty(&final_output).map_err(io::Error::other)?;

        // Extract task number from filename
        let task_num_str = query_file
          .file_stem()
          .and_then(|s| s.to_str())
          .map(|s| s.trim_start_matches("ldbc-bi-"))
          .unwrap_or(""); // Default to empty if parsing fails
        let output_filename = format!("bi_{task_num_str}.json");
        let output_path = BENCHMARK_OUTPUT_DIR.join(output_filename);

        fs::write(&output_path, output_json)?;
        println!(
          "{} Benchmark results saved to '{}'",
          "INFO:".cyan(),
          output_path.display()
        );
      }
      Err(e) => {
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

  println!("{} All benchmark tasks finished.", "INFO:".cyan());

  Ok(())
}

fn arg_validation(args: &Args) -> io::Result<()> {
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
      #[cfg(not(feature = "benchmark_with_cache_eviction"))]
      let cached_adapter = CachedStorageAdapter::new(neo4j_adapter, args.cache_size);
      #[cfg(not(feature = "benchmark_with_cache_eviction"))]
      let adapter = Arc::new(cached_adapter);
      #[cfg(feature = "benchmark_with_cache_eviction")]
      let adapter = Arc::new(neo4j_adapter);
      let mut executor = ExecEngine::new(plan_arc, adapter);

      // Warm-up runs
      if args.warmup > 0 {
        for _ in 0..args.warmup {
          executor.exec().await;
        }
      }
      // Measurement runs
      for _ in 0..args.runs {
        let start_time = Instant::now();
        executor.exec().await;
        let duration = start_time.elapsed();
        durations_ms.push(duration.as_secs_f64() * 1000.0);
      }
    }
    "sqlite" => {
      // Create adapter first
      let sqlite_adapter = SqliteStorageAdapter::async_default().await;
      #[cfg(not(feature = "benchmark_with_cache_eviction"))]
      let cached_adapter = CachedStorageAdapter::new(sqlite_adapter, args.cache_size);
      #[cfg(not(feature = "benchmark_with_cache_eviction"))]
      let adapter = Arc::new(cached_adapter);
      #[cfg(feature = "benchmark_with_cache_eviction")]
      let adapter = Arc::new(sqlite_adapter);
      let mut executor = ExecEngine::new(plan_arc, adapter);

      // Warm-up runs
      if args.warmup > 0 {
        for _ in 0..args.warmup {
          executor.exec().await;
        }
      }
      // Measurement runs
      for _ in 0..args.runs {
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
        format!("Invalid storage type encountered: {storage_type}"),
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
