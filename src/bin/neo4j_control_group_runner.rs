use clap::Parser;
use colored::Colorize;
use dotenv::dotenv;
use ember_graph::utils::parallel;
use neo4rs::{ConfigBuilder, Graph, query};
use serde::Serialize;
use std::env;
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

  /// Neo4j server pid
  #[arg(long)]
  neo4j_server_pid: usize,
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
  num_runs: usize,
  num_warmup: usize,
  timing: TimingStats,
  resource_usage: Vec<ResourceUsage>,
}

static BENCHMARK_OUTPUT_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
  let mut res = project_root::get_project_root()
    .unwrap()
    .join("resources")
    .join("out")
    .join("benchmarks");

  #[cfg(feature = "benchmark_with_cache_eviction")]
  res.push("control_group");
  #[cfg(not(feature = "benchmark_with_cache_eviction"))]
  res.push("control_group_with_cache");

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

fn main() -> io::Result<()> {
  parallel::config_before_run(run_benchmark())
}

async fn run_benchmark() -> io::Result<()> {
  dotenv().ok();

  let args = Args::parse();
  arg_validation(&args)?;

  let neo4j_db = init_neo4j_db()
    .await
    .expect("❌  Failed to connect to Neo4j database");

  println!(
    "{} Running benchmark for {} ({} on {} db)",
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
    "control group".yellow(),
    "neo4j".purple()
  );

  println!(
    "{} Runs: {}, Warm-up: {}",
    "INFO:".cyan(),
    args.runs.to_string().yellow(),
    args.warmup.to_string().yellow()
  );

  if args.all_bi_tasks {
    let query_dir = project_root::get_project_root()
      .map_err(io::Error::other)?
      .join("resources")
      .join("cypher");

    let mut query_paths: Vec<PathBuf> = vec![];

    for entry in fs::read_dir(&query_dir).map_err(io::Error::other)? {
      let entry = entry.map_err(io::Error::other)?;
      let path = entry.path();
      if path.is_file()
        && let Some(name) = path.file_name().and_then(|n| n.to_str())
      {
        if !name.starts_with("bi-") || !name.ends_with("cypher") {
          continue;
        }
        query_paths.push(path);
      }
    }

    // Natural sort
    query_paths.sort_by(|a, b| {
      let num_a = a
        .file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.trim_start_matches("bi-"))
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(u32::MAX);
      let num_b = b
        .file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.trim_start_matches("bi-"))
        .and_then(|s| s.parse::<u32>().ok())
        .unwrap_or(u32::MAX);
      num_a.cmp(&num_b)
    });

    println!(
      "{} Found {} BI task query files.",
      "INFO:".cyan(),
      query_paths.len().to_string().yellow()
    );

    for query_file in query_paths {
      // Extract task number from filename
      let task_num_str = query_file
        .file_stem()
        .and_then(|s| s.to_str())
        .map(|s| s.trim_start_matches("bi-"))
        .unwrap_or(""); // Default to empty if parsing fails

      println!(
        "{} Running task: {} (Task Num: {})",
        "--->".purple(),
        query_file.display().to_string().purple(),
        task_num_str.yellow()
      );

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
            let neo4j_server_cpu_usage = {
              let neo4j_process = sys.process(args.neo4j_server_pid.into());
              neo4j_process.map(|p| p.cpu_usage()).unwrap_or(0.0)
            };
            let cpu_usage = process.cpu_usage() + neo4j_server_cpu_usage;
            task_usage_data.push(ResourceUsage {
              timestamp_ms,
              cpu_usage_percent: cpu_usage / sys.cpus().len() as f32,
              memory_bytes: process.memory(),
            });
          }
        }
        task_resource_tx.send(task_usage_data).ok();
      });

      match execute_single(&query_file, &args, &neo4j_db).await {
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
            query_file: query_file.display().to_string(),
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
          query_file.display().to_string().purple(),
          e
        ),
      }
    }
  } else {
    // Execute single task

    let query_file = args.query_file.as_ref().unwrap();

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
          let neo4j_server_cpu_usage = {
            let neo4j_process = sys.process(args.neo4j_server_pid.into());
            neo4j_process.map(|p| p.cpu_usage()).unwrap_or(0.0)
          };
          let cpu_usage = process.cpu_usage() + neo4j_server_cpu_usage;
          task_usage_data.push(ResourceUsage {
            timestamp_ms,
            cpu_usage_percent: cpu_usage / sys.cpus().len() as f32,
            memory_bytes: process.memory(),
          });
        }
      }
      task_resource_tx.send(task_usage_data).ok();
    });

    match execute_single(query_file, &args, &neo4j_db).await {
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
          println!("{output_json}");
        }
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

  println!(
    "{} All benchmark tasks {} finished.",
    "INFO:".cyan(),
    "(control group)".yellow()
  );

  Ok(())
}

fn arg_validation(args: &Args) -> io::Result<()> {
  if args.all_bi_tasks {
    if args.query_file.is_some() {
      eprintln!(
        "{} Cannot use {} with {}.",
        "ERROR:".red(),
        "--query-file".yellow(),
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
  } else if args.query_file.is_none() {
    eprintln!(
      "{} Must provide {} unless {} is used.",
      "ERROR:".red(),
      "--query-file".yellow(),
      "--all-bi-tasks".cyan()
    );
    return Err(io::Error::new(
      io::ErrorKind::InvalidInput,
      "Missing arguments",
    ));
  }

  Ok(())
}

async fn init_neo4j_db() -> Result<Graph, neo4rs::Error> {
  let uri = env::var("NEO4J_URI").unwrap();
  let username = env::var("NEO4J_USERNAME").unwrap();
  let password = env::var("NEO4J_PASSWORD").unwrap();
  let db_name = env::var("NEO4J_DATABASE").unwrap();
  let config = ConfigBuilder::default()
    .uri(uri)
    .user(username)
    .password(password)
    .db(db_name)
    .fetch_size(1000)
    .max_connections(num_cpus::get() * 2)
    .build()
    .unwrap();

  Graph::connect(config).await
}

async fn execute_single(cypher_file: &PathBuf, args: &Args, db: &Graph) -> io::Result<TimingStats> {
  let cypher_stmt = fs::read_to_string(cypher_file).unwrap();
  let mut durations_ms = Vec::with_capacity(args.runs);

  // Warm-up runs
  if args.warmup > 0 {
    for _ in 0..args.warmup {
      db.run(query(&cypher_stmt)).await.unwrap();
    }
  }

  // Measurement runs
  for _ in 0..args.runs {
    let start_time = Instant::now();
    db.run(query(&cypher_stmt)).await.unwrap();
    let duration = start_time.elapsed();
    durations_ms.push(duration.as_secs_f64() * 1000.0);
  }

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
