#![feature(duration_millis_float)]

use crate::planner::generate_optimal_plan;
use colored::Colorize;
use hashbrown::HashMap;
use planner::generate_plan_with_given_order;
use std::{path::PathBuf, sync::LazyLock};
use tokio::io;

pub mod demos;
pub mod executor;
pub mod matching_ctx;
pub mod parser;
pub mod planner;
pub mod result_dump;
pub mod schemas;
pub mod storage;
pub mod utils;

pub use executor::ExecEngine;
pub use parser::PatternParser;
pub use schemas::PlanData;

#[cfg(feature = "enable_log")]
pub mod init_log {
  use project_root::get_project_root;
  use tokio::io::{self};
  use tracing_appender::non_blocking::WorkerGuard;
  use tracing_appender::{non_blocking, rolling};
  use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt, util::SubscriberInitExt};

  pub async fn init_log() -> io::Result<WorkerGuard> {
    let mut path = get_project_root()?;
    path.push("logs");

    let file_appender = rolling::never(path, "application.log");
    let (non_blocking, guard) = non_blocking(file_appender);

    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let file_layer = tracing_subscriber::fmt::layer()
      .with_ansi(false)
      .with_writer(non_blocking)
      .with_timer(tracing_subscriber::fmt::time::uptime())
      .with_thread_names(true);

    Registry::default().with(env_filter).with(file_layer).init();

    Ok(guard)
  }
}

static QUERIES: LazyLock<PathBuf> = LazyLock::new(|| {
  let res = project_root::get_project_root()
    .unwrap()
    .join("resources")
    .join("queries");

  if !res.exists() {
    std::fs::create_dir_all(&res).unwrap();
    println!(
      "⚠️  Directory '{}' does not exist, created it.",
      res.to_str().unwrap().yellow()
    );
  }

  res
});

#[cfg(not(feature = "no_optimizations"))]
static PLANS: LazyLock<PathBuf> = LazyLock::new(|| {
  let res = project_root::get_project_root()
    .unwrap()
    .join("resources")
    .join("plan");

  if !res.exists() {
    std::fs::create_dir_all(&res).unwrap();
    println!(
      "⚠️  Directory '{}' does not exist, created it.",
      res.to_str().unwrap().yellow()
    );
  }

  res
});

#[cfg(feature = "no_optimizations")]
static PLANS: LazyLock<PathBuf> = LazyLock::new(|| {
  let res = project_root::get_project_root()
    .unwrap()
    .join("resources")
    .join("plan")
    .join("unoptimized");

  if !res.exists() {
    std::fs::create_dir_all(&res).unwrap();
    println!(
      "⚠️  Directory '{}' does not exist, created it.",
      res.to_str().unwrap().yellow()
    );
  }

  res
});

pub async fn plan_gen_with_given_orders() -> io::Result<()> {
  let queries = QUERIES.clone();
  let plans = PLANS.clone().join("neo4j_ordered");
  if !plans.exists() {
    std::fs::create_dir_all(&plans).unwrap();
    println!(
      "⚠️  Directory '{}' does not exist, created it.",
      plans.to_str().unwrap().yellow()
    );
  }

  let query_2_given_order: HashMap<&str, Vec<&str>> = HashMap::from_iter([
    (
      "ldbc-bi-3",
      vec![
        "country", "city", "person", "forum", "post", "comment", "tag", "tagClass",
      ],
    ),
    (
      "ldbc-bi-5",
      vec!["liker", "post", "tag", "comment", "person"],
    ),
    (
      "ldbc-bi-6",
      vec!["person2", "post1", "tag", "person1", "post2", "person3"],
    ),
    ("ldbc-bi-7", vec!["comment", "post", "tag", "relatedTag"]),
    (
      "ldbc-bi-11",
      vec!["country", "city_c", "c", "b", "city_b", "a", "city_a"],
    ),
    (
      "ldbc-bi-17",
      vec![
        "forum1", "person2", "comment", "person3", "message2", "post2", "tag", "message1",
        "person1", "post1", "forum2",
      ],
    ),
  ]);

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
    if path.extension().unwrap_or_default() != "txt" {
      continue;
    }

    let filename = path.file_stem().unwrap().to_str().unwrap().to_string();
    if !query_2_given_order.contains_key(filename.as_str()) {
      continue;
    }

    let plans = plans.clone();
    let given_order = query_2_given_order[filename.as_str()].clone();

    println!(
      "🪄  Generating plan for query '{}' with given order {}",
      path.to_str().unwrap().green(),
      format!("{given_order:?}").yellow()
    );

    let handle = tokio::spawn(async move {
      let plan_data = generate_plan_with_given_order(&path, &given_order);
      let plan_json = serde_json::to_string_pretty(&plan_data).unwrap();
      let filepath = plans.join(format!("{filename}.json"));

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
      eprintln!("❌  Task failed: {e}");
    }
  }

  println!("✅  All plans generated\n");

  Ok(())
}

pub async fn plan_gen() -> io::Result<()> {
  let queries = QUERIES.clone();
  let plans = PLANS.clone();

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
    if path.extension().unwrap_or_default() != "txt" {
      continue;
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
      let filepath = plans.join(format!("{filename}.json"));

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
      eprintln!("❌  Task failed: {e}");
    }
  }

  println!("✅  All plans generated\n");

  Ok(())
}
