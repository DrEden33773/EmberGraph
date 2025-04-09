#![feature(duration_millis_float)]

pub mod demos;
pub mod executor;
pub mod matching_ctx;
pub mod parser;
pub mod planner;
pub mod result_dump;
pub mod schemas;
pub mod storage;
pub mod utils;

#[cfg(feature = "use_tracing")]
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
