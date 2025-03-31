use dotenv::dotenv;
#[allow(unused_imports)]
use ember_sgm_backend::demos::{complex_interactive_sf01::*, simple_interactive_sf01::*};
use project_root::get_project_root;
use tokio::io::{self};
use tracing::info;
use tracing_appender::{non_blocking, rolling};
use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt, util::SubscriberInitExt};

async fn demo() -> io::Result<()> {
  ic_11_on_sf_01().await
}

#[tokio::main(flavor = "multi_thread", worker_threads = 20)]
async fn main() -> io::Result<()> {
  dotenv().ok();

  let mut path = get_project_root()?;
  path.push("logs");

  let file_appender = rolling::never(path, "application.log");
  let (non_blocking, _guard) = non_blocking(file_appender);

  let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
  let file_layer = tracing_subscriber::fmt::layer()
    .with_ansi(false)
    .with_writer(non_blocking)
    .with_timer(tracing_subscriber::fmt::time::uptime())
    .with_thread_ids(true)
    .with_thread_names(true);

  Registry::default().with(env_filter).with(file_layer).init();

  info!("Application started.\n");
  println!("Application started.\n");

  demo().await?;

  info!("Application finished.\n");
  println!("Application started.\n");

  Ok(())
}
