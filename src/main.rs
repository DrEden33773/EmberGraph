use dotenv::dotenv;
#[allow(unused_imports)]
use ember_sgm_backend::demos::{complex_interactive_sf01::*, simple_interactive_sf01::*};
use tokio::io::{self};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
  dotenv().ok();

  #[cfg(feature = "use_tracing")]
  let _guard = init_log::init_log().await?;

  ic_11_on_sf_01().await?;

  Ok(())
}

#[cfg(feature = "use_tracing")]
pub(crate) mod init_log {
  use project_root::get_project_root;
  use tokio::io::{self};
  use tracing_appender::non_blocking::WorkerGuard;
  use tracing_appender::{non_blocking, rolling};
  use tracing_subscriber::{EnvFilter, Registry, layer::SubscriberExt, util::SubscriberInitExt};

  #[allow(dead_code)]
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
      .with_thread_ids(true)
      .with_thread_names(true);

    Registry::default().with(env_filter).with(file_layer).init();

    Ok(guard)
  }
}
