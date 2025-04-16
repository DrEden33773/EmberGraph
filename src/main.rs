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

  // ember_graph::plan_gen().await?;
  ember_graph::plan_gen_with_given_orders().await?;

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
