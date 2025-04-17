use dotenv::dotenv;
use ember_graph::{demos::bi_sf01_neo4j_ordered::bi_6_on_sf_01, utils::parallel};
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

fn main() -> io::Result<()> {
  parallel::config_before_run(to_run())
}

async fn to_run() -> io::Result<()> {
  dotenv().ok();
  bi_6_on_sf_01().await
}
