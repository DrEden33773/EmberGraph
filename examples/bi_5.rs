use dotenv::dotenv;
use ember_graph::{demos::bi_sf01::query, storage::SqliteStorageAdapter, utils::parallel};
use tokio::io;

#[cfg(unix)]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(windows)]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> io::Result<()> {
  parallel::config_before_run(to_run())
}

async fn to_run() -> io::Result<()> {
  dotenv().ok();
  println!("Querying 'BI-5' on 'SF0.1' ...\n");
  query::<SqliteStorageAdapter>("ldbc-bi-5.json").await
}
