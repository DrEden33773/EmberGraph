use colored::Colorize;
use dotenv::dotenv;
use ember_graph::{
  demos::bi_sf01_neo4j_ordered::query, storage::SqliteStorageAdapter, utils::parallel,
};
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
  println!(
    "Querying 'BI-6' {} on 'SF0.1' ...\n",
    "with neo4j_matching_order".purple()
  );
  query::<SqliteStorageAdapter>("ldbc-bi-6.json").await
}
