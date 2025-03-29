use dotenv::dotenv;
#[allow(unused_imports)]
use ember_sgm_backend::demos::{complex_interactive_sf01::*, simple_interactive_sf01::*};
use tokio::io;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
  dotenv().ok();

  is_3_reversed_directed_knows_on_sf_01().await
}
