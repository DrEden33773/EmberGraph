use dotenv::dotenv;
#[allow(unused_imports)]
use ember_sgm_backend::demos::{complex_interactive_sf01::*, simple_interactive_sf01::*};
use tokio::io::{self};

#[tokio::main(flavor = "multi_thread")]
async fn main() -> io::Result<()> {
  dotenv().ok();

  ic_5_on_sf_01().await
}
