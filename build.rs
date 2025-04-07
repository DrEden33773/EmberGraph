use std::env;
use std::process::Command;

fn main() {
  println!("cargo:rerun-if-changed=ember-graph-import/pyproject.toml");

  let is_windows = cfg!(windows);

  if is_windows {
    windows_setup();
  } else {
    unix_setup();
  }
}

fn windows_setup() {
  println!("cargo:warning=🧩  Running on Windows platform");

  // check if the `uv` package manager is installed
  let uv_exists = Command::new("powershell")
    .arg("-Command")
    .arg("Get-Command uv -ErrorAction SilentlyContinue")
    .status()
    .map(|status| status.success())
    .unwrap_or(false);

  if !uv_exists {
    println!("cargo:warning=🌐  Installing uv package manager on Windows...");

    // install the `uv` package manager (via Powershell)
    let install_status = Command::new("powershell")
      .arg("-ExecutionPolicy")
      .arg("ByPass")
      .arg("-Command")
      .arg("irm https://astral.sh/uv/install.ps1 | iex")
      .status()
      .expect("❌  Failed to install uv on Windows");

    if !install_status.success() {
      panic!("❌  Failed to install uv package manager on Windows");
    }

    println!("cargo:warning=☑️   uv package manager installed successfully on Windows");
  } else {
    println!(
      "cargo:warning=☑️   uv package manager already installed on Windows, skipping installation"
    );
  }

  // sync Python dependencies
  println!("cargo:warning=🌐  Syncing Python dependencies on Windows...");

  // Windows requires specific handling for paths
  let current_dir = env::current_dir().expect("❌  Failed to get current directory");
  let ember_dir = current_dir.join("ember-graph-import");

  let sync_status = Command::new("powershell")
    .arg("-Command")
    .arg(format!(
      "cd '{}'; uv sync; cd '{}'",
      ember_dir.display(),
      current_dir.display()
    ))
    .status()
    .expect("❌  Failed to sync dependencies on Windows");

  if !sync_status.success() {
    panic!("❌  Failed to sync Python dependencies on Windows");
  }

  println!("cargo:warning=☑️   Python dependencies synced successfully on Windows");
}

fn unix_setup() {
  println!("cargo:warning=🧩  Running on Unix-like platform");

  // check if the `uv` package manager is installed
  let uv_exists = Command::new("sh")
    .arg("-c")
    .arg("command -v uv")
    .status()
    .map(|status| status.success())
    .unwrap_or(false);

  if !uv_exists {
    println!("cargo:warning=🌐  Installing uv package manager on Unix...");

    // install the `uv` package manager (via sh)
    let install_status = Command::new("sh")
      .arg("-c")
      .arg("curl -LsSf https://astral.sh/uv/install.sh | sh")
      .status()
      .expect("❌  Failed to install uv on Unix");

    if !install_status.success() {
      panic!("❌  Failed to install uv package manager on Unix");
    }

    println!("cargo:warning=☑️   uv package manager installed successfully on Unix");
  } else {
    println!(
      "cargo:warning=☑️   uv package manager already installed on Unix, skipping installation"
    );
  }

  // sync Python dependencies
  println!("cargo:warning=🌐  Syncing Python dependencies on Unix...");
  let sync_status = Command::new("sh")
    .arg("-c")
    .arg("cd ember-graph-import && uv sync && cd ..")
    .status()
    .expect("❌  Failed to sync dependencies on Unix");

  if !sync_status.success() {
    panic!("❌  Failed to sync Python dependencies on Unix");
  }

  println!("cargo:warning=☑️   Python dependencies synced successfully on Unix");
}
