from pathlib import Path

ROOT = Path(__file__).parent.parent.absolute()

BENCHMARK_OUTPUT_DIR = ROOT / "resources" / "out" / "benchmarks"
EXPERIMENT_GROUP_BENCH_OUTPUT_DIR = BENCHMARK_OUTPUT_DIR / "sqlite"
CONTROL_GROUP_BENCH_OUTPUT_DIR = BENCHMARK_OUTPUT_DIR / "control_group"
EXPERIMENT_GROUP_BENCH_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CONTROL_GROUP_BENCH_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PLOTTING_OUTPUT_DIR = ROOT / "resources" / "out" / "plotting"
PLOTTING_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
