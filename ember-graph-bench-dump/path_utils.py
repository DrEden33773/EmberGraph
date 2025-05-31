from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.absolute()
PROJECT_OUTPUT_DIR = PROJECT_ROOT / "out"
PROJECT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ROOT = Path(__file__).parent.parent.absolute()

BENCHMARK_OUTPUT_DIR = ROOT / "resources" / "out" / "benchmarks"
UNOPTIMIZED_BENCHMARK_OUTPUT_DIR = BENCHMARK_OUTPUT_DIR / "unoptimized"
CONTROL_GROUP_BENCH_OUTPUT_DIR = BENCHMARK_OUTPUT_DIR / "control_group"
BENCHMARK_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
UNOPTIMIZED_BENCHMARK_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CONTROL_GROUP_BENCH_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PLOTTING_OUTPUT_DIR = ROOT / "resources" / "out" / "plotting"
PLOTTING_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def exp_group_bench_output_dir(use_sqlite_backend: bool = True) -> Path:
    if use_sqlite_backend:
        res = BENCHMARK_OUTPUT_DIR / "sqlite"
        res.mkdir(parents=True, exist_ok=True)
        return res
    else:
        res = BENCHMARK_OUTPUT_DIR / "neo4j"
        res.mkdir(parents=True, exist_ok=True)
        return BENCHMARK_OUTPUT_DIR / "neo4j"


def unoptimized_group_bench_output_dir(use_sqlite_backend: bool = True) -> Path:
    if use_sqlite_backend:
        res = UNOPTIMIZED_BENCHMARK_OUTPUT_DIR / "sqlite"
        res.mkdir(parents=True, exist_ok=True)
        return res
    else:
        res = UNOPTIMIZED_BENCHMARK_OUTPUT_DIR / "neo4j"
        res.mkdir(parents=True, exist_ok=True)
        return res
