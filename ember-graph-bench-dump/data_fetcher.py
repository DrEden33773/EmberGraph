import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

from path_utils import (
    CONTROL_GROUP_BENCH_OUTPUT_DIR,
    exp_group_bench_output_dir,
    unoptimized_group_bench_output_dir,
)
from schema_def import BenchmarkOutput, ResourceUsage, TimingStats


def benchmark_output_from_dict(data: Dict[str, Any]) -> BenchmarkOutput:
    timing_data = data.get("timing", {})
    timing_stats = TimingStats(
        min_ms=float(timing_data.get("min_ms", 0.0)),
        max_ms=float(timing_data.get("max_ms", 0.0)),
        avg_ms=float(timing_data.get("avg_ms", 0.0)),
        all_runs_ms=[float(x) for x in timing_data.get("all_runs_ms", [])],
    )

    resource_usage_data: List[Dict[str, Any]] = data.get("resource_usage", [])
    resource_usage_list = [
        ResourceUsage(
            # timestamp_ms=int(ru.get("timestamp_ms", 0)),
            cpu_usage_percent=float(ru.get("cpu_usage_percent", 0.0)),
            memory_bytes=int(ru.get("memory_bytes", 0)),
        )
        for ru in resource_usage_data
    ]

    return BenchmarkOutput(
        query_file=str(data.get("query_file", "")),
        storage_type=str(data.get("storage_type", "")),
        cache_size=int(data.get("cache_size", 0)),
        num_runs=int(data.get("num_runs", 0)),
        num_warmup=int(data.get("num_warmup", 0)),
        timing=timing_stats,
        resource_usage=resource_usage_list,
    )


def load_experiment_and_control_group_timing_data(
    use_sqlite_backend: bool = True,
) -> Dict[str, Tuple[TimingStats, TimingStats]]:
    sqlite_bench_data: dict[str, TimingStats] = {}
    control_group_data: dict[str, TimingStats] = {}

    for file in Path(exp_group_bench_output_dir(use_sqlite_backend)).glob("*.json"):
        filename = file.stem
        with open(file, "r") as f:
            json_string = f.read()
            data_dict: Dict[str, Any] = json.loads(json_string)
            benchmark_output = benchmark_output_from_dict(data_dict)
            timing_stats = benchmark_output.timing
            sqlite_bench_data[filename] = timing_stats

    for file in Path(CONTROL_GROUP_BENCH_OUTPUT_DIR).glob("*.json"):
        filename = file.stem
        with open(file, "r") as f:
            json_string = f.read()
            data_dict: Dict[str, Any] = json.loads(json_string)
            benchmark_output = benchmark_output_from_dict(data_dict)
            timing_stats = benchmark_output.timing
            control_group_data[filename] = timing_stats

    result: Dict[str, Tuple[TimingStats, TimingStats]] = {}
    for filename in sqlite_bench_data:
        result[filename] = (sqlite_bench_data[filename], control_group_data[filename])

    return result


def load_experiment_and_unoptimized_group_timing_data(
    use_sqlite_backend: bool = True,
) -> Dict[str, Tuple[TimingStats, TimingStats]]:
    sqlite_bench_data: dict[str, TimingStats] = {}
    control_group_data: dict[str, TimingStats] = {}

    for file in Path(exp_group_bench_output_dir(use_sqlite_backend)).glob("*.json"):
        filename = file.stem
        with open(file, "r") as f:
            json_string = f.read()
            data_dict: Dict[str, Any] = json.loads(json_string)
            benchmark_output = benchmark_output_from_dict(data_dict)
            timing_stats = benchmark_output.timing
            sqlite_bench_data[filename] = timing_stats

    for file in Path(unoptimized_group_bench_output_dir(use_sqlite_backend)).glob(
        "*.json"
    ):
        filename = file.stem
        with open(file, "r") as f:
            json_string = f.read()
            data_dict: Dict[str, Any] = json.loads(json_string)
            benchmark_output = benchmark_output_from_dict(data_dict)
            timing_stats = benchmark_output.timing
            control_group_data[filename] = timing_stats

    result: Dict[str, Tuple[TimingStats, TimingStats]] = {}
    for filename in sqlite_bench_data:
        result[filename] = (sqlite_bench_data[filename], control_group_data[filename])

    return result


def load_experiment_group_resource_usage_data(
    use_sqlite_backend: bool = True,
) -> Dict[int, ResourceUsage]:
    curr_ms = 0
    result: Dict[int, ResourceUsage] = {}

    for file in Path(exp_group_bench_output_dir(use_sqlite_backend)).glob("*.json"):
        with open(file, "r") as f:
            json_string = f.read()
            data_dict: Dict[str, Any] = json.loads(json_string)
            benchmark_output = benchmark_output_from_dict(data_dict)
            resource_usage = benchmark_output.resource_usage
            for ru in resource_usage:
                result[curr_ms] = ru
                curr_ms += 200

    return result
