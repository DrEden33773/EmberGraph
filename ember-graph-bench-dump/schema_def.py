from dataclasses import dataclass


@dataclass
class TimingStats:
    min_ms: float
    max_ms: float
    avg_ms: float
    all_runs_ms: list[float]


@dataclass
class ResourceUsage:
    # timestamp_ms: int
    cpu_usage_percent: float
    memory_bytes: int


@dataclass
class BenchmarkOutput:
    query_file: str
    storage_type: str
    cache_size: int
    num_runs: int
    num_warmup: int
    timing: TimingStats
    resource_usage: list[ResourceUsage]
