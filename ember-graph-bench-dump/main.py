from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import scienceplots  # type: ignore
from data_fetcher import (
    load_experiment_and_control_group_timing_data,
    load_experiment_and_unoptimized_group_timing_data,
    load_experiment_group_resource_usage_data,
)
from path_utils import PLOTTING_OUTPUT_DIR

print(scienceplots)

# use English labels by default
USE_ENGLISH_LABELS = False

MARKER: Optional[str] = None

plt.style.use(["science", "ieee", "no-latex", "cjk-sc-font"])

print()


def plot_avg_execution_time(
    test_between_experiment_and_control_group: bool = True,
    use_sqlite_backend: bool = True,
):
    """Plot the average execution time comparison."""
    timing_data = (
        load_experiment_and_control_group_timing_data(use_sqlite_backend)
        if test_between_experiment_and_control_group
        else load_experiment_and_unoptimized_group_timing_data(use_sqlite_backend)
    )

    sample_names = sorted(
        list(timing_data.keys()),
        key=lambda x: int(x.split("_")[1]),
    )
    x = np.arange(len(sample_names))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))  # type: ignore

    # avg_ms
    exp_avg_times = [timing_data[name][0].avg_ms for name in sample_names]
    ctrl_or_unoptimized_avg_times = [
        timing_data[name][1].avg_ms for name in sample_names
    ]

    filename = (
        "avg_execution_time"
        if test_between_experiment_and_control_group
        else "avg_execution_time_after_and_before_unoptimized"
    )
    filename += "__sqlite" if use_sqlite_backend else "__neo4j"

    optimized_label = "(Optimized)" if USE_ENGLISH_LABELS else "(有优化)"
    unoptimized_label = "(Unoptimized)" if USE_ENGLISH_LABELS else "(无优化)"

    backend_label = "SQLite" if use_sqlite_backend else "Neo4j"
    backend_label += " Storage" if USE_ENGLISH_LABELS else " 存储"

    ax.bar(  # type: ignore
        x - width / 2,
        exp_avg_times,
        width,
        label=(
            f"EmberGraph[{backend_label}]"
            if test_between_experiment_and_control_group
            else f"EmberGraph[{backend_label}]" + optimized_label
        ),
        color="white",
        edgecolor="black",
        # hatch="///",
    )
    ax.bar(  # type: ignore
        x + width / 2,
        ctrl_or_unoptimized_avg_times,
        width,
        label=(
            "Neo4j"
            if test_between_experiment_and_control_group
            else f"EmberGraph[{backend_label}]" + unoptimized_label
        ),
        color="black",
        edgecolor="black",
        # hatch="...",
    )
    ax.set_title("Average Execution Time" if USE_ENGLISH_LABELS else "平均执行时间")  # type: ignore
    ax.set_ylabel("Time (ms)" if USE_ENGLISH_LABELS else "时间 (ms)")  # type: ignore
    ax.set_xticks(x)  # type: ignore
    ax.set_xticklabels(sample_names)  # type: ignore
    ax.legend()  # type: ignore

    plt.tight_layout()
    plt.savefig(PLOTTING_OUTPUT_DIR / f"{filename}.png", dpi=600)  # type: ignore
    plt.close(fig)


def plot_cpu_usage(
    use_sqlite_backend: bool = True,
):
    """Plot the CPU usage over time."""
    resource_data = load_experiment_group_resource_usage_data(use_sqlite_backend)

    timestamps = sorted(resource_data.keys())
    cpu_usage = [resource_data[ts].cpu_usage_percent for ts in timestamps]

    fig, ax = plt.subplots(figsize=(10, 6))  # type: ignore

    # cpu_usage
    ax.plot(timestamps, cpu_usage, marker=MARKER, linestyle="-")  # type: ignore
    ax.set_title(  # type: ignore
        "CPU Usage over Time" if USE_ENGLISH_LABELS else "CPU 使用率随时间的变化"
    )
    ax.set_xlabel("Time (ms)" if USE_ENGLISH_LABELS else "时间 (ms)")  # type: ignore
    ax.set_ylabel("CPU Usage (%)" if USE_ENGLISH_LABELS else "CPU 使用率 (%)")  # type: ignore
    ax.grid(True)  # type: ignore

    filename = "cpu_usage" + ("__sqlite" if use_sqlite_backend else "__neo4j")

    plt.tight_layout()
    plt.savefig(PLOTTING_OUTPUT_DIR / f"{filename}.png", dpi=600)  # type: ignore
    plt.close(fig)


def plot_memory_usage(use_sqlite_backend: bool = True):
    """Plot the memory usage over time."""
    resource_data = load_experiment_group_resource_usage_data(use_sqlite_backend)

    timestamps = sorted(resource_data.keys())
    memory_usage = [resource_data[ts].memory_bytes for ts in timestamps]

    fig, ax = plt.subplots(figsize=(10, 6))  # type: ignore

    # memory_usage
    # convert memory_usage from bytes to MB
    memory_usage_mb = [mem / (1024 * 1024) for mem in memory_usage]
    ax.plot(timestamps, memory_usage_mb, marker=MARKER, linestyle="-")  # type: ignore
    ax.set_title(  # type: ignore
        "Memory Usage over Time" if USE_ENGLISH_LABELS else "内存使用量随时间的变化"
    )
    ax.set_xlabel("Time (ms)" if USE_ENGLISH_LABELS else "时间 (ms)")  # type: ignore
    ax.set_ylabel("Memory Usage (MB)" if USE_ENGLISH_LABELS else "内存使用量 (MB)")  # type: ignore
    ax.grid(True)  # type: ignore

    filename = "memory_usage" + ("__sqlite" if use_sqlite_backend else "__neo4j")

    plt.tight_layout()
    plt.savefig(PLOTTING_OUTPUT_DIR / f"{filename}.png", dpi=600)  # type: ignore
    plt.close(fig)


if __name__ == "__main__":
    print("Plotting...")

    plot_avg_execution_time(
        test_between_experiment_and_control_group=True, use_sqlite_backend=True
    )
    plot_avg_execution_time(
        test_between_experiment_and_control_group=True, use_sqlite_backend=False
    )

    plot_avg_execution_time(test_between_experiment_and_control_group=False)

    plot_cpu_usage(use_sqlite_backend=True)
    plot_cpu_usage(use_sqlite_backend=False)

    plot_memory_usage(use_sqlite_backend=True)
    plot_memory_usage(use_sqlite_backend=False)

    print("Plots saved to: ", PLOTTING_OUTPUT_DIR)
