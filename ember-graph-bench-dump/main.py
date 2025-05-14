from typing import Optional

import matplotlib.pyplot as plt
import numpy as np
import scienceplots  # type: ignore
from csv_dumper import csv_dumper
from data_fetcher import (
    load_experiment_and_control_group_timing_data,
    load_experiment_group_resource_usage_data,
)
from path_utils import PLOTTING_OUTPUT_DIR

print(scienceplots)

# use English labels by default
USE_ENGLISH_LABELS = False

MARKER: Optional[str] = None

plt.style.use(["science", "ieee", "no-latex", "cjk-sc-font"])

print()


def plot_min_execution_time():
    """Plot the minimum execution time comparison."""
    timing_data = load_experiment_and_control_group_timing_data()

    sample_names = sorted(
        list(timing_data.keys()),
        key=lambda x: int(x.split("_")[1]),
    )
    x = np.arange(len(sample_names))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))  # type: ignore

    # min_ms
    exp_min_times = [timing_data[name][0].min_ms for name in sample_names]
    ctrl_min_times = [timing_data[name][1].min_ms for name in sample_names]

    csv_dumper(exp_min_times, ctrl_min_times, "min_execution_time")

    ax.bar(  # type: ignore
        x - width / 2,
        exp_min_times,
        width,
        label="Experimental Group" if USE_ENGLISH_LABELS else "实验组",
        color="white",
        edgecolor="black",
        # hatch="///",
    )
    ax.bar(  # type: ignore
        x + width / 2,
        ctrl_min_times,
        width,
        label="Control Group" if USE_ENGLISH_LABELS else "对照组",
        color="black",
        edgecolor="black",
        # hatch="...",
    )
    ax.set_title("Minimum Execution Time" if USE_ENGLISH_LABELS else "最短执行时间")  # type: ignore
    ax.set_ylabel("Time (ms)" if USE_ENGLISH_LABELS else "时间 (ms)")  # type: ignore
    ax.set_xticks(x)  # type: ignore
    ax.set_xticklabels(sample_names)  # type: ignore
    ax.legend()  # type: ignore

    plt.tight_layout()
    plt.savefig(PLOTTING_OUTPUT_DIR / "min_execution_time.png", dpi=300)  # type: ignore
    plt.close(fig)


def plot_avg_execution_time():
    """Plot the average execution time comparison."""
    timing_data = load_experiment_and_control_group_timing_data()

    sample_names = sorted(
        list(timing_data.keys()),
        key=lambda x: int(x.split("_")[1]),
    )
    x = np.arange(len(sample_names))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))  # type: ignore

    # avg_ms
    exp_avg_times = [timing_data[name][0].avg_ms for name in sample_names]
    ctrl_avg_times = [timing_data[name][1].avg_ms for name in sample_names]

    csv_dumper(exp_avg_times, ctrl_avg_times, "avg_execution_time")

    ax.bar(  # type: ignore
        x - width / 2,
        exp_avg_times,
        width,
        label="EmberGraph",
        color="white",
        edgecolor="black",
        # hatch="///",
    )
    ax.bar(  # type: ignore
        x + width / 2,
        ctrl_avg_times,
        width,
        label="Neo4j",
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
    plt.savefig(PLOTTING_OUTPUT_DIR / "avg_execution_time.png", dpi=300)  # type: ignore
    plt.close(fig)


def plot_max_execution_time():
    """Plot the maximum execution time comparison."""
    timing_data = load_experiment_and_control_group_timing_data()

    sample_names = sorted(
        list(timing_data.keys()),
        key=lambda x: int(x.split("_")[1]),
    )
    x = np.arange(len(sample_names))
    width = 0.35

    fig, ax = plt.subplots(figsize=(10, 6))  # type: ignore

    # max_ms
    exp_max_times = [timing_data[name][0].max_ms for name in sample_names]
    ctrl_max_times = [timing_data[name][1].max_ms for name in sample_names]

    csv_dumper(exp_max_times, ctrl_max_times, "max_execution_time")

    ax.bar(  # type: ignore
        x - width / 2,
        exp_max_times,
        width,
        label="EmberGraph",
        color="white",
        edgecolor="black",
        # hatch="///",
    )
    ax.bar(  # type: ignore
        x + width / 2,
        ctrl_max_times,
        width,
        label="Neo4j",
        color="black",
        edgecolor="black",
        # hatch="...",
    )
    ax.set_title("Maximum Execution Time" if USE_ENGLISH_LABELS else "最长执行时间")  # type: ignore
    ax.set_ylabel("Time (ms)" if USE_ENGLISH_LABELS else "时间 (ms)")  # type: ignore
    ax.set_xticks(x)  # type: ignore
    ax.set_xticklabels(sample_names)  # type: ignore
    ax.legend()  # type: ignore

    plt.tight_layout()
    plt.savefig(PLOTTING_OUTPUT_DIR / "max_execution_time.png", dpi=300)  # type: ignore
    plt.close(fig)


def plot_cpu_usage():
    """Plot the CPU usage over time."""
    resource_data = load_experiment_group_resource_usage_data()

    timestamps = sorted(resource_data.keys())
    cpu_usage = [resource_data[ts].cpu_usage_percent for ts in timestamps]

    fig, ax = plt.subplots(figsize=(10, 6))  # type: ignore

    # cpu_usage
    ax.plot(timestamps, cpu_usage, marker=MARKER, linestyle="-")  # type: ignore
    ax.set_title("CPU Usage" if USE_ENGLISH_LABELS else "CPU累计使用率")  # type: ignore
    ax.set_xlabel("Time (ms)" if USE_ENGLISH_LABELS else "时间 (ms)")  # type: ignore
    ax.set_ylabel("CPU Usage (%)" if USE_ENGLISH_LABELS else "CPU使用率 (%)")  # type: ignore
    ax.grid(True)  # type: ignore

    plt.tight_layout()
    plt.savefig(PLOTTING_OUTPUT_DIR / "cpu_usage.png", dpi=300)  # type: ignore
    plt.close(fig)


def plot_memory_usage():
    """Plot the memory usage over time."""
    resource_data = load_experiment_group_resource_usage_data()

    timestamps = sorted(resource_data.keys())
    memory_usage = [resource_data[ts].memory_bytes for ts in timestamps]

    fig, ax = plt.subplots(figsize=(10, 6))  # type: ignore

    # memory_usage
    # convert memory_usage from bytes to MB
    memory_usage_mb = [mem / (1024 * 1024) for mem in memory_usage]
    ax.plot(timestamps, memory_usage_mb, marker=MARKER, linestyle="-")  # type: ignore
    ax.set_title("Memory Usage" if USE_ENGLISH_LABELS else "内存累计使用量")  # type: ignore
    ax.set_xlabel("Time (ms)" if USE_ENGLISH_LABELS else "时间 (ms)")  # type: ignore
    ax.set_ylabel("Memory Usage (MB)" if USE_ENGLISH_LABELS else "内存使用量 (MB)")  # type: ignore
    ax.grid(True)  # type: ignore

    plt.tight_layout()
    plt.savefig(PLOTTING_OUTPUT_DIR / "memory_usage.png", dpi=300)  # type: ignore
    plt.close(fig)


if __name__ == "__main__":
    plot_min_execution_time()
    plot_avg_execution_time()
    plot_max_execution_time()
    plot_cpu_usage()
    plot_memory_usage()

    print("Plots saved to: ", PLOTTING_OUTPUT_DIR)
