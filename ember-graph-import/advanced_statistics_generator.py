import asyncio
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, cast

import numpy as np
import polars as pl
from colorama import Fore, Style  # type: ignore
from path_utils import validate_nodes_relationships_dir
from schema import AttrType

ROOT = Path(__file__).parent.parent.absolute()
TEST_DATASET = ROOT / "data" / "ldbc-sn-interactive-sf01"

NODES = TEST_DATASET / "nodes"
NODES.mkdir(parents=True, exist_ok=True)

RELATIONSHIPS = TEST_DATASET / "relationships"
RELATIONSHIPS.mkdir(parents=True, exist_ok=True)

STATISTICS = ROOT / "resources" / "statistics"
STATISTICS.mkdir(parents=True, exist_ok=True)


@dataclass
class AttributeHistogram:
    """The histogram of an attribute"""

    bins: List[float] = field(default_factory=list)
    counts: List[int] = field(default_factory=list)

    value_counts: Dict[str, int] = field(default_factory=dict)
    """ For `AttrType.String`, record the frequency of each value. """


@dataclass
class OperatorSelectivity:
    """
    Estimation of the selectivity of different operators

    - Selectivity is the probability of a value being selected by the operator
        - range: [0.0, 1.0]
    """

    eq: float = 1.0
    ne: float = 1.0

    gt: Dict[float, float] = field(default_factory=dict)
    """ `>`'s selectivity for different values """
    ge: Dict[float, float] = field(default_factory=dict)
    """ `>=`'s selectivity for different values """
    lt: Dict[float, float] = field(default_factory=dict)
    """ `<`'s selectivity for different values """
    le: Dict[float, float] = field(default_factory=dict)
    """ `<=`'s selectivity for different values """


@dataclass
class AttributeStats:
    """Statistics of an attribute"""

    count: int = 0
    """ The number of times the attribute appears in the dataset """
    null_count: int = 0
    """ The number of `null` values in the attribute """
    distinct_count: int = 0
    """ The number of `distinct` values in the attribute """
    min_value: Optional[Union[float, int]] = None
    """ The minimum value of the attribute """
    max_value: Optional[Union[float, int]] = None
    """ The maximum value of the attribute """

    histogram: AttributeHistogram = field(default_factory=AttributeHistogram)
    selectivity: OperatorSelectivity = field(default_factory=OperatorSelectivity)

    type: AttrType = AttrType.String
    """ The type of the attribute """


@dataclass
class Statistics:
    """Statistics of the graph"""

    v_cnt: int = 0
    e_cnt: int = 0

    v_label_cnt: Dict[str, int] = field(default_factory=dict)
    e_label_cnt: Dict[str, int] = field(default_factory=dict)

    v_attr_stats: Dict[str, Dict[str, AttributeStats]] = field(default_factory=dict)
    """ {v_label: {v_attr_name: stats}} """
    e_attr_stats: Dict[str, Dict[str, AttributeStats]] = field(default_factory=dict)
    """ {e_label: {e_attr_name: stats}} """


class StatisticsEncoder(json.JSONEncoder):
    def default(self, o: Any):
        if (
            isinstance(o, Statistics)
            or isinstance(o, AttributeStats)
            or isinstance(o, AttributeHistogram)
            or isinstance(o, OperatorSelectivity)
        ):
            return asdict(o)
        return super().default(o)


statistics = Statistics()


def collect_attribute_stats(df: pl.DataFrame, label: str) -> Dict[str, AttributeStats]:
    stats: Dict[str, AttributeStats] = {}
    total_rows = len(df)

    for col in df.columns:
        # exclude special columns
        if col.startswith(":") or col in [
            ":ID",
            ":START_ID",
            ":END_ID",
            ":LABEL",
            ":TYPE",
        ]:
            continue

        col_stats = AttributeStats()
        col_stats.count = df[col].count()
        col_stats.null_count = total_rows - col_stats.count

        if df[col].dtype in [pl.Int64, pl.Float64]:
            col_stats.type = (
                AttrType.Int if df[col].dtype == pl.Int64 else AttrType.Float
            )

            non_null = df.filter(pl.col(col).is_not_null())
            if not len(non_null) > 0:
                continue

            # figure out `min` and `max` values
            if (_min := non_null[col].min()) and isinstance(_min, (int, float)):
                col_stats.min_value = _min
            if (_max := non_null[col].max()) and isinstance(_max, (int, float)):
                col_stats.max_value = _max

            # figure out `distinct_count`
            col_stats.distinct_count = non_null[col].n_unique()

            # create histogram
            num_bins = min(10, col_stats.distinct_count)
            if not num_bins > 1:
                continue

            if not col_stats.min_value or not col_stats.max_value:
                continue

            bin_edges = np.linspace(
                col_stats.min_value, col_stats.max_value, num_bins + 1
            )
            counts = [0] * num_bins

            # count the number of values in each bin
            for i in range(num_bins):
                if i == num_bins - 1:
                    counts[i] = non_null.filter(
                        (pl.col(col) >= bin_edges[i])
                        & (pl.col(col) <= bin_edges[i + 1])
                    ).height
                else:
                    counts[i] = non_null.filter(
                        (pl.col(col) >= bin_edges[i]) & (pl.col(col) < bin_edges[i + 1])
                    ).height

            if bins := bin_edges.tolist():
                if isinstance(bins, float):
                    col_stats.histogram.bins = [bins]
                elif isinstance(bins, list) and isinstance(bins[0], float):
                    col_stats.histogram.bins = cast(list[float], bins)
            col_stats.histogram.counts = counts

            # figure out the selectivity of `=` and `!=` operators
            if col_stats.distinct_count > 0:
                col_stats.selectivity.eq = 1.0 / col_stats.distinct_count
            col_stats.selectivity.ne = 1.0 - col_stats.selectivity.eq

            # figure out the selectivity of `range` operators
            cum_sum = 0
            for i in range(num_bins):
                bin_value: float = bin_edges[i]
                bin_count = counts[i] if i < len(counts) else 0
                cum_count = cum_sum + bin_count

                # <
                col_stats.selectivity.lt[bin_value] = (
                    cum_sum / total_rows if total_rows > 0 else 0
                )
                # <=
                col_stats.selectivity.le[bin_value] = (
                    cum_count / total_rows if total_rows > 0 else 0
                )

                # >
                col_stats.selectivity.gt[bin_value] = 1.0 - (
                    cum_count / total_rows if total_rows > 0 else 0
                )
                # >=
                col_stats.selectivity.ge[bin_value] = 1.0 - (
                    cum_sum / total_rows if total_rows > 0 else 0
                )

                cum_sum = cum_count

        else:  # String
            col_stats.type = AttrType.String
            non_null = df.filter(pl.col(col).is_not_null())

            if len(non_null) > 0:
                # count distinct values
                col_stats.distinct_count = non_null[col].n_unique()

                # record the frequency of each value
                value_counts = non_null.group_by(col).agg(pl.len().alias("count"))

                for row in value_counts.iter_rows():
                    value, count = row
                    col_stats.histogram.value_counts[str(value)] = count

                # figure out the selectivity of `=` and `!=` operators
                if col_stats.distinct_count > 0:
                    col_stats.selectivity.eq = 1.0 / col_stats.distinct_count
                col_stats.selectivity.ne = 1.0 - col_stats.selectivity.eq

        stats[col] = col_stats

    return stats


async def visit_node_csv(file_path: Path):
    global statistics
    lf = pl.scan_csv(file_path, separator="|")

    # count the label
    column_names = lf.collect_schema().names()
    if ":LABEL" in column_names:
        collected = await lf.select(pl.col(":LABEL")).collect_async()
        label_counts = collected[":LABEL"].value_counts()

        v_label_cnt: Dict[str, int] = {}
        for row in label_counts.iter_rows():
            label, count = row
            if (label := str(label)) in v_label_cnt:
                v_label_cnt[label] += int(count)
            else:
                v_label_cnt[label] = int(count)

        statistics.v_cnt += sum(v_label_cnt.values())
        statistics.v_label_cnt.update(v_label_cnt)

        df = await lf.collect_async()
        labels = df[":LABEL"].unique()

        for label in labels:
            label_str = str(label)
            label_df = df.filter(pl.col(":LABEL") == label)

            # to collect statistics of attributes under the same label
            attr_stats = collect_attribute_stats(label_df, label_str)

            if label_str not in statistics.v_attr_stats:
                statistics.v_attr_stats[label_str] = {}

            statistics.v_attr_stats[label_str].update(attr_stats)

        print(f"""\
✅  '{Fore.GREEN + file_path.stem + Style.RESET_ALL}' \
=> {Fore.YELLOW + str(v_label_cnt) + Style.RESET_ALL}\
        """)


async def visit_relationship_csv(file_path: Path):
    global statistics
    lf = pl.scan_csv(file_path, separator="|")

    # count the label
    column_names = lf.collect_schema().names()
    if ":TYPE" in column_names:
        collected = await lf.select(pl.col(":TYPE")).collect_async()
        label_counts = collected[":TYPE"].value_counts()

        e_label_cnt: Dict[str, int] = {}
        for row in label_counts.iter_rows():
            label, count = row
            if (label := str(label)) in e_label_cnt:
                e_label_cnt[label] += int(count)
            else:
                e_label_cnt[label] = int(count)

        statistics.e_cnt += sum(e_label_cnt.values())
        statistics.e_label_cnt.update(e_label_cnt)

        df = await lf.collect_async()
        labels = df[":TYPE"].unique()

        for label in labels:
            label_str = str(label)
            label_df = df.filter(pl.col(":TYPE") == label)

            # to collect statistics of attributes under the same label
            attr_stats = collect_attribute_stats(label_df, label_str)

            if label_str not in statistics.e_attr_stats:
                statistics.e_attr_stats[label_str] = {}

            statistics.e_attr_stats[label_str].update(attr_stats)

        print(f"""\
✅  '{Fore.GREEN + file_path.stem + Style.RESET_ALL}' \
=> {Fore.YELLOW + str(e_label_cnt) + Style.RESET_ALL}\
        """)


async def exec_async(
    nodes_dir: Optional[Path] = None, relationships_dir: Optional[Path] = None
):
    nodes_dir, relationships_dir = validate_nodes_relationships_dir(
        nodes_dir, relationships_dir
    )

    tasks: list[asyncio.Task[None]] = []

    for node_file in nodes_dir.iterdir():
        if node_file.suffix == ".csv":
            task = asyncio.create_task(visit_node_csv(node_file))
            tasks.append(task)

    for relationship_file in relationships_dir.iterdir():
        if relationship_file.suffix == ".csv":
            task = asyncio.create_task(visit_relationship_csv(relationship_file))
            tasks.append(task)

    await asyncio.gather(*tasks)


def exec(nodes_dir: Optional[Path] = None, relationships_dir: Optional[Path] = None):
    return asyncio.run(exec_async(nodes_dir, relationships_dir))


def dump():
    filename = "advanced_statistics.json"
    path = STATISTICS / filename

    with open(path, "w", encoding="utf-8") as f:
        json.dump(statistics, f, cls=StatisticsEncoder, indent=2, ensure_ascii=False)

    print(f"✅  Statistics saved to {Fore.GREEN + str(path) + Style.RESET_ALL}")


if __name__ == "__main__":
    exec()
    dump()
