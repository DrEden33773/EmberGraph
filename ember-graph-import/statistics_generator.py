"""
Core module.

Generate statistics from the given dataset.
"""

import asyncio
import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional, override

import polars as pl
from colorama import Fore, Style  # type: ignore
from path_utils import validate_nodes_relationships_dir

ROOT = Path(__file__).parent.parent.absolute()
TEST_DATASET = ROOT / "data" / "ldbc-sn-interactive-sf01"

NODES = TEST_DATASET / "nodes"
NODES.mkdir(parents=True, exist_ok=True)

RELATIONSHIPS = TEST_DATASET / "relationships"
RELATIONSHIPS.mkdir(parents=True, exist_ok=True)

STATISTICS = ROOT / "resources" / "statistics"
STATISTICS.mkdir(parents=True, exist_ok=True)


@dataclass
class Statistics:
    v_cnt: int = 0
    e_cnt: int = 0
    v_label_cnt: dict[str, int] = field(default_factory=dict)
    e_label_cnt: dict[str, int] = field(default_factory=dict)


class StatisticsEncoder(json.JSONEncoder):
    @override
    def default(self, o: Any):
        if isinstance(o, Statistics):
            return asdict(o)
        return super().default(o)


statistics = Statistics()


async def visit_node_csv(file_path: Path):
    global statistics
    lf = pl.scan_csv(file_path, separator="|")
    v_label_cnt: dict[str, int] = {}

    column_names = lf.collect_schema().names()
    if ":LABEL" in column_names:
        collected = await lf.select(pl.col(":LABEL")).collect_async()
        label_counts = collected[":LABEL"].value_counts()

        for row in label_counts.iter_rows():
            label, count = row
            if (label := str(label)) in v_label_cnt:
                v_label_cnt[label] += int(count)
            else:
                v_label_cnt[label] = int(count)

        statistics.v_cnt += sum(v_label_cnt.values())
        statistics.v_label_cnt.update(v_label_cnt)

        print(f"""\
✅  '{Fore.GREEN + file_path.stem + Style.RESET_ALL}' \
=> {Fore.YELLOW + str(v_label_cnt) + Style.RESET_ALL}
        """)


async def visit_relationship_csv(file_path: Path):
    global statistics
    lf = pl.scan_csv(file_path, separator="|")
    e_label_cnt: dict[str, int] = {}

    column_names = lf.collect_schema().names()
    if ":TYPE" in column_names:
        collected = await lf.select(pl.col(":TYPE")).collect_async()
        label_counts = collected[":TYPE"].value_counts()

        for row in label_counts.iter_rows():
            label, count = row
            if (label := str(label)) in e_label_cnt:
                e_label_cnt[label] += int(count)
            else:
                e_label_cnt[label] = int(count)

        statistics.e_cnt += sum(e_label_cnt.values())
        statistics.e_label_cnt.update(e_label_cnt)

        print(f"""\
✅  '{Fore.GREEN + file_path.stem + Style.RESET_ALL}' \
=> {Fore.YELLOW + str(e_label_cnt) + Style.RESET_ALL}
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
    filename = "label_statistics.json"
    path = STATISTICS / filename

    with open(path, "w", encoding="utf-8") as f:
        json.dump(statistics, f, cls=StatisticsEncoder, indent=2, ensure_ascii=False)

    print(f"✅  Statistics saved to {Fore.GREEN + str(path) + Style.RESET_ALL}")


if __name__ == "__main__":
    exec()
    dump()
