import os
import platform
import subprocess
from pathlib import Path
from typing import Optional

from colorama import Fore, Style  # type: ignore

ROOT = Path(__file__).parent.parent.absolute()
TEST_DATASET = ROOT / "data" / "ldbc-sn-interactive-sf01"
NODES = TEST_DATASET / "nodes"
RELATIONSHIPS = TEST_DATASET / "relationships"

SUDO_PREFIX = "sudo" if platform.system().lower() != "windows" else ""
NEO4J_IMPORT_PRE = f"{SUDO_PREFIX} neo4j-admin database import full"
NEO4J_IMPORT_OPT = f'--delimiter="|" --threads={os.cpu_count()} --high-parallel-io=on --overwrite-destination --verbose'
NEO4J_IMPORT = f"{NEO4J_IMPORT_PRE} {NEO4J_IMPORT_OPT} neo4j"


def exec(nodes_dir: Optional[Path] = None, relationships_dir: Optional[Path] = None):
    nodes_dir = nodes_dir or NODES
    relationships_dir = relationships_dir or RELATIONSHIPS

    if not nodes_dir.exists():
        raise FileNotFoundError(f"⚠️  Nodes directory '{nodes_dir}' doesn't exist.")
    if not relationships_dir.exists():
        raise FileNotFoundError(
            f"⚠️  Relationships directory '{relationships_dir}' doesn't exist."
        )
    if not nodes_dir.is_dir():
        print(
            f"""\
            ⚠️  '{Fore.YELLOW + str(nodes_dir) + Style.RESET_ALL}' \
            is not a directory, fall back to default \
            ('{Fore.GREEN + NODES.stem + Style.RESET_ALL}').\
            """
        )
        nodes_dir = NODES
    if not relationships_dir.is_dir():
        print(
            f"""\
            ⚠️  '{Fore.YELLOW + str(relationships_dir) + Style.RESET_ALL} '\
            is not a directory, fall back to default \
            ('{Fore.GREEN + NODES.stem + Style.RESET_ALL}').\
            """
        )
        relationships_dir = RELATIONSHIPS

    cmd_partials = [NEO4J_IMPORT]

    for node_file in nodes_dir.iterdir():
        if node_file.suffix != ".csv":
            continue
        partial = f'--nodes="{node_file}"'
        cmd_partials.append(partial)

    for relationship_file in relationships_dir.iterdir():
        if relationship_file.suffix != ".csv":
            continue
        partial = f'--relationships="{relationship_file}"'
        cmd_partials.append(partial)

    subprocess.call(" ".join(cmd_partials), shell=True)


if __name__ == "__main__":
    exec()
