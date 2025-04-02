import os
import platform
import subprocess
from pathlib import Path
from typing import Optional

from path_utils import validate_nodes_relationships_dir

SUDO_PREFIX = "sudo" if platform.system().lower() != "windows" else ""
NEO4J_IMPORT_PRE = f"{SUDO_PREFIX} neo4j-admin database import full"
NEO4J_IMPORT_OPT = f'--delimiter="|" --threads={os.cpu_count()} --high-parallel-io=on --overwrite-destination --verbose'
NEO4J_IMPORT = f"{NEO4J_IMPORT_PRE} {NEO4J_IMPORT_OPT} neo4j"


def exec(nodes_dir: Optional[Path] = None, relationships_dir: Optional[Path] = None):
    nodes_dir, relationships_dir = validate_nodes_relationships_dir(
        nodes_dir, relationships_dir
    )

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
