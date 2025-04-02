from pathlib import Path
from typing import Optional

from colorama import Fore, Style  # type: ignore

ROOT = Path(__file__).parent.parent.absolute()
TEST_DATASET = ROOT / "data" / "ldbc-sn-interactive-sf01"

NODES = TEST_DATASET / "nodes"
NODES.mkdir(parents=True, exist_ok=True)

RELATIONSHIPS = TEST_DATASET / "relationships"
RELATIONSHIPS.mkdir(parents=True, exist_ok=True)


def validate_nodes_relationships_dir(
    nodes_dir: Optional[Path] = None, relationships_dir: Optional[Path] = None
):
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

    return nodes_dir, relationships_dir
