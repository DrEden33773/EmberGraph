from pathlib import Path
from typing import Optional

import polars as pl
from colorama import Fore, Style  # type: ignore

ROOT = Path(__file__).parent.parent.absolute()
TEST_DATASET = ROOT / "data" / "ldbc-sn-interactive-sf01"
NODES = TEST_DATASET / "nodes"
RELATIONSHIPS = TEST_DATASET / "relationships"


def transform_relationship_csv(file_path: Path):
    updated = False
    df = pl.read_csv(file_path, separator="|")
    columns = df.columns

    src_v_label = file_path.stem.split("_")[0].capitalize()
    e_label = file_path.stem.split("_")[1]
    dst_v_label = file_path.stem.split("_")[2].capitalize()

    rename_mapping: dict[str, str] = {}
    if len(columns) > 0 and "(" not in columns[0] and ")" not in columns[0]:
        rename_mapping[columns[0]] = f":START_ID({src_v_label})"
    if len(columns) > 1 and "(" not in columns[1] and ")" not in columns[1]:
        rename_mapping[columns[1]] = f":END_ID({dst_v_label})"

    if rename_mapping:
        df = df.rename(rename_mapping)
        updated = True

    if ":TYPE" not in df.columns:
        df = df.with_columns(pl.lit(e_label).alias(":TYPE"))
        updated = True

    if updated:
        df.write_csv(file_path, separator="|")
        print(f"✅  Updated '{Fore.GREEN + file_path.stem + Style.RESET_ALL}'.")
    else:
        print(f"👌  Skipped updated '{Fore.GREEN + file_path.stem + Style.RESET_ALL}'.")


def transform_node_csv(file_path: Path):
    updated = False
    df = pl.read_csv(file_path, separator="|")
    columns = df.columns

    v_label = file_path.stem.split("_")[0].capitalize()

    rename_mapping: dict[str, str] = {}
    if len(columns) > 0 and columns[0] == "id":
        rename_mapping[columns[0]] = f":ID({v_label})"
        df = df.with_columns(pl.col(columns[0]).cast(pl.Utf8).alias("attr_id:long"))

    if rename_mapping:
        df = df.rename(rename_mapping)
        df = df.rename({"attr_id:long": "id:long"})
        updated = True

    if "type" in df.columns:
        df = df.with_columns(
            pl.col("type").cast(pl.Utf8).str.to_titlecase().alias(":LABEL")
        ).drop("type")
        updated = True
    elif ":LABEL" not in df.columns:
        df = df.with_columns(pl.lit(v_label).alias(":LABEL"))
        updated = True

    if updated:
        df.write_csv(file_path, separator="|")
        print(f"✅  Updated '{Fore.GREEN + file_path.stem + Style.RESET_ALL}'.")
    else:
        print(f"👌  Skipped updated '{Fore.GREEN + file_path.stem + Style.RESET_ALL}'.")


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

    for node_file in nodes_dir.iterdir():
        if node_file.suffix == ".csv":
            transform_node_csv(node_file)

    for relationship_file in relationships_dir.iterdir():
        if relationship_file.suffix == ".csv":
            transform_relationship_csv(relationship_file)


if __name__ == "__main__":
    exec()
