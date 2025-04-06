import asyncio
from pathlib import Path
from typing import Optional

import polars as pl
from colorama import Fore, Style  # type: ignore
from path_utils import validate_nodes_relationships_dir

parse_as_long_list = set(
    ["birthday", "classYear", "creationDate", "joinDate", "length"]
)


async def transform_relationship_csv(file_path: Path):
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

    # to parse these columns as `long`
    for column in columns[2:]:
        if column in parse_as_long_list:
            rename_mapping[column] = f"{column}:long"

    if rename_mapping:
        df = df.rename(rename_mapping)
        updated = True

    if ":TYPE" not in df.columns:
        df = df.with_columns(pl.lit(e_label).alias(":TYPE"))
        updated = True

    if updated:
        df.write_csv(file_path, separator="|")
        print(f"☑️   Updated '{Fore.GREEN + file_path.stem + Style.RESET_ALL}'.")
    else:
        print(f"👌  Skipped updated '{Fore.GREEN + file_path.stem + Style.RESET_ALL}'.")


async def transform_node_csv(file_path: Path):
    updated = False
    df = pl.read_csv(file_path, separator="|")
    columns = df.columns

    v_label = file_path.stem.split("_")[0].capitalize()
    id_flag = False

    rename_mapping: dict[str, str] = {}
    if len(columns) > 0 and columns[0] == "id":
        rename_mapping[columns[0]] = f":ID({v_label})"
        df = df.with_columns(pl.col(columns[0]).cast(pl.Utf8).alias("attr_id:long"))
        id_flag = True

    # to parse these columns as `long`
    for column in columns[1:]:
        if column in parse_as_long_list:
            rename_mapping[column] = f"{column}:long"

    if rename_mapping:
        df = df.rename(rename_mapping)
        df = df.rename({"attr_id:long": "id:long"}) if id_flag else df
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
        print(f"☑️   Updated '{Fore.GREEN + file_path.stem + Style.RESET_ALL}'.")
    else:
        print(f"👌  Skipped updated '{Fore.GREEN + file_path.stem + Style.RESET_ALL}'.")


async def exec_async(
    nodes_dir: Optional[Path] = None, relationships_dir: Optional[Path] = None
):
    nodes_dir, relationships_dir = validate_nodes_relationships_dir(
        nodes_dir, relationships_dir
    )

    tasks: list[asyncio.Task[None]] = []

    for node_file in nodes_dir.iterdir():
        if node_file.suffix == ".csv":
            task = asyncio.create_task(transform_node_csv(node_file))
            tasks.append(task)

    for relationship_file in relationships_dir.iterdir():
        if relationship_file.suffix == ".csv":
            task = asyncio.create_task(transform_relationship_csv(relationship_file))
            tasks.append(task)

    await asyncio.gather(*tasks)


def exec(nodes_dir: Optional[Path] = None, relationships_dir: Optional[Path] = None):
    return asyncio.run(exec_async(nodes_dir, relationships_dir))


if __name__ == "__main__":
    exec()
