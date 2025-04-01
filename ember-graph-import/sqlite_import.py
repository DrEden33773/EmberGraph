from pathlib import Path
from typing import Any, Optional

import polars as pl
from colorama import Fore, Style  # type: ignore
from sqlite_entity import DB_Edge, DB_Vertex, init_db_with_clear
from sqlmodel import Session
from tqdm import tqdm

ROOT = Path(__file__).parent.parent.absolute()
TEST_DATASET = ROOT / "data" / "ldbc-sn-interactive-sf01"
NODES = TEST_DATASET / "nodes"
RELATIONSHIPS = TEST_DATASET / "relationships"

raw_eid_cnt: dict[str, int] = {}


def to_typed_attrs(attrs: dict[str, Any]):
    result: dict[str, str | int | float] = {}
    for key, value in attrs.items():
        if ":" not in key:
            result[key] = str(value)
            continue
        name = key.split(":")[0]
        type_ = key.split(":")[1]
        match type_:
            case "int" | "long":
                result[name] = int(value)
            case "float":
                result[name] = float(value)
            case _:
                result[name] = str(value)
    return result


def load_v(file_path: Path, session: Session):
    global unique_vid_cnt

    df = pl.read_csv(file_path, separator="|")
    columns = df.columns
    scope = file_path.stem.split("_")[0].capitalize()
    new_vertices: list[DB_Vertex] = []

    for i in range(len(df)):
        row = df.row(i)
        attrs = {name: value for name, value in zip(columns[1:], row[1:])}
        vid = f"{scope}^{row[0]}"
        label = str(attrs.pop(":LABEL"))
        typed_attrs = to_typed_attrs(attrs)
        new_vertex = DB_Vertex(vid=vid, label=label, attrs=typed_attrs)
        new_vertices.append(new_vertex)

    with tqdm(
        total=len(new_vertices),
        desc=f"💾  Loading vertex: '{Fore.GREEN + file_path.stem + Style.RESET_ALL}'",
    ) as bar:
        for v in new_vertices:
            session.add(v)
            v.load_pending_attrs(session)
            bar.update(1)
    session.commit()


def load_e(file_path: Path, session: Session):
    global raw_eid_cnt

    df = pl.read_csv(file_path, separator="|")
    columns = df.columns
    src_scope = file_path.stem.split("_")[0].capitalize()
    dst_scope = file_path.stem.split("_")[2].capitalize()
    new_edges: list[DB_Edge] = []

    for i in range(len(df)):
        row = df.row(i)
        attrs = {name: value for name, value in zip(columns[2:], row[2:])}
        src_vid = f"{src_scope}^{row[0]}"
        dst_vid = f"{dst_scope}^{row[1]}"

        raw_eid = f"{src_vid} -> {dst_vid}"
        raw_eid_cnt.setdefault(raw_eid, 0)
        eid = f"{raw_eid} @ {raw_eid_cnt[raw_eid]}"
        raw_eid_cnt[raw_eid] += 1

        label = attrs.pop(":TYPE")
        typed_attrs = to_typed_attrs(attrs)
        new_edge = DB_Edge(
            eid=eid,
            src_vid=src_vid,
            dst_vid=dst_vid,
            label=label,
            attrs=typed_attrs,
        )
        new_edges.append(new_edge)

    with tqdm(
        total=len(new_edges),
        desc=f"💾  Loading edge: '{Fore.GREEN + file_path.stem + Style.RESET_ALL}'",
    ) as bar:
        for e in new_edges:
            session.add(e)
            e.load_pending_attrs(session)
            bar.update(1)
    session.commit()


def exec(
    db_name: str = "ldbc_sn_interactive_sf01.db",
    nodes_dir: Optional[Path] = None,
    relationships_dir: Optional[Path] = None,
):
    db_name = db_name if db_name.endswith(".db") else f"{db_name}.db"
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
            ⚠️  '{Fore.YELLOW + nodes_dir.stem + Style.RESET_ALL}' \
            is not a directory, fall back to default \
            ('{Fore.GREEN + NODES.stem + Style.RESET_ALL}').\
            """
        )
        nodes_dir = NODES
    if not relationships_dir.is_dir():
        print(
            f"""\
            ⚠️  '{Fore.YELLOW + relationships_dir.stem + Style.RESET_ALL} '\
            is not a directory, fall back to default \
            ('{Fore.GREEN + NODES.stem + Style.RESET_ALL}').\
            """
        )
        relationships_dir = RELATIONSHIPS

    DB_URL = f"sqlite:///{ROOT}/{db_name}"
    engine = init_db_with_clear(DB_URL, echo=False)

    with Session(engine) as session:
        for node_file in nodes_dir.iterdir():
            if node_file.suffix != ".csv":
                continue
            load_v(node_file, session)
        for relationship_file in relationships_dir.iterdir():
            if relationship_file.suffix != ".csv":
                continue
            load_e(relationship_file, session)


if __name__ == "__main__":
    exec()
