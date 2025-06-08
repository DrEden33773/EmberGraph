import polars as pl
from path_utils import PROJECT_OUTPUT_DIR


def csv_dumper(
    exp_times: list[float], ctrl_times: list[float], filename: str = "平均执行时间"
):
    exp_times = [round(time, 2) for time in exp_times]
    ctrl_times = [round(time, 2) for time in ctrl_times]
    df = pl.DataFrame(
        {
            "task": [f"BI-{i}" for i in range(1, 21)],
            "EmberGraph": exp_times,
            "Neo4j": ctrl_times,
        }
    )
    if not filename.endswith(".csv"):
        filename = filename + ".csv"
    df.write_csv(PROJECT_OUTPUT_DIR / filename, separator="&")
