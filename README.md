# EmberGraph (pre-published)

To query a pattern on `multi directed graph` with `label / attribute filtering` efficiently, support `Neo4j` and `SQLite3` as the storage layer.

> ## ❤️ Please give a `Star` / `Follow` if you like this project

## To begin with

The project is still under development, however it has already been proved to be a correct implementation.

The test dataset is `LDBC-SNB-SF0.1`, and the example used for testing all come from `LDBC-SNB Business Intelligence (BI 1 ~ 20)`. You could find the actual `query` statements from `./resources/cypher`.

`EmberGraph` is only one-more-step to be formally published:

> To **optimize** the `matching order generation algorithm` (most likely to be `iterative dynamic programming` just like what `Neo4j` has done since it's `2.2` version)

## What's the most impressive?

Even though we're still working on the one-last-step to use the **better** `matching-order seeking strategy`, the performance of this `pre-published version` is approximately **equivalent** to that of `Neo4j`. (Oh, even **faster** in some cases!)

## How to build

You should have installed `git-lfs` first, to correctly clone this project:

```bash
git clone https://github.com/DrEden33773/EmberGraph
git lfs pull
cargo build # This will automatically install uv if in need, and then initialize `./ember-graph-import`
```

Then, you should manually do some initialization steps:

1. Transformed the original dataset into a `Neo4j-import-friendly` one. (Using `./ember-graph-import/raw_data_formatter.py`)
2. Generated basic `label-based` statistics. (Using `./ember-graph-import/statistics_generator.py`)
3. Imported `Neo4j-import-friendly` dataset into `Neo4j` or `SQLite3`. (Using `./ember-graph-import/neo4j_admin_import.py` or `./ember-graph-import/sqlite_import.py`)

All of the `python scripts` mentioned above could executed via the command:

```bash
cd ./ember-graph-import
source ./.venv/bin/activate
uv run <SCRIPT-NAME>
```

Right now, if you want to, you could run commands:

```bash
cargo run --example bi_<x> # (where x in [1..=20])
```

To check the query result of `bi_1` to `bi_20`.

## Something important for `release` mode building

Yes, you might have guessed -- It's totally possible to get the highest performance to build under the `release` mode.

However, I have to mind you that could be `TOO SLOW`.

So, if you really don't mind, here're several better options:

- Linux:
  
```bash
cargo build --release --all-target -j $(nproc)
```

- Mac:

```bash
cargo build --release --all-target -j $(sysctl -n hw.ncpu)
```

- Windows(Powershell):

```powershell
cargo build --release --all-target -j $env:NUMBER_OF_PROCESSORS
```

- Windows(CMD):

```cmd
cargo build --release --all-target -j %NUMBER_OF_PROCESSORS%
```

After all, that's a short-term pain for long-term gain😂.

Then, you could run the command below to check the query result of `bi_1` to `bi_20`:

```bash
cargo run --release --example bi_<x> # (where x in [1..=20])
```
