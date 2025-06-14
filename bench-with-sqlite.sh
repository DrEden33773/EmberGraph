#!/bin/bash

# 检查是否提供了参数，如果没有则使用默认值
QUERY_FILE=${1:-"./resources/plan/ldbc-bi-1.json"}

cargo run --bin benchmark_runner --release --features bench_via_sqlite_only_with_cache_eviction -- --storage=sqlite --query-file="$QUERY_FILE"
