#!/bin/bash

QUERY_FILE=${1:-"./resources/plan/ldbc-bi-1.json"}
NEO4J_SERVER_PID=${2:-$(pgrep -f "neo4j" | head -1)}

cargo run --bin benchmark_runner --release --features bench_via_neo4j_only_with_cache_eviction -- --storage=neo4j --query-file="$QUERY_FILE" --neo4j-server-pid=$NEO4J_SERVER_PID
