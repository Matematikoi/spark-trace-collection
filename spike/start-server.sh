#!/usr/bin/env bash
# Start the long-lived LQP server.
# usage: spike/start-server.sh [port] [extra LqpServer args...]
set -euo pipefail
source "$(dirname "$0")/env.sh"

PORT="${1:-8080}"
shift || true

exec "$SPARK_HOME/bin/spark-submit" \
  --class edu.polytechnique.cedar.spark.server.LqpServer \
  --name lqp-server \
  "${SPARK_COMMON_ARGS[@]}" \
  "$JAR" \
  --port "$PORT" "$@"
