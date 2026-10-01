#!/usr/bin/env bash
# Collect one full trace with RunTemplateGeneric.
# usage: spike/trace-one.sh <query.sql> <out-dir> [database] [templateId] [queryId]
set -euo pipefail
source "$(dirname "$0")/env.sh"

QUERY="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
OUT="$2"
DB="${3:-default}"
TID="${4:-1}"
QID="${5:-1}"

"$SPARK_HOME/bin/spark-submit" \
  --class edu.polytechnique.cedar.spark.benchmark.RunTemplateGeneric \
  --name "trace_${TID}-${QID}" \
  "${SPARK_COMMON_ARGS[@]}" \
  "$JAR" \
  --databaseName "$DB" -t "$TID" -b spike -q "$QID" -p "$QUERY" -x "$OUT"
