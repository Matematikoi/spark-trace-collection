#!/usr/bin/env bash
# Build a local catalog (no data) from a metastore-export.json made by spike/export-metastore.scala.
# usage: spike/import-metastore.sh <metastore-export.json> <catalog-dir>
# then:  CATALOG=<catalog-dir> spike/start-server.sh
set -euo pipefail
source "$(dirname "$0")/env.sh"

EXPORT="$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
CATALOG="$(mkdir -p "$2" && cd "$2" && pwd)"
mkdir -p "$CATALOG/warehouse"

"$SPARK_HOME/bin/spark-submit" \
  --class edu.polytechnique.cedar.spark.benchmark.ImportMetastore \
  --name import-metastore \
  "${SPARK_COMMON_ARGS[@]}" \
  --conf "spark.sql.warehouse.dir=$CATALOG/warehouse" \
  --conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$CATALOG/metastore_db;create=true" \
  "$JAR" \
  --file "$EXPORT" --dataRoot "$CATALOG/emptydata"
