#!/usr/bin/env bash
# One-off: build a Hive metastore (Derby) for a parquet dataset with this Spark.
# usage: spike/setup-catalog.sh <parquet-dir> [database]
set -euo pipefail
source "$(dirname "$0")/env.sh"

PARQUET_DIR="$1"
DB="${2:-default}"
mkdir -p "$CATALOG/warehouse"

"$SPARK_HOME/bin/spark-submit" \
  --class edu.polytechnique.cedar.spark.benchmark.RegisterParquetTables \
  --name catalog-setup \
  "${SPARK_COMMON_ARGS[@]}" \
  --conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$CATALOG/metastore_db;create=true" \
  "$JAR" \
  --parquetDir "$PARQUET_DIR" --database "$DB"
