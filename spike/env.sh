# Shared settings for the spike scripts. Source it, don't run it.
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export SPARK_HOME="${SPARK_HOME:-/Users/matematikoi/phd/repos/relevant/custom_spark/spark}"
export JAVA_HOME="${JAVA_HOME:-$REPO/.pixi/envs/default}"
CATALOG="${CATALOG:-$REPO/spike/catalog}"
JAR="$REPO/target/scala-2.12/spark-stage-tuning_2.12-1.0-SNAPSHOT.jar"
SCOPT_JAR="${SCOPT_JAR:-$HOME/Library/Caches/Coursier/v1/https/repo1.maven.org/maven2/com/github/scopt/scopt_2.12/3.7.1/scopt_2.12-3.7.1.jar}"

# Hive metastore (embedded Derby: one process at a time)
SPARK_COMMON_ARGS=(
  --master 'local[*]'
  --properties-file "$REPO/conf/defaults.conf"
  --conf spark.sql.catalogImplementation=hive
  --conf "spark.sql.warehouse.dir=$CATALOG/warehouse"
  --conf "spark.hadoop.javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=$CATALOG/metastore_db;create=false"
  --conf spark.hadoop.javax.jdo.option.ConnectionDriverName=org.apache.derby.jdbc.EmbeddedDriver
  --jars "$SCOPT_JAR"
)
