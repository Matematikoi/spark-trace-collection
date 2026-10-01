package edu.polytechnique.cedar.spark.benchmark

import org.apache.spark.sql.SparkSession

import java.io.File

/** One-off catalog setup: registers every sub-directory of a parquet dir as a
  * table in the Hive metastore and computes table + column statistics.
  * Mirrors query-generator's spark_connection/catalog_setup.py. Run it in its
  * own process, never in the same session as a traced query.
  */
object RegisterParquetTables {

  case class Config(parquetDir: String = null, database: String = "default")

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("RegisterParquetTables") {
      opt[String]("parquetDir")
        .action((x, c) => c.copy(parquetDir = x))
        .text("directory with one sub-directory of parquet files per table")
        .required()
      opt[String]("database")
        .action((x, c) => c.copy(database = x))
        .text("target database, default 'default'")
    }
    parser.parse(args, Config()) match {
      case Some(config) => run(config)
      case None         => System.exit(1)
    }
  }

  def run(config: Config): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${config.database}")
    spark.sql(s"USE ${config.database}")

    val tableDirs = new File(config.parquetDir)
      .listFiles()
      .filter(d => d.isDirectory && !d.getName.startsWith("."))
      .sortBy(_.getName)

    tableDirs.foreach { dir =>
      val table = dir.getName
      println(s"registering $table")
      spark.sql(
        s"CREATE TABLE IF NOT EXISTS $table USING PARQUET LOCATION '${dir.getAbsolutePath}'"
      )
      spark.sql(s"ANALYZE TABLE $table COMPUTE STATISTICS")
      spark.sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR ALL COLUMNS")
    }
    println(s"registered ${tableDirs.length} tables in ${config.database}")
    spark.stop()
  }
}
