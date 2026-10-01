// Exports table definitions + statistics (everything the planner reads, no data)
// from the metastore of the Spark it runs in, into metastore-export.json.
//
// Run on the server with the same Spark/Hive config as the trace runs:
//   EXPORT_DBS=tpcds_100,tpch_100 $SPARK_HOME/bin/spark-shell --master 'local[1]' -i export-metastore.scala
// EXPORT_DBS is optional (default: all databases). EXPORT_FILE overrides the output path.
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

val exportCatalog = spark.sessionState.catalog
val exportDbs: Seq[String] = sys.env.get("EXPORT_DBS").map(_.split(",").map(_.trim).toSeq)
  .getOrElse(exportCatalog.listDatabases())
val exportFile = sys.env.getOrElse("EXPORT_FILE", "metastore-export.json")

def strMap(m: Map[String, String]): JObject = JObject(m.toList.sortBy(_._1).map { case (k, v) => k -> JString(v) })

def statsJson(s: Option[CatalogStatistics]): JValue = s match {
  case None => JNull
  case Some(st) =>
    ("sizeInBytes" -> st.sizeInBytes.toString) ~
      ("rowCount" -> st.rowCount.map(_.toString)) ~
      ("colStats" -> JObject(st.colStats.toList.sortBy(_._1).map { case (c, cs) => c -> strMap(cs.toMap(c)) }))
}

val exportedTables: Seq[JValue] = exportDbs.flatMap { db =>
  exportCatalog.listTables(db).filter(_.database.isDefined).flatMap { ident =>
    val t = exportCatalog.getTableMetadata(ident)
    if (t.tableType == CatalogTableType.VIEW) {
      println(s"skipping view ${t.identifier}"); None
    } else {
      val parts = if (t.partitionColumnNames.nonEmpty) exportCatalog.listPartitions(ident) else Seq.empty
      println(s"exporting ${t.identifier}: stats=${t.stats.isDefined}, partitions=${parts.size}")
      Some(
        ("database" -> db) ~
          ("table" -> t.identifier.table) ~
          ("tableType" -> t.tableType.name) ~
          ("provider" -> t.provider) ~
          ("schema" -> parse(t.schema.json)) ~
          ("partitionColumnNames" -> t.partitionColumnNames) ~
          ("tracksPartitionsInCatalog" -> t.tracksPartitionsInCatalog) ~
          ("bucketSpec" -> t.bucketSpec.map(b =>
            ("numBuckets" -> b.numBuckets) ~ ("bucketColumnNames" -> b.bucketColumnNames) ~
              ("sortColumnNames" -> b.sortColumnNames))) ~
          ("location" -> t.storage.locationUri.map(_.toString)) ~
          ("inputFormat" -> t.storage.inputFormat) ~
          ("outputFormat" -> t.storage.outputFormat) ~
          ("serde" -> t.storage.serde) ~
          ("compressed" -> t.storage.compressed) ~
          ("storageProperties" -> strMap(t.storage.properties)) ~
          ("properties" -> strMap(t.properties)) ~
          ("stats" -> statsJson(t.stats)) ~
          ("partitions" -> parts.map(p =>
            ("spec" -> strMap(p.spec)) ~
              ("location" -> p.storage.locationUri.map(_.toString)) ~
              ("parameters" -> strMap(p.parameters)) ~
              ("stats" -> statsJson(p.stats))))
      )
    }
  }
}

val exportJson = ("sparkVersion" -> spark.version) ~ ("tables" -> exportedTables)
java.nio.file.Files.write(java.nio.file.Paths.get(exportFile), pretty(render(exportJson)).getBytes("UTF-8"))
println(s"wrote ${exportedTables.size} tables to $exportFile")
sys.exit(0)
