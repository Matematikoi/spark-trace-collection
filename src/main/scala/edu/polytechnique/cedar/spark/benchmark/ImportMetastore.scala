package edu.polytechnique.cedar.spark.benchmark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.types.{DataType, StructType}
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import java.io.File
import java.net.URI
import scala.io.Source

/** Recreates the tables of a metastore-export.json (see spike/export-metastore.scala)
  * in this Spark's metastore, with the exported statistics and no data. Every
  * location is redirected to an empty local directory under --dataRoot, so the
  * planner works from the statistics alone. Run in its own process.
  */
object ImportMetastore {

  case class Config(file: String = null, dataRoot: String = null)

  private implicit val formats: DefaultFormats.type = DefaultFormats

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("ImportMetastore") {
      opt[String]("file")
        .action((x, c) => c.copy(file = x))
        .text("metastore-export.json")
        .required()
      opt[String]("dataRoot")
        .action((x, c) => c.copy(dataRoot = x))
        .text("local directory that replaces every table/partition location")
        .required()
    }
    parser.parse(args, Config()) match {
      case Some(config) => run(config)
      case None         => System.exit(1)
    }
  }

  private def strMap(v: JValue): Map[String, String] = v match {
    case JObject(fields) => fields.collect { case (k, JString(s)) => k -> s }.toMap
    case _               => Map.empty
  }

  private def optStr(v: JValue): Option[String] = v match {
    case JString(s) => Some(s)
    case _          => None
  }

  // spark.sql.* keys and EXTERNAL are reserved by HiveExternalCatalog (it rebuilds
  // them from provider/schema/stats/tableType); passing them back in is rejected.
  private def userProps(m: Map[String, String]): Map[String, String] =
    m.filterKeys(k => !k.startsWith("spark.sql.") && k != "EXTERNAL").toMap

  private def stats(v: JValue, table: TableIdentifier): Option[CatalogStatistics] = v match {
    case JNull | JNothing => None
    case s =>
      val colStats = (s \ "colStats") match {
        case JObject(cols) =>
          cols.flatMap { case (col, m) =>
            CatalogColumnStat.fromMap(table.unquotedString, col, strMap(m)).map(col -> _)
          }.toMap
        case _ => Map.empty[String, CatalogColumnStat]
      }
      Some(
        CatalogStatistics(
          sizeInBytes = BigInt((s \ "sizeInBytes").extract[String]),
          rowCount = optStr(s \ "rowCount").map(BigInt(_)),
          colStats = colStats
        )
      )
  }

  // Spark sizes pruned partitions (and tables without stats) by listing files, so
  // each directory gets one sparse file as large as its data was on the cluster.
  // It takes no disk space and is never opened during planning.
  private def placeholder(dir: URI, fileSize: JValue): Unit = fileSize match {
    case JInt(size) if size > 0 =>
      val f = new java.io.RandomAccessFile(new File(new File(dir), "part-00000.placeholder"), "rw")
      try f.setLength(size.toLong)
      finally f.close()
    case _ =>
  }

  private def localDir(root: File, parts: String*): URI = {
    val dir = parts.foldLeft(root)((d, p) => new File(d, p))
    dir.mkdirs()
    dir.toURI
  }

  def run(config: Config): Unit = {
    val root = new File(config.dataRoot).getAbsoluteFile
    val export = parse(Source.fromFile(config.file).mkString)
    println(s"importing ${(export \ "tables").children.size} tables")

    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val catalog = spark.sessionState.catalog

    val tables = (export \ "tables").children
    tables.foreach { t =>
      val db = (t \ "database").extract[String]
      val name = (t \ "table").extract[String]
      val ident = TableIdentifier(name, Some(db))

      if (!catalog.databaseExists(db)) {
        catalog.createDatabase(
          CatalogDatabase(db, "", localDir(root, "_warehouse", s"$db.db"), Map.empty),
          ignoreIfExists = true
        )
      }

      val location = localDir(root, db, name)
      val storageProps = strMap(t \ "storageProperties") ++
        strMap(t \ "storageProperties").get("path").map(_ => "path" -> location.toString)
      val bucketSpec = (t \ "bucketSpec") match {
        case JNull | JNothing => None
        case b =>
          Some(BucketSpec(
            (b \ "numBuckets").extract[Int],
            (b \ "bucketColumnNames").extract[Seq[String]],
            (b \ "sortColumnNames").extract[Seq[String]]
          ))
      }
      val table = CatalogTable(
        identifier = ident,
        tableType = CatalogTableType.EXTERNAL,
        storage = CatalogStorageFormat(
          locationUri = Some(location),
          inputFormat = optStr(t \ "inputFormat"),
          outputFormat = optStr(t \ "outputFormat"),
          serde = optStr(t \ "serde"),
          compressed = (t \ "compressed").extractOrElse[Boolean](false),
          properties = storageProps
        ),
        schema = (t \ "schema") match {
          case JNull | JNothing => StructType.fromDDL((t \ "ddlSchema").extract[String])
          case s                => DataType.fromJson(compact(render(s))).asInstanceOf[StructType]
        },
        provider = optStr(t \ "provider"),
        partitionColumnNames = (t \ "partitionColumnNames").extract[Seq[String]],
        bucketSpec = bucketSpec,
        properties = userProps(strMap(t \ "properties")),
        // partitions listed from the metastore (and pruned there), not discovered from directories
        tracksPartitionsInCatalog = (t \ "tracksPartitionsInCatalog").extractOrElse[Boolean](false)
      )
      placeholder(location, t \ "fileSize")
      catalog.createTable(table, ignoreIfExists = true, validateLocation = false)
      catalog.alterTableStats(ident, stats(t \ "stats", ident))

      val partitions = (t \ "partitions").children.map { p =>
        val spec = strMap(p \ "spec")
        val dirs = table.partitionColumnNames.map(c => s"$c=${spec(c)}")
        val partLocation = localDir(root, (Seq(db, name) ++ dirs): _*)
        placeholder(partLocation, p \ "fileSize")
        CatalogTablePartition(
          spec = spec,
          storage = table.storage.copy(locationUri = Some(partLocation), properties = Map.empty),
          parameters = userProps(strMap(p \ "parameters")),
          stats = stats(p \ "stats", ident)
        )
      }
      if (partitions.nonEmpty) {
        catalog.createPartitions(ident, partitions, ignoreIfExists = true)
        if (partitions.exists(_.stats.isDefined)) catalog.alterPartitions(ident, partitions)
      }
      println(s"imported $db.$name (stats=${(t \ "stats") != JNull}, partitions=${partitions.size})")
    }
    println(s"imported ${tables.size} tables")
    spark.stop()
  }
}
