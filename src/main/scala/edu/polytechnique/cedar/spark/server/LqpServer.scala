package edu.polytechnique.cedar.spark.server

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import edu.polytechnique.cedar.spark.benchmark.config.RunTemplateGenericConfig
import edu.polytechnique.cedar.spark.collector.CompileTimeCollector
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{Command, ParsedStatement}
import org.json4s.{DefaultFormats, JNothing, JNull, JString, JValue}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, parse, pretty, render}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong

/** Long-lived Spark session that returns the compile-time logical plan of a
  * query without executing it, in the same JSON format as the "CompileTimeLQP"
  * section of a trace written by RunTemplateGeneric.
  *
  * POST /lqp  {"sql": "...", "database": "default", "queryId": "1", "templateId": "1"}
  * GET  /health
  */
object LqpServer {

  case class Config(
      port: Int = 8080,
      resetExprIds: Boolean = true
  )

  private implicit val formats: DefaultFormats.type = DefaultFormats

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("LqpServer") {
      opt[Int]("port")
        .action((x, c) => c.copy(port = x))
        .text("HTTP port, default 8080")
      opt[Boolean]("resetExprIds")
        .action((x, c) => c.copy(resetExprIds = x))
        .text("reset expression ids before each request so plans match a fresh JVM, default true")
    }
    parser.parse(args, Config()) match {
      case Some(config) => run(config)
      case None         => System.exit(1)
    }
  }

  def run(config: Config): Unit = {
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    val appId = spark.sparkContext.applicationId

    val server = HttpServer.create(new InetSocketAddress(config.port), 0)
    server.createContext(
      "/health",
      (exchange: HttpExchange) => respond(exchange, 200, compact(render("status" -> "ok")))
    )
    server.createContext(
      "/lqp",
      (exchange: HttpExchange) => handleLqp(exchange, spark, appId, config)
    )
    // default executor = one thread, so requests are serialized (required by resetExprIds)
    server.start()
    println(s"LqpServer listening on port ${config.port} (appId $appId)")
  }

  private def handleLqp(
      exchange: HttpExchange,
      spark: SparkSession,
      appId: String,
      config: Config
  ): Unit = {
    try {
      if (exchange.getRequestMethod != "POST") {
        respond(exchange, 405, error("use POST"))
        return
      }
      val body = new String(readAll(exchange), StandardCharsets.UTF_8)
      val request = parse(body)
      val sql = optString(request \ "sql").getOrElse(
        throw new IllegalArgumentException("missing field: sql")
      )
      val database = optString(request \ "database").getOrElse("default")
      val queryId = optString(request \ "queryId").orNull
      val templateId = optString(request \ "templateId").orNull

      val session = spark.newSession()
      session.sessionState.catalog.setCurrentDatabase(database)

      // spark.sql() executes commands eagerly; only plan queries.
      session.sessionState.sqlParser.parsePlan(sql) match {
        case _: Command | _: ParsedStatement =>
          respond(exchange, 400, error("only queries are accepted, not commands/DDL/DML"))
          return
        case _ =>
      }

      if (config.resetExprIds) resetExprIds()
      val start = System.nanoTime()
      val compileTimeLqp = CompileTimeCollector.compile(session, sql)
      val elapsedMs = (System.nanoTime() - start) / 1e6

      val metadata = RunTemplateGenericConfig(queryId = queryId, templateId = templateId)
        .getJsonMetadata(appId)
      exchange.getResponseHeaders.add("X-Planning-Time-Ms", f"$elapsedMs%.1f")
      respond(exchange, 200, pretty(render(("CompileTimeLQP" -> compileTimeLqp) ~ metadata)))
    } catch {
      case e @ (_: IllegalArgumentException | _: ParseException | _: AnalysisException |
          _: org.json4s.ParserUtil.ParseException) =>
        respond(exchange, 400, error(e.getMessage))
      case e: Throwable =>
        e.printStackTrace()
        respond(exchange, 500, error(e.toString))
    }
  }

  // Expression ids (e.g. d_year#123) come from a JVM-wide counter and appear in
  // "predicate", "sign" and "rawPlan". Resetting it makes every request plan as
  // if it were the first one in a fresh JVM, like a RunTemplateGeneric run.
  private lazy val exprIdCounter: AtomicLong = {
    val cls = Class.forName("org.apache.spark.sql.catalyst.expressions.NamedExpression$")
    val module = cls.getField("MODULE$").get(null)
    val field = cls.getDeclaredField("curId")
    field.setAccessible(true)
    field.get(module).asInstanceOf[AtomicLong]
  }

  private def resetExprIds(): Unit = exprIdCounter.set(0)

  private def optString(v: JValue): Option[String] = v match {
    case JString(s)       => Some(s)
    case JNothing | JNull => None
    case other            => Some(other.extract[Any].toString)
  }

  private def error(message: String): String =
    compact(render("error" -> message))

  private def readAll(exchange: HttpExchange): Array[Byte] = {
    val in = exchange.getRequestBody
    try {
      val out = new java.io.ByteArrayOutputStream()
      val buf = new Array[Byte](8192)
      var n = in.read(buf)
      while (n != -1) { out.write(buf, 0, n); n = in.read(buf) }
      out.toByteArray
    } finally in.close()
  }

  private def respond(exchange: HttpExchange, status: Int, body: String): Unit = {
    val bytes = body.getBytes(StandardCharsets.UTF_8)
    exchange.getResponseHeaders.set("Content-Type", "application/json")
    exchange.sendResponseHeaders(status, bytes.length)
    val out = exchange.getResponseBody
    try out.write(bytes)
    finally out.close()
  }
}
