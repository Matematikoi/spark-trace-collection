package edu.polytechnique.cedar.spark.collector

import edu.polytechnique.cedar.spark.sql.component.F
import org.apache.spark.sql.SparkSession
import org.json4s.JsonAST
import org.json4s.JsonDSL._

class CompileTimeCollector {

  private var compileTimeJson: Option[JsonAST.JObject] = None

  def onCompile(spark: SparkSession, queryContent: String): Unit = {
    compileTimeJson = Some(CompileTimeCollector.compile(spark, queryContent))
  }

  def exposeJson: JsonAST.JObject = compileTimeJson.get
}

object CompileTimeCollector {

  // Shared by the trace collector and LqpServer so both emit the same "CompileTimeLQP" format.
  def compile(spark: SparkSession, queryContent: String): JsonAST.JObject = {
    val thetaMap = F.getAllConfiguration(spark)
    val plan = spark.sql(queryContent).queryExecution.optimizedPlan
    val lqpUnit = F.exportLQP(plan)
    lqpUnit.json ~
      ("Configuration" -> thetaMap.map(x => (x._1, x._2.toList)))
  }
}
