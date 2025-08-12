package edu.polytechnique.cedar.spark.benchmark.config
import org.json4s.JsonAST._
import org.json4s.JsonDSL._

case class RunTemplateGenericConfig(
    queryPath: String = null,
    databaseName: String = null,
    benchmarkId: String = null,
    queryId: String = null,
    templateId: String = null,
    traceCollectionPath: String = null,
    localDebug: Boolean = false,
    verbose: Boolean = false,
    enableRuntimeSolver: Boolean = false,
    runtimeSolverHost: String = "localhost",
    runtimeSolverPort: Int = 12345
) {
    def getJsonMetadata: JObject =
    ("Metadata" ->
        (("QueryId"    -> queryId) ~
        ("TemplateId" -> templateId))
    )
}
