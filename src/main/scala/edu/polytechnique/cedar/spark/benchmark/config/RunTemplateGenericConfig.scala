package edu.polytechnique.cedar.spark.benchmark.config

case class RunTemplateGenericConfig(
    queryPath: String = null,
    databaseName: String = null, // tpch_100, tpcds_100, job
    benchmarkId: String = null, //snowflake, ceb, etc ...
    queryId: String = null,
    templateName: String = null,
    traceCollectionPath: String = null,
    localDebug: Boolean = false,
    verbose: Boolean = false,
    enableRuntimeSolver: Boolean = false,
    runtimeSolverHost: String = "localhost",
    runtimeSolverPort: Int = 12345
)
