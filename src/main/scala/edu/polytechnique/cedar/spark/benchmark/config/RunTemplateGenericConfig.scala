package edu.polytechnique.cedar.spark.benchmark.config

case class RunTemplateGenericConfig(
    queryLocation: String = null,
    databaseName: String = null, // tpch_100, tpcds_100, job
    queryName: String = null,
    templateName: String = null,
    traceCollectionPath: String = null,
    localDebug: Boolean = false,
    verbose: Boolean = false,
    enableRuntimeSolver: Boolean = false,
    runtimeSolverHost: String = "localhost",
    runtimeSolverPort: Int = 12345
)
