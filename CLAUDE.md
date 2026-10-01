# CLAUDE.md

This file guides Claude Code when it works in this repository.

## What this repo is

This Scala/sbt project collects **query execution traces from Spark SQL**. It was originally the `spark-stage-tuning` project by Chenghao Lyu (UDAO), and this fork is maintained by Gabriel Lozano. For each query it runs, it writes one JSON file. The file holds:
- the compile-time logical plan;
- every runtime (AQE) re-optimized logical plan;
- every query stage;
- task and stage runtime metrics, IO bytes and Spark configuration.

It can optionally talk to an external "runtime solver", the UDAO server, which returns tuned Spark confs during AQE.

All source lives under `src/main/scala/edu/polytechnique/cedar/spark/`. Paths below are relative to it unless they start from the repo root.

| Dir | Role |
|---|---|
| `benchmark/` | Entry-point `main`s (`RunTemplateGeneric`, `RunTemplateQueryForRuntime`, …) and `benchmark/config/` scopt config case classes |
| `collector/` | `UdaoCollector` plus the sub-collectors that accumulate trace state |
| `listeners/` | `UDAOSparkListener`, the only `SparkListener` |
| `sql/extensions/` | Catalyst/AQE `Rule`s injected via `SparkSessionExtensions` |
| `sql/component/` | Data model (`LQPUnit`, `QSUnit`, `SGUnit`, `RunningSnapshot`, …) and `F.scala` helpers (plan export, conf get/set) |
| `udao/` | `UdaoClient`, the TCP client to the runtime solver |
| `src/main/scala/local/debug/AQETest.scala` | Scratch/debug code |

## Toolchain and libraries

- **Scala** 2.12.17 (`build.sbt`), **sbt** 1.8.2 (`project/build.properties`), and **Java 8**.
- `pixi.toml` provides `openjdk 8` and `sbt` for `linux-64` and `osx-arm64`. Use `pixi run sbt ...`. Pixi does not provide Spark.
- **Spark: a patched Spark is required.** The code calls `injectRuntimeOptimizerPrefixRule` and `injectQueryStageOptimizerPrefixRule`, and it consumes `SparkListenerOnQueryStageSubmitted`. None of these exist in stock Spark.
  - **Local (macOS): `/Users/matematikoi/phd/repos/relevant/custom_spark/spark`.** This is Apache Spark **3.5.0-SNAPSHOT** plus 4 patch commits ("add prefix rules", "inject QS rules after processing subquery and dpp", …). It is built with Maven and **must include `-Phive`**: `pixi run ./build/mvn -Phive -DskipTests -pl assembly -am package`. Its jars land in `assembly/target/scala-2.12/jars`, and `SPARK_HOME` is the checkout root.
  - The legacy cluster scripts instead expect `~/spark` → a patched `spark-3.2.1-hadoop3.3.0`.
- **`build.sbt` does not declare Spark, Hadoop or json4s**, yet the code imports `org.apache.spark.*` and `org.json4s.*`. The Spark jars come in as unmanaged jars through `lib/`, which is gitignored. Locally, `lib` is a symlink: `ln -s /Users/matematikoi/phd/repos/relevant/custom_spark/spark/assembly/target/scala-2.12/jars lib`. json4s ships inside Spark's jars.
- Declared dependencies:
  - `com.github.scopt %% scopt % 3.7.1` comes from Maven Central. It is **not** bundled in the output jar; at runtime pass `--jars $SPARK_HOME/examples/jars/scopt_2.12-3.7.1.jar`.
  - `spark-sql-perf_2.12-0.5.1-SNAPSHOT` is a local jar at `benchmark-res/libs/` (committed). You can rebuild it with `cd benchmark-res/script && bash 1.get-spark-sql-perf.sh`, which clones `Angryrou/spark-sql-perf`, runs `sbt +package` and copies the jar into `benchmark-res/libs`.
- `benchmark-res/libs/mysql-connector-j-8.0.33.jar` is used only at runtime. It goes on `spark.driver.extraClassPath` for a MySQL-backed Hive metastore during data generation.
- **Benchmark kits** (optional, only needed to generate data and queries):
  - `cd benchmark-res/script && bash 2.get-spark-tpch-kit-local.sh MACOS` clones `git@github.com:Angryrou/tpch-kit.git` (needs an SSH key) into `benchmark-res/dataset-gen/tpch-kit` and builds `dbgen`. It must be run from inside `script/`, and only `MACOS` is implemented.
  - `bash 3.get-spark-tpcds-kit-local.sh MACOS` is the same for `tpcds-kit`. The setup doc misnames it `2.get-spark-tpcds-kit-local.sh`.
- On the local Mac there is no system Java, sbt or Spark. Everything comes from pixi plus the custom Spark checkout.

## Compile

```bash
# prerequisites: `pixi install`, and the patched Spark jars linked at ./lib
pixi run sbt -batch package
# -> target/scala-2.12/spark-stage-tuning_2.12-1.0-SNAPSHOT.jar  (thin jar, no deps)
```

There is no sbt-assembly plugin and `project/` is gitignored, so there is no fat jar. Dependencies are supplied at `spark-submit` time.

## How a trace is collected

Collection uses three cooperating mechanisms, all wired up programmatically in each `Run*` main. No `spark.extraListeners` or `spark.sql.extensions` confs are involved.

```scala
val collector  = new UdaoCollector(verbose, templateId)
val udaoClient = if (enableRuntimeSolver) Some(new UdaoClient(host, port)) else None
val spark = SparkSession.builder().withExtensions { e =>
  e.injectRuntimeOptimizerPrefixRule(ExportRuntimeLogicalPlan(_, collector, debug, udaoClient))
  e.injectQueryStageOptimizerPrefixRule(ExportRuntimeQueryStage(_, collector, debug, udaoClient))
}.enableHiveSupport().getOrCreate()
spark.sparkContext.addSparkListener(UDAOSparkListener(collector))
spark.sql(s"use $db")
collector.onCompile(spark, sql)        // compile-time LQP + all confs (executionId 0, ignored by rules)
spark.sql(sql).collect()               // the traced execution: executionId == 1
spark.close()
// write collector.buildJson (+ Metadata) to <outDir>/<appName>_<appId>.json
```

1. **Compile-time** (`collector/CompileTimeCollector.scala`): `onCompile` exports `queryExecution.optimizedPlan` through `F.exportLQP`, together with all configuration (theta_c/theta_p/theta_s from `F.getAllConfiguration`).
2. **AQE rules** (`sql/extensions/`): these run synchronously on the query thread.
   - `ExportRuntimeLogicalPlan` is a `Rule[LogicalPlan]` that fires before each runtime re-optimization.
     - It acts only when `executionId == 1` and asserts `executionId <= 1`.
     - It takes a `RunningSnapshot`, exports the LQP and registers it under an incrementing `lqpId`.
     - It may also call the UDAO solver (see Endpoints).
     - It returns the plan unchanged.
   - `ExportRuntimeQueryStage` is a `Rule[SparkPlan]` that fires before each query-stage optimization.
     - It skips already-seen stages (by canonicalized plan) and reused exchanges.
     - It exports `QSMetrics` (logical and physical plan, input meta, partition distribution), allocates a `qsOptId`, and tags the plan with `TEMP_OPTIMIZED_STAGE_ORDER_TAG`. The listener later sees that tag as `optimizedStageOrder`.
   - `UpdateThetaR` is used only by `RunTemplateQueryForRuntimeTaskB`. When `lqpId == updateLqpId` it hard-sets `spark.sql.adaptive.autoBroadcastJoinThreshold=320MB`, acting as a local stand-in for the solver.
3. **Listener** (`listeners/UDAOSparkListener.scala`): see the next section.

### Trace JSON layout (`UdaoCollector.buildJson`)

```
{
  "CompileTimeLQP":  { "LQP", "IM", "PD", "Configuration" },
  "RuntimeLQPs":     { "<lqpId>": { LQP/IM/PD, RunningQueryStageSnapshot, StartTimeInMs,
                                    RuntimeConfiguration, RuntimeOptSolvingMeasure, Objectives{DurationInMs, IOBytes} } },
  "SQLStartTimeInMs", "SQLEndTimeInMs",
  "Objectives":      { "DurationInMs", "IOBytes": {IR, IW, SR, SW, ...} },
  "RuntimeQSs":      { "<qsId>": { QSLogical, QSPhysical, IM, InitialPartitionNum, PD, snapshot,
                                   QueryStageOptimizationId, RuntimeConfiguration, RelevantQueryStageIds,
                                   Objectives{DurationInMs, TotalTasksDurationInMs, IOBytes} } },
  "Metadata":        { "QueryId", "TemplateId", "AppId" }      // RunTemplateGeneric only
}
```

- `RuntimeQSs` is **silently omitted** unless the number of stage groups equals the number of query-stage indices (`sgMap.size == qsCollector.getQsIndexMapSize`).
- The legacy runners write `collector.dump2String` instead of `buildJson`. It has the same content but no `Metadata`.
- Stage groups (`SGUnit`) are sets of Spark stages that share RDD scope ids. They are joined to query stages by wave order and leaf table to compute the per-QS objectives. "Listing leaf files and directories" jobs are excluded.

## How the listener works

`UDAOSparkListener(rc: UdaoCollector)` is a stateless case class that forwards events to `UdaoCollector`, which fans them out to the sub-collectors:

| Spark event | Sub-collector(s) | Purpose |
|---|---|---|
| `onJobStart` | `RuntimeSparkStageGroupCollector` | Remember the stage ids of "Listing leaf files" jobs so they can be ignored |
| `onStageSubmitted` | `RuntimeSnapshotCollector`, `RuntimeSparkStageGroupCollector` | Book-keep the task count per stage; build stage groups from RDD scope ids |
| `onStageCompleted` | `RuntimeSparkStageGroupCollector` | Record stage start and end times and IO bytes |
| `onTaskStart` / `onTaskEnd` | `RuntimeSnapshotCollector` (+ SG on end) | Track running and finished tasks and their durations, which feed `RunningSnapshot` |
| `onOtherEvent: SparkListenerOnQueryStageSubmitted` | `RuntimeQueryStageCollector` | Build the `QSIndex` keyed by `(waveId, idInWave)` from `optimizedStageOrder` (patched-Spark event) |
| `onOtherEvent: SparkListenerSQLExecutionStart/End` | `UdaoCollector` | Record `SQLStartTimeInMs` and `SQLEndTimeInMs`, only for `executionId == 1` |

Gotchas:
- Listener events arrive asynchronously on the listener bus, while the AQE rules read collector state synchronously. The collectors use `TrieMap`s for this, but some fields are plain `var`s or `mutable.Set`s.
- **One traced SQL execution per session.** The rules assert `executionId <= 1`. `use db` is execution 0, `onCompile` doesn't execute, and the real query is execution 1. A second action would break this.
- `onSQLExecutionEnd` asserts that `errorMessage` is empty, so a failed query aborts inside the listener.

## Endpoints

There are two endpoints:
- the inbound REST endpoint of `LqpServer` (see "LqpServer" below);
- an **outbound TCP socket to the UDAO runtime solver** (`udao/UdaoClient.scala`), described in this section.

No code makes JDBC calls, and apart from `LqpServer` none makes REST calls.

- It is off by default. Enable it with `-u true`, plus `--runtimeSolverHost` (default `localhost`) and `--runtimeSolverPort` (default `12345`). These are CLI flags only, with no Spark conf or env var.
- The socket is opened eagerly in the constructor. If no server is listening, the program fails at startup with `ConnectException`. There are no timeouts, retries or reconnects. One connection is reused for the whole run and is never closed.
- Wire format:
  - Request: a 4-byte big-endian length followed by UTF-8 JSON.
  - Response: a single line of JSON read with `readLine()`, and the call blocks query optimization until it arrives.
- Requests:
  - `RequestType: "RuntimeLQP"` is sent from `ExportRuntimeLogicalPlan`. It is sent only if the plan has a `Join` and every `LogicalQueryStage` has runtime stats. Payload: `TemplateId`, `LqpId`, LQP/IM/PD, `RunningQueryStageSnapshot`, `Configuration`.
  - `RequestType: "RuntimeQS"` is sent from `ExportRuntimeQueryStage`. It is sent only for non-scan stages with at most one operator and runtime stats, whose input is larger than `max(advisoryPartitionSize, 64MB)`. Payload: `TemplateId`, `QsOptId`, `QSPhysical`, IM, `InitialPartitionNum`, PD, snapshot, `Configuration`.
- Response `{ "Configuration": {k: v}, "Measure": {k: float} }`: `F.decodeMessageAndSetconf` applies each conf with `spark.conf.set`. The measure and round-trip time are stored as `RuntimeOptSolvingMeasure` in the trace.
- `spark.yarn.historyServer.address=http://localhost:18088` appears in configs but only sets the YARN tracking URL. The History Server REST API is never called.

## Entry points (`edu.polytechnique.cedar.spark.benchmark.*`)

All of them use scopt with `opt[String]`. Booleans take an explicit value (`-u true`, `-v true`).

| Class | Collects? | Notes |
|---|---|---|
| `RunTemplateGeneric` | yes, plus `Metadata` | **Preferred / newest.** Benchmark-agnostic. Runs one arbitrary SQL file. |
| `RunTemplateQueryForRuntime` | yes | TPC-H/DS only. Query at `<-l>/<tid>/<tid>-<qid>.sql`, db `<bench>_<sf>`, `-x` out dir. |
| `RunJOBTemplateQueryForRuntime` | yes | JOB (`-b job-*`), query at `<-l>/<tid>.sql`, db `job`. |
| `RunTemplateQueryForRuntimeTaskB` | LQPs only | Uses `UpdateThetaR` with `-i updateLqpId`. No QS rule, no solver. |
| `RunTemplateQueryRaw` | no | Only times the query. |
| `SetBenchmark` | no | Generates TPC-H/DS tables via spark-sql-perf (`-b -d <kitDir> -s <SF> -l <warehouse>`). Its defaults are cluster-specific paths. |

`RunTemplateGeneric` flags:
- `-d/--databaseName` (required)
- `-t/--templateId` (required)
- `-b/--benchmarkId` (required; used only in the debug appName)
- `-q/--queryId` (required)
- `-p/--queryPath` (path to a `.sql` file, effectively required)
- `-x/--traceCollectionPath` (required; output dir, created if missing)
- `-v` verbose
- `-u` enableRuntimeSolver
- `--runtimeSolverHost`, `--runtimeSolverPort`
- ⚠️ `-d` is **also** declared for `localDebug`. Always pass the database as `--databaseName`, and set the master with `spark-submit --master`.

## Local workflow (macOS, `spike/` scripts): verified 2026-10-01

All scripts source `spike/env.sh`. It sets `SPARK_HOME` (custom Spark), `JAVA_HOME` (the pixi env), `--master local[*]` and `--properties-file conf/defaults.conf`, and points at an embedded Derby Hive metastore in `spike/catalog/` (gitignored).

```bash
pixi run sbt -batch package
spike/setup-catalog.sh /Users/matematikoi/phd/repos/query-generator/tmp_spark/database_parquet/TPCDS_0.1  # one-off
spike/trace-one.sh spike/queries/q3.sql outs/spike default 3 1   # full trace, ~10 s
spike/start-server.sh 8080                                        # LqpServer
```

- **`setup-catalog.sh`** runs `RegisterParquetTables`. For each sub-directory of the dataset it runs `CREATE TABLE … USING PARQUET LOCATION`, then `ANALYZE TABLE` with and without `FOR ALL COLUMNS`. It mirrors query-generator's `catalog_setup.py`.
  - You can't reuse query-generator's own metastore. It was created by pyspark 4.1 with Derby 10.16, and Spark 3.5's Derby 10.14 refuses to open it.
  - Never register tables inside a tracer process. Every SQL statement consumes an execution id, and the tracer requires the traced query to be execution 1.
- **Embedded Derby allows one process at a time.** Stop the server before running `trace-one.sh`, or give each its own catalog copy with `CATALOG=... spike/...`.
- **`conf/defaults.conf`** pins all 19 knobs dumped into `"Configuration"`, plus `spark.sql.cbo.enabled=true` and AQE. The tracer and the server must use the same file, or their `Configuration` sections and plans differ.

## Planning against the cluster's tables without its data

- **The cluster's metastore is a Hive 3.1.3 metastore service** (`thrift://192.168.11.7:9083`). It is backed by **MySQL** (`hivehex` database), not Derby. The cluster runs **Spark 3.2.1**.
- **Compile-time planning needs only table definitions and statistics, not data.** This was verified: with `ANALYZE` stats in the catalog, a table pointing at a missing or empty directory plans identically. `hdfs://` locations must not be left in place, because Spark tries to contact the HDFS server.
- **Workflow used (2026-10-01): nothing runs on the server except one read-only dump.** Never ssh on the user's behalf.
  1. The user ran `mysqldump -u root -p --single-transaction --skip-lock-tables hivehex > hivehex.sql` on 192.168.11.7. The dump lives at `/Users/matematikoi/phd/repos/relevant/custom_spark/hive/hivehex.sql` (34 MB).
  2. `python3 spike/dump_to_export.py hivehex.sql outs/cluster/metastore-export.json [--dbs a,b]` parses the dump directly, with no MySQL (`spike/hive_dump.py`). It takes about 2 s.
  3. `spike/import-metastore.sh outs/cluster/metastore-export.json outs/cluster/catalog` (`ImportMetastore`) took 43 s for 140 tables and ~48k partitions.
  4. `CATALOG=$PWD/outs/cluster/catalog spike/start-server.sh`
- **What the dump contains:** `bigbench_sf_100` (23 Hive ORC tables), `job` (21 parquet tables, created by 3.5.0-SNAPSHOT), and `tpcds_100/200/500/1000` (24 tables each, 7 of them partitioned by `*_date_sk`, ~12k partitions per database). Every table has Spark stats except `tpcds_200.ship_mode` and `tpcds_200.web_returns`.
- **Why there are placeholder files:** partitions carry only Hive `numFiles`/`totalSize`, and Spark sizes pruned partitions by *listing files*. The importer therefore writes one **sparse** `part-00000.placeholder` per table/partition directory, as large as the original data. That is 585 GB apparent and 0 bytes on disk. Planning never opens these files, but anything that tries to read them will fail.
- **`tracksPartitionsInCatalog` must be preserved** (`spark.sql.partitionProvider=catalog` on the server). Without it, Spark discovers partitions from directories and does not prune them via the metastore.
- An alternative path, if the user ever agrees to run Spark on the server: `spike/export-metastore.scala` in `spark-shell -i` produces the same JSON. It was verified 3.5→3.5: q3 plus 64 queries gave identical plans from an imported, data-less catalog.
- **Not verified against the server:** no server-side plans were available to diff. The server runs Spark 3.2.1 for most databases.

## LqpServer: compile-time plans over REST (`server/LqpServer.scala`)

`LqpServer` is a long-lived Spark session that plans queries without executing them. It returns `{"CompileTimeLQP": …, "Metadata": …}`, the same format as those sections of a `RunTemplateGeneric` trace.

- It uses the JDK's built-in `com.sun.net.httpserver.HttpServer`. Avoid Scala HTTP libraries, whose Jackson and Netty versions clash with Spark's.
- `POST /lqp` takes `{"sql": "...", "database": "default", "queryId": "1", "templateId": "3"}`. Only `sql` is required. `GET /health` is a liveness check.
- The response has the header `X-Planning-Time-Ms`. Errors return `{"error": ...}`:
  - 400 for a non-query, a parse error or an analysis error;
  - 405 for a non-POST request;
  - 500 for anything else.
- Each request runs in `spark.newSession()`, so the current database and confs are isolated per request. The catalog is shared.
- Commands, DDL and DML (`Command` / `ParsedStatement`) are rejected before planning, because `spark.sql` executes those immediately.
- The compile-time JSON comes from `CompileTimeCollector.compile`, the same function the tracer's `onCompile` uses. **Keep the two on that one function.**
- **Expression ids:**
  - `predicate`, `sign` and `rawPlan` embed expression ids such as `d_year#9L`. These come from a counter shared by the whole JVM (`NamedExpression.curId`).
  - The server resets that counter to 0 through reflection before each request (`--resetExprIds true`, the default). The default single-threaded executor serializes requests, which the reset needs.
  - Without the reset, the output diverges from the tracer from the first request on: the request-validation parse itself allocates ids.
- **Spike result (TPC-DS SF0.1, q3):**
  - `CompileTimeLQP` was byte-identical to the tracer's on requests 1 and 5.
  - Planning took 1.7 s cold, then about 0.1 s warm.

## Run the project / collect ONE trace

Prerequisites:
1. A patched Spark with Hive support is installed (`$SPARK_HOME`); see Toolchain. The local workflow above covers this on the Mac.
2. The jar is built (see Compile).
3. A database exists in the Spark/Hive catalog with the benchmark tables. For TPC-H, generate it with `SetBenchmark` as described in `benchmark-res/spark-benchmark-setup-local.md`. That needs HDFS at `localhost:8020` and the mysql connector for the metastore. For JOB, see `benchmark-res/script/job-benchmark/create-job-over-spark.md`.
4. A SQL file exists. For TPC-H, `bash benchmark-res/script/2.get-spark-tpch-queries.sh $PWD/benchmark-res/dataset-gen/tpch-kit <queriesPerTemplate> <SF>` writes `<kit>/spark-sqls/<t>/<t>-<i>.sql`.

Run one query with the generic runner:

```bash
$SPARK_HOME/bin/spark-submit \
  --class edu.polytechnique.cedar.spark.benchmark.RunTemplateGeneric \
  --master 'local[*]' --deploy-mode client --name tpch_1-1 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --jars $SPARK_HOME/examples/jars/scopt_2.12-3.7.1.jar,$PWD/benchmark-res/libs/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar \
  target/scala-2.12/spark-stage-tuning_2.12-1.0-SNAPSHOT.jar \
  -d tpch_1 -b tpch -t 1 -q 1 \
  -p $PWD/benchmark-res/dataset-gen/tpch-kit/spark-sqls/1/1-1.sql \
  -x $PWD/outs/test
# add `-u true` (and a running UDAO server on localhost:12345) to enable the runtime solver
```

- **Output:** `outs/test/<appName>_<appId>.json`, for example `tpch_1-1_local-1696...json`. `outs/` is gitignored.
- **Stdout:** the app id, `run <path>`, and one `QueryStageId: … OptimizationOrder: … RelevantStages: … table: …` line per query stage.
- **Check the result:** `jq 'keys, .Metadata, .Objectives' outs/test/*.json`. If `RuntimeQSs` is missing, the SG/QS count mismatch occurred.

### Cluster scripts (`benchmark-run/script-tpch/*.sh`)

- `run-tpch-q-{raw,with-runtime,wo-runtime}.sh` wrap `spark-submit` with `--master yarn`, many executor and AQE knobs (`-c`, `-p`, `-s` vectors), `-q "<tid> <qid>"`, `-n name` and `-x outdir`. They fix the scale factor at SF100.
- The `run-task*`/`run-colleration.sh` scripts are parameter sweeps. They `ssh node2..6` and call a `run-tpch-q.sh` that is not in the repo.
- Hard-coded paths to change before reuse: `/opt/hex_users/$USER/chenghao/...`, `~/spark`, `spark.yarn.*`, `-Xms20g`, and the HDFS hosts `node1-opa`/`node13-opa`. The setup doc also uses `/Users/chenghao/...`.
- `src/main/resources/log4j2.properties` is actually log4j1 syntax at ERROR level. It is passed with `--driver-java-options -Dlog4j.configuration=file:...`.

### Visualization

`python/visualize/1.plot_logical_plan.py -o <dir> -f <file>.json` plots an LQP graph with networkx and pydot (needs graphviz). It reads a top-level `"LQP"` key, but traces nest it as `CompileTimeLQP.LQP` or `RuntimeLQPs.<id>.LQP`, so the script is stale. It also has a stray Scala `import` line.

## Known quirks

- In the `localDebug` branch, `spark.sql.adaptive.enable` is a typo for `enabled`. AQE is on by default since Spark 3.2, so this is mostly harmless.
- `RunTemplateQueryForRuntime` prints `spark.yarn.historyServer.address`, which throws if that conf is unset. The generic runner has this line commented out.
- `.gitignore` lists `benchmark-res/libs`, `lib` and `project`, yet the two jars in `benchmark-res/libs` are tracked.
