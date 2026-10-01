# LQP server: logical plans over REST

A long-running Spark session that returns a query's compile-time logical plan **without executing it**. The JSON has the same format as `CompileTimeLQP` in a `RunTemplateGeneric` trace. Tables and statistics come from a copy of the cluster's Hive metastore. No data is needed.

## One-time setup

```bash
pixi install                         # JDK 8 + sbt
ln -s /Users/matematikoi/phd/repos/relevant/custom_spark/spark/assembly/target/scala-2.12/jars lib   # Spark jars (custom Spark, built with -Phive)
pixi run sbt -batch package          # -> target/scala-2.12/spark-stage-tuning_2.12-1.0-SNAPSHOT.jar
```

Build the local copy of the cluster metastore from a MySQL dump of `hivehex`:

```bash
# on the metastore host (read-only):  mysqldump -u root -p --single-transaction --skip-lock-tables hivehex > hivehex.sql
python3 spike/dump_to_export.py hivehex.sql outs/cluster/metastore-export.json
spike/import-metastore.sh outs/cluster/metastore-export.json outs/cluster/catalog
```

This creates 140 tables in `tpcds_100/200/500/1000`, `job` and `bigbench_sf_100`, with statistics and with no data on disk.

## Start the server

```bash
CATALOG=$PWD/outs/cluster/catalog spike/start-server.sh        # port 8080; ready when this prints "LqpServer listening"
curl localhost:8080/health                                     # {"status":"ok"}
```

## Example request

```bash
curl -s -X POST localhost:8080/lqp -d '{
  "database": "tpcds_100",
  "sql": "SELECT i_brand, sum(ss_ext_sales_price) FROM store_sales JOIN item ON ss_item_sk = i_item_sk WHERE i_manufact_id = 128 GROUP BY i_brand"
}'
```

Optional fields: `queryId` and `templateId`, which are copied into `Metadata`.

Response (abridged):

```json
{
  "CompileTimeLQP": {
    "LQP": {
      "operators": { "0": { "className": "...Aggregate", "rowCount": 4, "sizeInBytes": 180, "predicate": "..." }, "...": {} },
      "links": [ { "fromId": 5, "fromName": "LogicalRelation", "toId": 4, "toName": "Filter", "linkType": "Operator" } ],
      "rawPlan": "Aggregate [i_brand#31], [i_brand#31, ..."
    },
    "IM": { "inputSizeInBytes": 43863874976, "inputRowCount": 288210388 },
    "PD": {},
    "Configuration": { "theta_c": [...], "theta_p": [...], "theta_s": [...] }
  },
  "Metadata": { "QueryId": null, "TemplateId": null, "AppId": "local-..." }
}
```

## Good to know

- **Latency:** the first request takes about 2–5 s while Spark warms up. After that, a query takes about 30–40 ms. Requests are handled one at a time.
- **Only queries are accepted.** DDL and DML (`INSERT`, `DROP`, …) return `400`. Parse errors and unknown tables also return `400` with `{"error": ...}`.
- **One process per catalog:** the metastore is embedded Derby. Stop the server (`pkill -f LqpServer`) before anything else opens the same catalog.
- **Spark settings** come from `conf/defaults.conf`. Keep it the same as the trace runs, or the `Configuration` block and plans will differ.
- The cluster runs Spark 3.2.1 for most databases, and this server runs the 3.5 fork. Plan details can differ for that reason.
