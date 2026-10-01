"""Convert a mysqldump of a Hive metastore (e.g. hivehex.sql) into metastore-export.json
for ImportMetastore, without MySQL and without running anything on the server.

usage: python3 spike/dump_to_export.py <hivehex.sql> <metastore-export.json> [--dbs db1,db2]

Besides schema and statistics, every table/partition gets "fileSize": the bytes
its directory held on the cluster (Hive's totalSize). The importer recreates that
size as a sparse placeholder file, because Spark sizes pruned partitions by
listing files, not from the metastore.
"""
import argparse
import collections
import json
from pathlib import Path

import hive_dump

NEEDED = {"DBS", "TBLS", "TABLE_PARAMS", "SDS", "SERDES", "SERDE_PARAMS", "COLUMNS_V2",
          "PARTITIONS", "PARTITION_PARAMS", "PARTITION_KEYS", "PARTITION_KEY_VALS"}
STATS = "spark.sql.statistics."
COL_STATS = STATS + "colStats."
SCHEMA = "spark.sql.sources.schema"


def grouped(rows, key):
    out = collections.defaultdict(list)
    for r in rows:
        out[r[key]].append(r)
    return out


def params(rows, id_key):
    out = collections.defaultdict(dict)
    for r in rows:
        out[r[id_key]][r["PARAM_KEY"]] = r["PARAM_VALUE"]
    return out


def numbered(p, count_key, item_key):
    return [p[f"{item_key}.{i}"] for i in range(int(p.get(count_key, 0)))]


def stats(p):
    if STATS + "totalSize" not in p:
        return None
    cols = collections.defaultdict(dict)
    for k, v in p.items():
        if k.startswith(COL_STATS):
            rest = k[len(COL_STATS):]
            cols[rest.rsplit(".", 1)[0]][rest] = v
    return {"sizeInBytes": p[STATS + "totalSize"], "rowCount": p.get(STATS + "numRows"),
            "colStats": dict(sorted(cols.items()))}


def file_size(p):
    size = p.get("totalSize", p.get(STATS + "totalSize"))
    return int(size) if size is not None else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("dump")
    ap.add_argument("out")
    ap.add_argument("--dbs", help="comma-separated databases (default: all with tables)")
    args = ap.parse_args()

    d = hive_dump.load(args.dump, NEEDED)
    dbs = {r["DB_ID"]: r["NAME"] for r in d["DBS"]}
    wanted = set(args.dbs.split(",")) if args.dbs else None
    sds = {r["SD_ID"]: r for r in d["SDS"]}
    serdes = {r["SERDE_ID"]: r for r in d["SERDES"]}
    serde_params = params(d.get("SERDE_PARAMS", []), "SERDE_ID")
    table_params = params(d.get("TABLE_PARAMS", []), "TBL_ID")
    part_params = params(d.get("PARTITION_PARAMS", []), "PART_ID")
    columns = grouped(d.get("COLUMNS_V2", []), "CD_ID")
    part_keys = grouped(d.get("PARTITION_KEYS", []), "TBL_ID")
    part_vals = grouped(d.get("PARTITION_KEY_VALS", []), "PART_ID")
    partitions = grouped(d.get("PARTITIONS", []), "TBL_ID")

    tables = []
    for t in sorted(d["TBLS"], key=lambda t: (dbs[t["DB_ID"]], t["TBL_NAME"])):
        db = dbs[t["DB_ID"]]
        if (wanted and db not in wanted) or t["TBL_TYPE"] == "VIRTUAL_VIEW":
            continue
        p = table_params[t["TBL_ID"]]
        sd = sds[t["SD_ID"]]
        keys = [k["PKEY_NAME"] for k in sorted(part_keys[t["TBL_ID"]], key=lambda k: k["INTEGER_IDX"])]
        part_cols = numbered(p, SCHEMA + ".numPartCols", SCHEMA + ".partCol") or keys

        if SCHEMA in p or SCHEMA + ".numParts" in p:
            schema = json.loads(p.get(SCHEMA) or "".join(numbered(p, SCHEMA + ".numParts", SCHEMA + ".part")))
            ddl = None
        else:  # Hive-native table: columns as Hive types, partition columns last
            cols = sorted(columns[sd["CD_ID"]], key=lambda c: c["INTEGER_IDX"])
            pcols = sorted(part_keys[t["TBL_ID"]], key=lambda k: k["INTEGER_IDX"])
            schema = None
            ddl = ", ".join([f"`{c['COLUMN_NAME']}` {c['TYPE_NAME']}" for c in cols] +
                            [f"`{k['PKEY_NAME']}` {k['PKEY_TYPE']}" for k in pcols])

        num_buckets = p.get(SCHEMA + ".numBuckets")
        parts_out = []
        for part in sorted(partitions[t["TBL_ID"]], key=lambda x: x["PART_NAME"]):
            vals = [v["PART_KEY_VAL"] for v in sorted(part_vals[part["PART_ID"]], key=lambda v: v["INTEGER_IDX"])]
            pp = part_params[part["PART_ID"]]
            parts_out.append({
                "spec": dict(zip(keys, vals)),
                "location": sds[part["SD_ID"]]["LOCATION"],
                "parameters": pp,
                "stats": stats(pp),
                "fileSize": file_size(pp),
            })

        serde_id = sd["SERDE_ID"]
        tables.append({
            "database": db,
            "table": t["TBL_NAME"],
            "tableType": "EXTERNAL" if t["TBL_TYPE"] == "EXTERNAL_TABLE" else "MANAGED",
            "provider": p.get("spark.sql.sources.provider", "hive"),
            "schema": schema,
            "ddlSchema": ddl,
            "partitionColumnNames": part_cols,
            "tracksPartitionsInCatalog": p.get("spark.sql.partitionProvider") == "catalog",
            "bucketSpec": None if num_buckets is None else {
                "numBuckets": int(num_buckets),
                "bucketColumnNames": numbered(p, SCHEMA + ".numBucketCols", SCHEMA + ".bucketCol"),
                "sortColumnNames": numbered(p, SCHEMA + ".numSortCols", SCHEMA + ".sortCol"),
            },
            "location": sd["LOCATION"],
            "inputFormat": sd["INPUT_FORMAT"],
            "outputFormat": sd["OUTPUT_FORMAT"],
            "serde": serdes[serde_id]["SLIB"] if serde_id in serdes else None,
            "compressed": False,
            "storageProperties": serde_params.get(serde_id, {}),
            "properties": p,
            "stats": stats(p),
            "fileSize": None if parts_out else file_size(p),
            "partitions": parts_out,
        })

    Path(args.out).write_text(json.dumps({"source": Path(args.dump).name, "tables": tables}, indent=1))
    per_db = collections.Counter(t["database"] for t in tables)
    print(f"wrote {len(tables)} tables to {args.out}: {dict(per_db)}")
    for t in tables:
        if t["stats"] is None:
            print(f"  no Spark statistics: {t['database']}.{t['table']}")


if __name__ == "__main__":
    main()
