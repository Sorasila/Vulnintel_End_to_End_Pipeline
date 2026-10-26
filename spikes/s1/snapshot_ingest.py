"""Spike S1: Auto Loader (availableNow) อ่าน CSV snapshot ของ assets จาก UC Volume เข้า bronze.cmdb_assets_snapshot.

รันเป็น spark_python_task ใน serverless job (ดู databricks.yml)
args: --case {seed,mutate,reset} --run_id <id> [--task_run_id <id>]

- seed/mutate  อ่านไฟล์ใหม่เข้า bronze (กฎ bronze เหมือน S2: inferColumnTypes=false, addNewColumns, _rescued_data)
               เพิ่มคอลัมน์ _load_dttm, _file_path, _filename, _file_size, _file_modification_time และ
               snapshot_id = run_id * 100 + attempt ที่แปลงจากชื่อไฟล์ cmdb_assets_<run_id>-<attempt>_0.csv
- reset        drop bronze table, ลบ checkpoint และไฟล์ landing/cmdb_assets/<YYYY> (ไม่ลบ silver: ดู S1.md)

ทุกบรรทัดผลพิมพ์เป็น `RESULT <text>` และเขียนลง vulnintel_dev.ops.spike_s1_results (run_id, case_name, line, ts)
ถ้า stream ล้ม รายงาน RESULT ก่อนแล้ว raise ซ้ำ เพื่อให้ job retry ทำงาน
"""

import argparse
import os
import shutil
import sys
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

CATALOG = "vulnintel_dev"
TARGET_TABLE = f"{CATALOG}.bronze.cmdb_assets_snapshot"
RESULTS_TABLE = f"{CATALOG}.ops.spike_s1_results"
LANDING_ROOT = f"/Volumes/{CATALOG}/ops/landing/cmdb_assets"
SOURCE_GLOB = f"{LANDING_ROOT}/*/*/*/*.csv"
CHECKPOINT = f"/Volumes/{CATALOG}/ops/_checkpoints/cmdb_assets_snapshot"
SCHEMA_LOCATION = f"{CHECKPOINT}/_schema"

# ชื่อไฟล์: cmdb_assets_<GITHUB_RUN_ID>-<GITHUB_RUN_ATTEMPT>_0.csv
FILE_NAME_PATTERN = r"cmdb_assets_([0-9]+)-([0-9]+)_"

RESULTS_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("case_name", StringType()),
        StructField("line", StringType()),
        StructField("ts", TimestampType()),
    ]
)


def short(text: object, limit: int = 600) -> str:
    one_line = " | ".join(str(text).split("\n"))
    return one_line if len(one_line) <= limit else one_line[:limit] + "...(truncated)"


def describe(exc: BaseException) -> str:
    error_class = None
    try:
        error_class = exc.getErrorClass()
    except Exception:
        pass
    prefix = f"{type(exc).__name__}" + (f" [{error_class}]" if error_class else "")
    return f"{prefix}: {short(exc)}"


class Reporter:
    def __init__(self, spark: SparkSession, run_id: str, case: str):
        self.spark, self.run_id, self.case = spark, run_id, case
        self.rows: list[tuple] = []

    def emit(self, line: str) -> None:
        print(f"RESULT {line}", flush=True)
        self.rows.append((self.run_id, self.case, line, datetime.now(timezone.utc)))

    def flush(self) -> None:
        try:
            self.spark.createDataFrame(self.rows, RESULTS_SCHEMA).write.mode("append").saveAsTable(RESULTS_TABLE)
        except Exception as exc:
            print(f"WARN could not write {RESULTS_TABLE}: {describe(exc)}", flush=True)


def rm_recursive(spark: SparkSession, path: str) -> str:
    try:
        from pyspark.dbutils import DBUtils

        DBUtils(spark).fs.rm(path, True)
        return "dbutils.fs.rm"
    except Exception as exc:
        print(f"WARN dbutils.fs.rm failed ({describe(exc)}); falling back to shutil.rmtree", flush=True)
        shutil.rmtree(path, ignore_errors=True)
        return "shutil.rmtree" + (" (FAILED: path still exists)" if os.path.exists(path) else "")


def lookup_attempt(task_run_id: str) -> str:
    try:
        from databricks.sdk import WorkspaceClient

        run = WorkspaceClient().jobs.get_run(run_id=int(task_run_id))
        return str(run.attempt_number)
    except Exception as exc:
        return f"unknown ({type(exc).__name__}: {short(exc, 120)})"


def do_reset(spark: SparkSession, rep: Reporter) -> None:
    spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
    rep.emit(f"reset: dropped table {TARGET_TABLE}")
    how = rm_recursive(spark, CHECKPOINT)
    rep.emit(f"reset: removed checkpoint dir {CHECKPOINT} via {how}")
    try:
        for name in sorted(os.listdir(LANDING_ROOT)):
            if name.isdigit() and len(name) == 4:
                how = rm_recursive(spark, f"{LANDING_ROOT}/{name}")
                rep.emit(f"reset: removed {LANDING_ROOT}/{name} via {how}")
    except FileNotFoundError:
        rep.emit(f"reset: {LANDING_ROOT} does not exist (nothing to remove)")
    except Exception as exc:
        rep.emit(f"reset: could not clean landing files: {describe(exc)}")
    rep.emit(
        "reset: silver spike_dim_asset is NOT dropped; the next `seed` runs the pipeline with full refresh "
        "(job spike_s1_snapshot_full) which rebuilds it from the new bronze"
    )
    rep.emit("reset: done")


def run_stream(spark: SparkSession) -> None:
    file_name = F.col("_metadata.file_name")
    snapshot_id = (
        F.regexp_extract(file_name, FILE_NAME_PATTERN, 1).cast("long") * F.lit(100)
        + F.regexp_extract(file_name, FILE_NAME_PATTERN, 2).cast("long")
    )
    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
        .load(SOURCE_GLOB)
        .select(
            "*",
            F.current_timestamp().alias("_load_dttm"),
            F.col("_metadata.file_path").alias("_file_path"),
            file_name.alias("_filename"),
            F.col("_metadata.file_size").alias("_file_size"),
            F.col("_metadata.file_modification_time").alias("_file_modification_time"),
            snapshot_id.alias("snapshot_id"),
        )
    )
    query = (
        df.writeStream.outputMode("append")
        .option("mergeSchema", "true")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)
        .toTable(TARGET_TABLE)
    )
    query.awaitTermination()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", required=True, choices=["seed", "mutate", "reset"])
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--task_run_id", default="")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    rep = Reporter(spark, args.run_id, args.case)
    failure: BaseException | None = None
    try:
        rep.emit(f"[ingest] case={args.case} run_id={args.run_id} attempt={lookup_attempt(args.task_run_id)}")
        rep.emit(f"[ingest] spark.version={spark.version} python={sys.version.split()[0]}")
        if args.case == "reset":
            do_reset(spark, rep)
        else:
            try:
                run_stream(spark)
                rep.emit("[ingest] stream=SUCCESS")
            except Exception as exc:
                rep.emit(f"[ingest] stream=FAILED {describe(exc)}")
                failure = exc  # raise ซ้ำหลังรายงาน เพื่อให้ job retry
    except Exception as exc:
        rep.emit(f"[ingest] probe error {describe(exc)}")
        failure = failure or exc
    finally:
        rep.flush()
    if failure is not None:
        raise failure


if __name__ == "__main__":
    main()
