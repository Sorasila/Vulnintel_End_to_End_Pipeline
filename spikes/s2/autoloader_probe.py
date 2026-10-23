"""Spike S2: Auto Loader (availableNow) บน serverless อ่าน NDJSON จาก UC Volume เข้า bronze.

รันเป็น spark_python_task ใน serverless job (ดู databricks.yml)
args: --case {initial,rerun,new_column,clean_source,reset} --run_id <id> [--task_run_id <id>]

ทุกบรรทัดผลพิมพ์เป็น `RESULT <text>` และเขียนลงตาราง vulnintel_dev.ops.spike_s2_results
(run_id, case, line, ts) เพื่อให้ workflow ดึงไปแสดงใน step summary

ถ้า stream ล้ม (เช่น schema เปลี่ยน) จะรายงาน RESULT ก่อนแล้ว raise ซ้ำ เพื่อให้ job retry ทำงาน
ยกเว้น case clean_source ที่ล้มเพราะ option: รายงาน accepted=False แล้วจบแบบสำเร็จ
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
TARGET_TABLE = f"{CATALOG}.bronze.spike_raw"
RESULTS_TABLE = f"{CATALOG}.ops.spike_s2_results"
LANDING_ROOT = f"/Volumes/{CATALOG}/ops/landing/spike"
SOURCE_GLOB = f"{LANDING_ROOT}/*/*/*/*.json"
CHECKPOINT = f"/Volumes/{CATALOG}/ops/_checkpoints/spike_raw"
SCHEMA_LOCATION = f"{CHECKPOINT}/_schema"

# คอลัมน์ที่ไม่ใช่ข้อมูลจาก source (ไม่ต้องเป็น string)
METADATA_COLUMNS = {"_load_dttm", "_file_path", "_filename", "_file_size", "_file_modification_time"}

RESULTS_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("case", StringType()),
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
        error_class = exc.getErrorClass()  # pyspark exceptions มี getErrorClass()
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
        # DataFrame append: สร้างตารางให้เองถ้ายังไม่มี ล้มแล้วต้องไม่บังผลหลัก
        try:
            self.spark.createDataFrame(self.rows, RESULTS_SCHEMA).write.mode("append").saveAsTable(RESULTS_TABLE)
        except Exception as exc:
            print(f"WARN could not write {RESULTS_TABLE}: {describe(exc)}", flush=True)


def table_rows(spark: SparkSession) -> int:
    return spark.table(TARGET_TABLE).count() if spark.catalog.tableExists(TARGET_TABLE) else 0


def rm_recursive(spark: SparkSession, path: str) -> str:
    try:
        from pyspark.dbutils import DBUtils

        DBUtils(spark).fs.rm(path, True)
        return "dbutils.fs.rm"
    except Exception as exc:
        print(f"WARN dbutils.fs.rm failed ({describe(exc)}); falling back to shutil.rmtree", flush=True)
        shutil.rmtree(path, ignore_errors=True)
        return "shutil.rmtree"


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
    # ลบ fixture ใน landing/spike/<YYYY> ด้วย เพื่อให้รันซ้ำ spike แล้วได้ 300 แถวอีกครั้ง
    # ไม่แตะไฟล์อื่น (เช่น auth_check_*.txt ของ S4a)
    try:
        for name in sorted(os.listdir(LANDING_ROOT)):
            if name.isdigit() and len(name) == 4:
                how = rm_recursive(spark, f"{LANDING_ROOT}/{name}")
                rep.emit(f"reset: removed {LANDING_ROOT}/{name} via {how}")
    except Exception as exc:
        rep.emit(f"reset: could not clean landing fixtures: {describe(exc)}")
    rep.emit("reset: done")


def run_stream(spark: SparkSession, case: str) -> None:
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    )
    if case == "clean_source":
        reader = reader.option("cloudFiles.cleanSource", "DELETE").option(
            "cloudFiles.cleanSource.retentionDuration", "30 days"
        )
    df = reader.load(SOURCE_GLOB).select(
        "*",
        F.current_timestamp().alias("_load_dttm"),
        F.col("_metadata.file_path").alias("_file_path"),
        F.col("_metadata.file_name").alias("_filename"),
        F.col("_metadata.file_size").alias("_file_size"),
        F.col("_metadata.file_modification_time").alias("_file_modification_time"),
    )
    query = (
        df.writeStream.outputMode("append")
        .option("mergeSchema", "true")
        .option("checkpointLocation", CHECKPOINT)
        .trigger(availableNow=True)
        .toTable(TARGET_TABLE)
    )
    query.awaitTermination()


def report_table_state(spark: SparkSession, rep: Reporter, rows_before: int) -> None:
    rows_after = table_rows(spark)
    rep.emit(f"rows_before={rows_before} rows_after={rows_after} rows_added={rows_after - rows_before}")
    rep.emit(f"total_rows={rows_after}")
    if not spark.catalog.tableExists(TARGET_TABLE):
        rep.emit(f"table {TARGET_TABLE} does not exist")
        return
    fields = spark.table(TARGET_TABLE).schema.fields
    rep.emit("columns=" + ", ".join(f"{f.name}:{f.dataType.simpleString()}" for f in fields))
    offenders = [f.name for f in fields if f.name not in METADATA_COLUMNS and not isinstance(f.dataType, StringType)]
    rep.emit(f"all_non_metadata_columns_string={not offenders}" + (f" offenders={offenders}" if offenders else ""))
    names = {f.name for f in fields}
    rep.emit(f"has_vendor={'vendor' in names}")
    rep.emit(f"has_metadata_column_named__metadata={'_metadata' in names}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", required=True, choices=["initial", "rerun", "new_column", "clean_source", "reset"])
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--task_run_id", default="")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    rep = Reporter(spark, args.run_id, args.case)
    failure: BaseException | None = None
    try:
        rep.emit(f"case={args.case} run_id={args.run_id} attempt={lookup_attempt(args.task_run_id)}")
        rep.emit(f"spark.version={spark.version} python={sys.version.split()[0]}")
        rep.emit(f"DATABRICKS_RUNTIME_VERSION={os.environ.get('DATABRICKS_RUNTIME_VERSION', '<unset>')}")

        if args.case == "reset":
            do_reset(spark, rep)
        else:
            rows_before = table_rows(spark)
            try:
                run_stream(spark, args.case)
                rep.emit("stream=SUCCESS")
                if args.case == "clean_source":
                    rep.emit("cleanSource.accepted=True (DELETE, retentionDuration=30 days)")
            except Exception as exc:
                rep.emit(f"stream=FAILED {describe(exc)}")
                if args.case == "clean_source":
                    rep.emit("cleanSource.accepted=False (see stream=FAILED above)")
                else:
                    failure = exc  # raise ซ้ำหลังรายงาน เพื่อให้ job retry
            report_table_state(spark, rep, rows_before)
    except Exception as exc:  # ความผิดพลาดนอก stream ก็ต้องไม่ทำให้ผลหาย
        rep.emit(f"probe error {describe(exc)}")
        failure = failure or exc
    finally:
        rep.flush()
    if failure is not None:
        raise failure


if __name__ == "__main__":
    main()
