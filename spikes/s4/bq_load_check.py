"""Spike S4b: GitHub Actions (Workload Identity Federation) load Parquet เข้า BigQuery sandbox
และตรวจว่า WRITE_TRUNCATE reset วันหมดอายุ 60 วันของตารางหรือไม่.

MODE (env `MODE`, default `load`):
  load           สร้าง Parquet 10 แถว แล้ว load ด้วย WRITE_TRUNCATE ไป vulnintel_spike.t1
  extend_expiry  load แล้ว update_table(["expires"]) เป็น now+59 วัน (ทาง ก)
  recreate       delete_table(not_found_ok=True) ก่อน load (ทาง ข)

พิมพ์ผลเป็นบรรทัด `RESULT key=value` (workflow เก็บไปไว้ใน Job summary) ไม่พิมพ์ token/credentials
exit code 1 ถ้า auth/load ล้ม ส่วน extend_expiry/delete_table ที่ sandbox ปฏิเสธเป็น "ผล" ของ spike
จึงพิมพ์ error class + message แล้ว exit 0
"""

import io
import os
import sys
from datetime import datetime, timedelta, timezone

import google.auth
import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

DATASET = "vulnintel_spike"
TABLE = "t1"
LOCATION = "US"
MODES = ("load", "extend_expiry", "recreate")
EXTEND_DAYS = 59

# ทุก call ไป BigQuery ต้องมี timeout (วินาที)
API_TIMEOUT_S = 30
UPLOAD_TIMEOUT_S = 120
JOB_TIMEOUT_S = 180


def result(key: str, value: object) -> None:
    print(f"RESULT {key}={value}", flush=True)


def build_parquet() -> bytes:
    """Parquet 10 แถว: int64, string, timestamp(UTC), double."""
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    table = pa.table(
        {
            "id": pa.array(range(1, 11), type=pa.int64()),
            "cve_id": pa.array([f"CVE-2026-{1000 + i}" for i in range(10)], type=pa.string()),
            "published_at": pa.array([base + timedelta(days=i) for i in range(10)], type=pa.timestamp("us", tz="UTC")),
            "cvss_score": pa.array([round(1.0 + i * 0.9, 1) for i in range(10)], type=pa.float64()),
        }
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def days_left(expires: datetime | None, now: datetime) -> str:
    if expires is None:
        return "none"
    return f"{(expires - now).total_seconds() / 86400:.3f}"


def report_table(prefix: str, table: bigquery.Table) -> None:
    now = datetime.now(timezone.utc)
    result(f"{prefix}.created", table.created.isoformat() if table.created else None)
    result(f"{prefix}.modified", table.modified.isoformat() if table.modified else None)
    result(f"{prefix}.expires", table.expires.isoformat() if table.expires else None)
    result(f"{prefix}.expires_minus_now_days", days_left(table.expires, now))
    result(f"{prefix}.num_rows", table.num_rows)


def get_table_or_none(client: bigquery.Client, table_id: str) -> bigquery.Table | None:
    try:
        return client.get_table(table_id, timeout=API_TIMEOUT_S)
    except NotFound:
        return None


def auth_principal(credentials: object) -> str:
    # external_account (WIF) ที่ impersonate SA มี service_account_email; ไม่พิมพ์ token
    email = getattr(credentials, "service_account_email", None)
    return f"{email} (credentials={type(credentials).__name__})"


def main() -> int:
    mode = os.environ.get("MODE", "load")
    if mode not in MODES:
        print(f"FAIL invalid MODE={mode!r}; expected one of {MODES}", flush=True)
        return 1

    result("mode", mode)
    result("github_run_id", os.environ.get("GITHUB_RUN_ID", "local"))
    result("github_run_attempt", os.environ.get("GITHUB_RUN_ATTEMPT", "1"))

    credentials, default_project = google.auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])
    project = os.environ.get("GCP_PROJECT_ID") or default_project
    if not project:
        print("FAIL GCP_PROJECT_ID is missing and google.auth.default() returned no project", flush=True)
        return 1
    result("project", project)
    result("auth_principal", auth_principal(credentials))

    client = bigquery.Client(project=project, credentials=credentials, location=LOCATION)
    table_id = f"{project}.{DATASET}.{TABLE}"

    dataset = client.get_dataset(f"{project}.{DATASET}", timeout=API_TIMEOUT_S)
    result("dataset.location", dataset.location)
    result("dataset.default_table_expiration_ms", dataset.default_table_expiration_ms)

    before = get_table_or_none(client, table_id)
    if before is None:
        result("before.exists", False)
    else:
        result("before.exists", True)
        report_table("before", before)

    if mode == "recreate":
        try:
            client.delete_table(table_id, not_found_ok=True, timeout=API_TIMEOUT_S)
            result("delete_table", "OK")
        except Exception as exc:  # sandbox ปฏิเสธ = ผลของ spike
            result("delete_table", f"REFUSED {type(exc).__name__}: {exc}")

    data = build_parquet()
    result("parquet_bytes", len(data))
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_file(io.BytesIO(data), table_id, job_config=job_config, timeout=UPLOAD_TIMEOUT_S)
    job.result(timeout=JOB_TIMEOUT_S)
    result("load_job", f"{job.job_id} state={job.state} output_rows={job.output_rows}")

    after = client.get_table(table_id, timeout=API_TIMEOUT_S)
    report_table("after", after)
    if before is not None and before.expires is not None and after.expires is not None:
        result("expires_changed_by_load", after.expires != before.expires)
        result("expires_delta_seconds", f"{(after.expires - before.expires).total_seconds():.0f}")

    if mode == "extend_expiry":
        target = datetime.now(timezone.utc) + timedelta(days=EXTEND_DAYS)
        after.expires = target
        try:
            updated = client.update_table(after, ["expires"], timeout=API_TIMEOUT_S)
            result("extend_expiry", "OK")
            report_table("extended", updated)
        except Exception as exc:  # sandbox ปฏิเสธ/จำกัดที่ 60 วัน = ผลของ spike
            result("extend_expiry", f"REFUSED {type(exc).__name__}: {exc}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
