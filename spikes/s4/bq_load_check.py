"""Spike S4b: GitHub Actions (Workload Identity Federation) load Parquet เข้า BigQuery sandbox
และตรวจว่า WRITE_TRUNCATE reset วันหมดอายุ 60 วันของตารางหรือไม่.

MODE (env `MODE`, default `load`):
  load           สร้าง Parquet 10 แถว แล้ว load ด้วย WRITE_TRUNCATE ไป vulnintel_spike.t1
  extend_expiry  load แล้ว update_table(["expires"]) เลื่อนไป expires+1 วัน (ทาง ก) ดูว่า sandbox ยอมเลื่อนหรือ cap
  recreate       delete_table(not_found_ok=True) ก่อน load (ทาง ข)

พิมพ์ผลเป็นบรรทัด `RESULT key=value` (workflow เก็บไปไว้ใน Job summary) ไม่พิมพ์ token/credentials
exit code 1 ถ้า auth/load ล้ม หรือเกิด error ที่ไม่ใช่การปฏิเสธของ sandbox
extend_expiry/delete_table ที่ถูกปฏิเสธด้วย Forbidden/BadRequest/PermissionDenied เป็น "ผล" ของ spike
พิมพ์ `REFUSED <class>: <message>` แล้ว exit 0 ส่วน exception อื่นพิมพ์ `ERROR ...` และ exit 1
"""

import io
import os
import sys
from datetime import datetime, timedelta, timezone

import google.auth
import pyarrow as pa
import pyarrow.parquet as pq
from google.api_core.exceptions import BadRequest, Forbidden, NotFound, PermissionDenied
from google.cloud import bigquery

DATASET = "vulnintel_spike"
TABLE = "t1"
LOCATION = "US"
MODES = ("load", "extend_expiry", "recreate")
EXTEND_DAYS_NO_EXPIRY = 61  # ใช้เมื่อตารางไม่มี expires: ตั้ง now+61 วัน (เกิน 60 วันของ sandbox)

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


def classify(exc: Exception) -> str:
    """REFUSED = sandbox ปฏิเสธ (เป็นผลของ spike), ERROR = ล้มด้วยเหตุอื่น (ต้อง exit non-zero)."""
    return "REFUSED" if isinstance(exc, (Forbidden, BadRequest, PermissionDenied)) else "ERROR"


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

    had_error = False

    if mode == "recreate":
        try:
            client.delete_table(table_id, not_found_ok=True, timeout=API_TIMEOUT_S)
            result("delete_table", "OK")
        except Exception as exc:
            label = classify(exc)
            had_error = had_error or label == "ERROR"
            result("delete_table", f"{label} {type(exc).__name__}: {exc}")

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

    # เทียบ expires ก่อน/หลัง load (พิมพ์เสมอ รวมกรณี None); recreate ใช้ชื่อ key ต่างกัน
    key = "expires_changed_by_recreate" if mode == "recreate" else "expires_changed_by_load"
    if before is None:
        result(key, "n/a (no table before)")
        result("expires_delta_seconds", "n/a")
    else:
        result(key, after.expires != before.expires)
        if before.expires is not None and after.expires is not None:
            result("expires_delta_seconds", f"{(after.expires - before.expires).total_seconds():.0f}")
        else:
            result("expires_delta_seconds", f"n/a (before={before.expires} after={after.expires})")

    if mode == "extend_expiry":
        # ต้องเป็นการเลื่อนจริง: ตั้งเป็น expires ปัจจุบัน + 1 วัน (now+59d จะเป็นการย่อ เพราะ expires อยู่ราว created+60d)
        original = after.expires
        if original is not None:
            target = original + timedelta(days=1)
        else:
            target = datetime.now(timezone.utc) + timedelta(days=EXTEND_DAYS_NO_EXPIRY)
        # BigQuery เก็บ expirationTime เป็น millisecond: ตัด microsecond ก่อนเทียบว่าถูก cap ไหม
        target = target.replace(microsecond=(target.microsecond // 1000) * 1000)
        result("extend_original", original.isoformat() if original else None)
        result("extend_target", target.isoformat())
        after.expires = target
        try:
            updated = client.update_table(after, ["expires"], timeout=API_TIMEOUT_S)
            result("extend_expiry", "OK")
            report_table("extended", updated)
            result("extend_capped", updated.expires != target)
            if original is not None and updated.expires is not None:
                result("extend_delta_seconds", f"{(updated.expires - original).total_seconds():.0f}")
            else:
                result("extend_delta_seconds", "n/a")
        except Exception as exc:  # REFUSED/capped = ผลของ spike; ERROR = ล้มด้วยเหตุอื่น
            label = classify(exc)
            had_error = had_error or label == "ERROR"
            result("extend_expiry", f"{label} {type(exc).__name__}: {exc}")

    return 1 if had_error else 0


if __name__ == "__main__":
    sys.exit(main())
