"""Spike S1: dump ตาราง assets จาก Neon เป็น CSV (ทุกคอลัมน์เป็น text, มี header) แล้ว upload เข้า UC Volume.

ใช้: python spikes/s1/snapshot_upload.py --case {seed,mutate,reset}   (reset = ไม่ทำอะไร)

path: /Volumes/vulnintel_dev/ops/landing/cmdb_assets/YYYY/MM/DD/cmdb_assets_<GITHUB_RUN_ID>-<GITHUB_RUN_ATTEMPT>_0.csv
(วันที่ = UTC วันนี้) ใช้ Files API overwrite=False (ผ่านการพิสูจน์ใน S2) ไฟล์เดียวต่อ run = 1 snapshot
snapshot_id ที่ ingest คำนวณจากชื่อไฟล์ = run_id * 100 + attempt (เพิ่มขึ้นเสมอ)
ค่า NULL ถูกเขียนเป็นช่องว่าง (Spark อ่านกลับเป็น null)

exit 0 เมื่อสำเร็จหรือไม่ต้องทำ, 1 เมื่อล้ม (ไม่พิมพ์ connection string)
"""

import argparse
import csv
import io
import os
import sys
import time
from datetime import datetime, timezone

import psycopg
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

LANDING_ROOT = "/Volumes/vulnintel_dev/ops/landing/cmdb_assets"
CONNECT_TIMEOUT_S = 15
SDK_HTTP_TIMEOUT_S = 10
SDK_RETRY_TIMEOUT_S = 20


def scrub(text: str, url: str) -> str:
    out = text.replace(url, "<NEON_DATABASE_URL>")
    try:
        password = psycopg.conninfo.conninfo_to_dict(url).get("password")
    except Exception:
        password = None
    return out.replace(password, "***") if password else out


def dump_assets_csv(url: str) -> tuple[bytes, int]:
    with psycopg.connect(url, connect_timeout=CONNECT_TIMEOUT_S) as conn:
        with conn.cursor() as cur:
            cur.execute("SET TIME ZONE 'UTC'")  # ให้ timestamptz::text คงที่ทุกรอบ
            cur.execute(
                "SELECT column_name FROM information_schema.columns"
                " WHERE table_schema = 'public' AND table_name = 'assets' ORDER BY ordinal_position"
            )
            columns = [r[0] for r in cur.fetchall()]
            if not columns:
                raise RuntimeError("table public.assets not found: run case=seed first")
            select_list = ", ".join(f'"{c}"::text AS "{c}"' for c in columns)
            cur.execute(f"SELECT {select_list} FROM public.assets ORDER BY id")
            rows = cur.fetchall()
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(columns)
    writer.writerows([["" if v is None else v for v in row] for row in rows])
    return buf.getvalue().encode("utf-8"), len(rows)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", required=True, choices=["seed", "mutate", "reset"])
    case = parser.parse_args().case
    if case == "reset":
        print("case=reset: no snapshot upload needed", flush=True)
        return 0

    url = os.environ.get("NEON_DATABASE_URL", "")
    if not url:
        print("FAIL: environment variable NEON_DATABASE_URL is missing or empty (check GitHub Secrets)", flush=True)
        return 1

    try:
        body, n_rows = dump_assets_csv(url)
    except Exception as exc:
        print(f"FAIL dump assets: {type(exc).__name__}: {scrub(str(exc), url)}", flush=True)
        return 1
    print(f"dumped assets: {n_rows} rows, {len(body)} bytes", flush=True)

    run_id = os.environ.get("GITHUB_RUN_ID", "local")
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    now = datetime.now(timezone.utc)
    path = f"{LANDING_ROOT}/{now:%Y}/{now:%m}/{now:%d}/cmdb_assets_{run_id}-{attempt}_0.csv"
    start = time.monotonic()
    try:
        ws = WorkspaceClient(
            config=Config(http_timeout_seconds=SDK_HTTP_TIMEOUT_S, retry_timeout_seconds=SDK_RETRY_TIMEOUT_S)
        )
        ws.files.upload(path, io.BytesIO(body), overwrite=False)
    except Exception as exc:
        print(f"FAIL files.upload {path}: {type(exc).__name__}: {exc}", flush=True)
        return 1
    print(f"UPLOADED {path} ({n_rows} rows) in {time.monotonic() - start:.2f} s", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
