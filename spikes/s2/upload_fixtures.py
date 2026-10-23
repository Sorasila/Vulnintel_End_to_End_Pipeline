"""Spike S2: สร้าง NDJSON แบบ deterministic แล้ว upload เข้า UC Volume ผ่าน Files API.

ใช้: python spikes/s2/upload_fixtures.py --case {initial,rerun,new_column,clean_source,reset}

- initial       3 ไฟล์ x 100 แถว (field cve_id, score, date ทั้งหมดเป็น JSON string)
- new_column    1 ไฟล์ x 100 แถว มี field เพิ่ม `vendor`
- clean_source  1 ไฟล์ x 10 แถว
- rerun, reset  ไม่ upload

path: /Volumes/vulnintel_dev/ops/landing/spike/YYYY/MM/DD/spike_<GITHUB_RUN_ID>-<GITHUB_RUN_ATTEMPT>_<part>.json
(วันที่ = UTC วันนี้) ใช้ overwrite=False เสมอ ต่อ RUN_ATTEMPT เพื่อให้ "Re-run jobs" ไม่ชน path เดิม

exit 0 เมื่อ upload สำเร็จหรือไม่ต้อง upload, exit 1 เมื่อ client/upload ล้ม (ไม่พิมพ์ secret)
"""

import argparse
import io
import json
import os
import sys
import time
from datetime import datetime, timezone

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

LANDING_ROOT = "/Volumes/vulnintel_dev/ops/landing/spike"

# กัน SDK ค้างเมื่อ host ใช้ไม่ได้ (ค่าเดียวกับ spike S4a)
SDK_HTTP_TIMEOUT_S = 10
SDK_RETRY_TIMEOUT_S = 20

# case -> (จำนวนไฟล์, แถวต่อไฟล์, มี field vendor ไหม, base ของเลข CVE กันซ้ำข้าม case)
PLANS = {
    "initial": (3, 100, False, 1000),
    "new_column": (1, 100, True, 8000),
    "clean_source": (1, 10, False, 9000),
    "rerun": (0, 0, False, 0),
    "reset": (0, 0, False, 0),
}


def build_rows(count: int, with_vendor: bool, base: int, part: int, day: str) -> list[dict]:
    rows = []
    for i in range(count):
        n = base + (part - 1) * count + i
        row = {
            "cve_id": f"CVE-2099-{n:05d}",
            "score": f"{(n * 37 % 100) / 10:.1f}",  # string ตั้งใจ (inferColumnTypes=false)
            "date": day,
        }
        if with_vendor:
            row["vendor"] = f"vendor-{n % 5}"
        rows.append(row)
    return rows


def to_ndjson(rows: list[dict]) -> bytes:
    return ("\n".join(json.dumps(r, separators=(",", ":")) for r in rows) + "\n").encode()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", required=True, choices=sorted(PLANS))
    case = parser.parse_args().case
    files, rows_per_file, with_vendor, base = PLANS[case]

    if files == 0:
        print(f"case={case}: no upload needed", flush=True)
        return 0

    run_id = os.environ.get("GITHUB_RUN_ID", "local")
    attempt = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
    now = datetime.now(timezone.utc)
    day = now.strftime("%Y-%m-%d")
    folder = f"{LANDING_ROOT}/{now:%Y}/{now:%m}/{now:%d}"
    paths = [f"{folder}/spike_{run_id}-{attempt}_{part}.json" for part in range(1, files + 1)]

    try:
        # Config อ่าน DATABRICKS_HOST/AUTH_TYPE/CLIENT_ID/... จาก env เอง
        ws = WorkspaceClient(
            config=Config(http_timeout_seconds=SDK_HTTP_TIMEOUT_S, retry_timeout_seconds=SDK_RETRY_TIMEOUT_S)
        )
    except Exception as exc:
        print(f"FAIL WorkspaceClient: {type(exc).__name__}: {exc}", flush=True)
        return 1

    payloads = {}
    start = time.monotonic()
    try:
        for part, path in enumerate(paths, start=1):
            body = to_ndjson(build_rows(rows_per_file, with_vendor, base, part, day))
            payloads[path] = body
            ws.files.upload(path, io.BytesIO(body), overwrite=False)
            print(f"UPLOADED {path} ({rows_per_file} rows, {len(body)} bytes)", flush=True)
    except Exception as exc:
        print(f"FAIL files.upload: {type(exc).__name__}: {exc}", flush=True)
        return 1
    print(f"case={case}: uploaded {len(paths)} file(s) in {time.monotonic() - start:.2f} s", flush=True)

    if case == "initial":
        # สังเกต "upload path ซ้ำ": overwrite=False ควร error
        dup = paths[0]
        try:
            ws.files.upload(dup, io.BytesIO(payloads[dup]), overwrite=False)
            print(f"DUPLICATE upload to {dup}: UNEXPECTED success (no exception)", flush=True)
        except Exception as exc:
            print(f"DUPLICATE upload to {dup}: {type(exc).__name__}: {exc}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
