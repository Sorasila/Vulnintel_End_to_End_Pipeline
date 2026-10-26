"""Spike S1: ดึง RESULT lines ของ run หนึ่งจาก vulnintel_dev.ops.spike_s1_results.

พิมพ์ลง stdout และเขียนลง $GITHUB_STEP_SUMMARY (ถ้ามี) เพื่อให้คัดลอกผลได้จากหน้า run
auth ใช้ pattern เดียวกับ spikes/s4/dbx_auth_check.py (OAuth M2M ผ่าน credentials_provider)
usage: python spikes/s1/fetch_results.py --run_id <github_run_id>-<attempt> --case <case>
exit 1 ถ้า query ล้ม หรือไม่พบแถวของ run นี้
"""

import argparse
import os
import sys

from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config, oauth_service_principal

RESULTS_TABLE = "vulnintel_dev.ops.spike_s1_results"
SDK_HTTP_TIMEOUT_S = 10
SDK_RETRY_TIMEOUT_S = 20
SQL_SOCKET_TIMEOUT_S = 60
SQL_RETRY_STOP_AFTER_S = 300


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"environment variable {name} is missing or empty (check GitHub Secrets)")
    return value


def fetch_lines(run_id: str) -> list[tuple]:
    host = require_env("DATABRICKS_HOST").removeprefix("https://").removeprefix("http://").rstrip("/")
    client_id = require_env("DATABRICKS_CLIENT_ID")
    client_secret = require_env("DATABRICKS_CLIENT_SECRET")

    def credentials_provider():
        config = Config(
            host=f"https://{host}",
            client_id=client_id,
            client_secret=client_secret,
            http_timeout_seconds=SDK_HTTP_TIMEOUT_S,
            retry_timeout_seconds=SDK_RETRY_TIMEOUT_S,
        )
        return oauth_service_principal(config)

    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID")
    if not warehouse_id:
        ws = WorkspaceClient(
            config=Config(http_timeout_seconds=SDK_HTTP_TIMEOUT_S, retry_timeout_seconds=SDK_RETRY_TIMEOUT_S)
        )
        warehouse_id = next(iter(ws.warehouses.list())).id

    with sql.connect(
        server_hostname=host,
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=credentials_provider,
        _socket_timeout=SQL_SOCKET_TIMEOUT_S,
        _retry_stop_after_attempts_duration=SQL_RETRY_STOP_AFTER_S,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT ts, line FROM {RESULTS_TABLE} WHERE run_id = :run_id ORDER BY ts",
                {"run_id": run_id},
            )
            return cursor.fetchall()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--case", required=True)
    args = parser.parse_args()

    try:
        rows = fetch_lines(args.run_id)
    except Exception as exc:
        print(f"FAIL fetch results: {type(exc).__name__}: {exc}", flush=True)
        return 1

    lines = [f"{r[0]:%H:%M:%S}Z  {r[1]}" for r in rows]
    print(f"--- RESULT lines for case={args.case} run_id={args.run_id} ({len(lines)} rows) ---")
    print("\n".join(lines) if lines else "(none)")

    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(f"### spike-s1 results: case=`{args.case}` run_id=`{args.run_id}`\n\n")
            fh.write("```\n" + ("\n".join(lines) if lines else "(no RESULT rows found)") + "\n```\n")
    return 0 if lines else 1


if __name__ == "__main__":
    sys.exit(main())
