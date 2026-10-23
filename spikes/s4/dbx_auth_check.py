"""Spike S4a: ตรวจว่า GitHub Actions ยืนยันตัวตนกับ Databricks ได้ (OAuth M2M หรือ PAT).

รัน 4 check ตามลำดับ แต่ละ check แยกอิสระ (ล้มแล้วไม่หยุด check ถัดไป)
พิมพ์ 1 บรรทัดต่อ check: `PASS <name>: <detail>` หรือ `FAIL <name>: <ExceptionType>: <message>`
exit code 1 ถ้ามี check ใดล้ม ไม่พิมพ์ token/secret
"""

import io
import os
import sys
from typing import Callable

from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config, oauth_service_principal

RUN_ID = os.environ.get("GITHUB_RUN_ID", "local")
UPLOAD_PATH = f"/Volumes/vulnintel_dev/ops/landing/spike/auth_check_{RUN_ID}.txt"


def check_current_user(ws: WorkspaceClient) -> str:
    me = ws.current_user.me()
    # SP ไม่มีฟิลด์ applicationId แยก: user_name ของ SP คือ application id
    return f"userName={me.user_name} id={me.id}"


def check_files_upload(ws: WorkspaceClient) -> str:
    body = f"s4a auth check, run={RUN_ID}\n".encode()
    # overwrite=False: path ไม่ซ้ำต่อ run ถ้าซ้ำจะ error ซึ่งเป็นสัญญาณที่ต้องการ
    ws.files.upload(UPLOAD_PATH, io.BytesIO(body), overwrite=False)
    return f"uploaded {len(body)} bytes to {UPLOAD_PATH}"


def check_sql_select_1(ws: WorkspaceClient) -> str:
    host = os.environ["DATABRICKS_HOST"].removeprefix("https://").removeprefix("http://").rstrip("/")
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID") or next(iter(ws.warehouses.list())).id
    http_path = f"/sql/1.0/warehouses/{warehouse_id}"

    if os.environ.get("DATABRICKS_CLIENT_ID"):
        # OAuth M2M: ให้ connector ขอ token ผ่าน credentials_provider
        def credentials_provider():
            config = Config(
                host=f"https://{host}",
                client_id=os.environ["DATABRICKS_CLIENT_ID"],
                client_secret=os.environ["DATABRICKS_CLIENT_SECRET"],
            )
            return oauth_service_principal(config)

        auth = {"credentials_provider": credentials_provider}
        mode = "oauth-m2m"
    else:
        auth = {"access_token": os.environ["DATABRICKS_TOKEN"]}
        mode = "pat"

    with sql.connect(server_hostname=host, http_path=http_path, **auth) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            value = cursor.fetchone()[0]
    return f"SELECT 1 -> {value} via {mode}, warehouse={warehouse_id}"


def check_jobs_list(ws: WorkspaceClient) -> str:
    jobs = list(ws.jobs.list(limit=1))
    return f"jobs.list returned {len(jobs)} job(s)"


def run_check(name: str, fn: Callable[[], str]) -> bool:
    try:
        print(f"PASS {name}: {fn()}", flush=True)
        return True
    except Exception as exc:  # แต่ละ check ต้องไม่ทำให้ check อื่นหยุด
        print(f"FAIL {name}: {type(exc).__name__}: {exc}", flush=True)
        return False


def main() -> int:
    ws, client_error = None, None
    try:
        ws = WorkspaceClient()  # อ่าน DATABRICKS_HOST/AUTH_TYPE/CLIENT_ID/... จาก env
    except Exception as exc:
        client_error = exc

    checks = [
        ("current_user.me", check_current_user),
        ("files.upload", check_files_upload),
        ("sql.select_1", check_sql_select_1),
        ("jobs.list", check_jobs_list),
    ]
    results = []
    for name, check in checks:
        if ws is None:
            err = client_error
            print(f"FAIL {name}: {type(err).__name__}: WorkspaceClient() failed: {err}", flush=True)
            results.append(False)
        else:
            results.append(run_check(name, lambda check=check: check(ws)))
    return 0 if all(results) else 1


if __name__ == "__main__":
    sys.exit(main())
