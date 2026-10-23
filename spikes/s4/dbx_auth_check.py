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
# GITHUB_RUN_ID ไม่เปลี่ยนเมื่อกด "Re-run jobs" ต้องต่อ GITHUB_RUN_ATTEMPT ด้วย
# ไม่เช่นนั้น overwrite=False จะ error ซ้ำและได้ FAIL ปลอม
RUN_ATTEMPT = os.environ.get("GITHUB_RUN_ATTEMPT", "1")
UPLOAD_PATH = f"/Volumes/vulnintel_dev/ops/landing/spike/auth_check_{RUN_ID}-{RUN_ATTEMPT}.txt"

# กัน SDK ค้างเมื่อ host ใช้ไม่ได้ (default retry 300 วินาทีต่อ call)
# ค่านี้ตั้งผ่าน env ไม่ได้ (SDK ไม่มี env var ให้สองตัวนี้) ต้องส่งใน code
SDK_HTTP_TIMEOUT_S = 10
SDK_RETRY_TIMEOUT_S = 20

# กัน SQL connector ค้างเมื่อ host ใช้ไม่ได้ (default retry นานถึง 900 วินาที)
# ต้องเผื่อเวลา cold start ของ warehouse ที่หยุดอยู่ (connector retry 429/503 ระหว่าง start)
SQL_SOCKET_TIMEOUT_S = 60
SQL_RETRY_STOP_AFTER_S = 300


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"environment variable {name} is missing or empty (check GitHub Secrets)")
    return value


def check_current_user(ws: WorkspaceClient) -> str:
    me = ws.current_user.me()
    # SP ไม่มีฟิลด์ applicationId แยก: user_name ของ SP คือ application id
    return f"userName={me.user_name} id={me.id}"


def check_files_upload(ws: WorkspaceClient) -> str:
    body = f"s4a auth check, run={RUN_ID}\n".encode()
    # overwrite=False: path ไม่ซ้ำต่อ run+attempt ถ้าซ้ำจะ error ซึ่งเป็นสัญญาณที่ต้องการ
    ws.files.upload(UPLOAD_PATH, io.BytesIO(body), overwrite=False)
    return f"uploaded {len(body)} bytes to {UPLOAD_PATH}"


def check_sql_select_1(ws: WorkspaceClient) -> str:
    host = require_env("DATABRICKS_HOST").removeprefix("https://").removeprefix("http://").rstrip("/")
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID") or next(iter(ws.warehouses.list())).id
    http_path = f"/sql/1.0/warehouses/{warehouse_id}"

    auth_type = os.environ.get("DATABRICKS_AUTH_TYPE", "")
    if auth_type == "oauth-m2m":
        client_id = require_env("DATABRICKS_CLIENT_ID")
        client_secret = require_env("DATABRICKS_CLIENT_SECRET")

        # OAuth M2M: ให้ connector ขอ token ผ่าน credentials_provider
        def credentials_provider():
            config = Config(
                host=f"https://{host}",
                client_id=client_id,
                client_secret=client_secret,
                http_timeout_seconds=SDK_HTTP_TIMEOUT_S,
                retry_timeout_seconds=SDK_RETRY_TIMEOUT_S,
            )
            return oauth_service_principal(config)

        auth = {"credentials_provider": credentials_provider}
        mode = "oauth-m2m"
    elif auth_type == "pat":
        auth = {"access_token": require_env("DATABRICKS_TOKEN")}
        mode = "pat"
    else:
        raise ValueError(f"DATABRICKS_AUTH_TYPE={auth_type!r}; expected 'oauth-m2m' or 'pat'")

    with sql.connect(
        server_hostname=host,
        http_path=http_path,
        _socket_timeout=SQL_SOCKET_TIMEOUT_S,
        _retry_stop_after_attempts_duration=SQL_RETRY_STOP_AFTER_S,
        **auth,
    ) as conn:
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
        # Config อ่าน DATABRICKS_HOST/AUTH_TYPE/CLIENT_ID/... จาก env เอง
        ws = WorkspaceClient(
            config=Config(http_timeout_seconds=SDK_HTTP_TIMEOUT_S, retry_timeout_seconds=SDK_RETRY_TIMEOUT_S)
        )
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
