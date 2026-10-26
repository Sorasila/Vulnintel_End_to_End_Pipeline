"""Spike S1: จัดการข้อมูล mock CMDB บน Neon (Postgres) ด้วย psycopg 3.

ใช้: python spikes/s1/neon_cmdb.py --case {seed,mutate,reset}

- seed    apply seeds/sql/cmdb_schema.sql แล้ว insert 10 แถวต่อตาราง (deterministic) ถ้าตารางว่าง
- mutate  update 2 assets (เปลี่ยน environment + bump updated_at), delete 1 asset, insert 1 asset
          (ออกแบบให้รันครั้งเดียวต่อ seed: รันซ้ำจะ bump updated_at ซ้ำ และ insert asset เพิ่มอีก)
- reset   TRUNCATE ทั้งสองตาราง RESTART IDENTITY

connection string อ่านจาก env NEON_DATABASE_URL (ห้ามพิมพ์) exit 0 เมื่อสำเร็จ, 1 เมื่อล้ม
"""

import argparse
import os
import sys
from pathlib import Path

import psycopg

SCHEMA_FILE = Path(__file__).resolve().parents[2] / "seeds" / "sql" / "cmdb_schema.sql"
CONNECT_TIMEOUT_S = 15

ENVIRONMENTS = ["prod", "staging", "dev"]
BUSINESS_UNITS = ["finance", "retail", "engineering", "hr", "operations"]
OWNER_TEAMS = ["platform", "payments", "data", "security", "support"]
SOFTWARE = [
    ("apache", "http_server", "2.4.57"),
    ("openssl", "openssl", "3.0.11"),
    ("postgresql", "postgresql", "16.2"),
    ("nginx", "nginx", "1.24.0"),
    ("microsoft", "windows_server", "2019"),
]

# id ของแถวที่ mutate แตะ (seed สร้าง id 1..10 เพราะ RESTART IDENTITY ใน reset)
UPDATE_IDS = (2, 5)
DELETE_ID = 8


def scrub(text: str, url: str) -> str:
    """ลบ connection string และ password ออกจากข้อความ error ก่อนพิมพ์"""
    out = text.replace(url, "<NEON_DATABASE_URL>")
    try:
        password = psycopg.conninfo.conninfo_to_dict(url).get("password")
    except Exception:
        password = None
    return out.replace(password, "***") if password else out


def seed_assets() -> list[tuple]:
    rows = []
    for i in range(1, 11):
        rows.append(
            (
                f"ASSET-{i:04d}",
                f"host-{i:02d}.corp.example",
                ENVIRONMENTS[i % 3],
                BUSINESS_UNITS[i % 5],
                (i % 5) + 1,
                i % 3 == 0,
                OWNER_TEAMS[i % 5],
                "active",
                f"2026-01-01T{i:02d}:00:00+00:00",  # คงที่ ไม่ใช้ now() เพื่อให้ snapshot รอบแรกเทียบกันได้
            )
        )
    return rows


def seed_software() -> list[tuple]:
    rows = []
    for i in range(1, 11):
        vendor, product, version = SOFTWARE[i % 5]
        rows.append((f"ASSET-{i:04d}", vendor, product, version, f"2026-01-02T{i:02d}:00:00+00:00", f"2026-01-02T{i:02d}:00:00+00:00"))
    return rows


def count(cur, table: str) -> int:
    cur.execute(f"SELECT count(*) FROM public.{table}")
    return cur.fetchone()[0]


def do_seed(cur) -> None:
    cur.execute(SCHEMA_FILE.read_text(encoding="utf-8"))
    print(f"applied schema {SCHEMA_FILE.name}")
    n_assets = count(cur, "assets")
    if n_assets == 0:
        cur.executemany(
            "INSERT INTO public.assets (asset_id, hostname, environment, business_unit, criticality,"
            " internet_facing, owner_team, status, updated_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            seed_assets(),
        )
        print("inserted 10 rows into assets")
    else:
        print(f"assets not empty ({n_assets} rows): skipped insert")
    n_sw = count(cur, "asset_software")
    if n_sw == 0:
        cur.executemany(
            "INSERT INTO public.asset_software (asset_id, vendor, product, version, installed_at, updated_at)"
            " VALUES (%s,%s,%s,%s,%s,%s)",
            seed_software(),
        )
        print("inserted 10 rows into asset_software")
    else:
        print(f"asset_software not empty ({n_sw} rows): skipped insert")


def do_mutate(cur) -> None:
    if count(cur, "assets") == 0:
        raise RuntimeError("assets is empty: run case=seed first")
    for asset_pk in UPDATE_IDS:
        cur.execute(
            "UPDATE public.assets SET environment = 'dr', updated_at = now() WHERE id = %s RETURNING id, asset_id",
            (asset_pk,),
        )
        row = cur.fetchone()
        print(f"updated asset id={row[0]} asset_id={row[1]}: environment='dr', updated_at=now()" if row else f"update id={asset_pk}: no such row")
    cur.execute("DELETE FROM public.assets WHERE id = %s RETURNING id, asset_id", (DELETE_ID,))
    row = cur.fetchone()
    print(f"deleted asset id={row[0]} asset_id={row[1]}" if row else f"delete id={DELETE_ID}: no such row (already deleted?)")
    cur.execute("SELECT coalesce(max(id), 0) + 1 FROM public.assets")
    n = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO public.assets (asset_id, hostname, environment, business_unit, criticality, internet_facing,"
        " owner_team, status) VALUES (%s,%s,'prod','operations',3,false,'platform','active') RETURNING id",
        (f"ASSET-{n:04d}", f"host-{n:02d}.corp.example"),
    )
    print(f"inserted asset id={cur.fetchone()[0]} asset_id=ASSET-{n:04d}")


def do_reset(cur) -> None:
    cur.execute("TRUNCATE TABLE public.assets, public.asset_software RESTART IDENTITY")
    print("truncated assets and asset_software (RESTART IDENTITY)")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--case", required=True, choices=["seed", "mutate", "reset"])
    case = parser.parse_args().case

    url = os.environ.get("NEON_DATABASE_URL", "")
    if not url:
        print("FAIL: environment variable NEON_DATABASE_URL is missing or empty (check GitHub Secrets)", flush=True)
        return 1
    try:
        # `with connect` commit เมื่อออกจาก block ปกติ และ rollback เมื่อมี exception
        with psycopg.connect(url, connect_timeout=CONNECT_TIMEOUT_S) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version()")
                print(f"connected: {cur.fetchone()[0]}")
                {"seed": do_seed, "mutate": do_mutate, "reset": do_reset}[case](cur)
                for table in ("assets", "asset_software"):
                    print(f"{table}: {count(cur, table)} rows")
    except Exception as exc:
        print(f"FAIL case={case}: {type(exc).__name__}: {scrub(str(exc), url)}", flush=True)
        return 1
    print(f"case={case}: done")
    return 0


if __name__ == "__main__":
    sys.exit(main())
