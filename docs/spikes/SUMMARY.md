# Phase 0 spikes: สรุปผลและการตัดสินใจที่ Gate G0

> วันที่สรุป: 2026-10-26 · Databricks Free Edition (serverless, Spark 4.2.0, Python 3.12), Neon Postgres 18.6, GCP project ไม่ผูก billing (BigQuery sandbox)
> รายละเอียดและหลักฐานของแต่ละ spike อยู่ใน `S1.md` ถึง `S4.md`

## ตารางผล

| Spike | คำถาม | ผล | ทางที่เลือก | สิ่งที่เปลี่ยนใน design |
| --- | --- | --- | --- | --- |
| S1 | PostgreSQL connector (CDC) จาก Neon ใช้ได้บน Free Edition ไหม | ❌ ส่วน CDC ไม่ผ่าน: wizard ให้เลือก **Change data capture** ไม่ได้ (disabled) เหลือแค่ Query based capture (Preview) / ✅ แผนสำรอง snapshot ผ่าน (`scd2_ok=True`: update 2 เวอร์ชัน, hard delete ปิด `__END_AT`, insert ใหม่) | **ทาง B: snapshot** CSV → UC Volume → Auto Loader → `dp.create_auto_cdc_from_snapshot_flow` (SCD2) | ตัด Lakeflow Connect ออกจาก architecture, ไม่เปิด logical replication ใน Neon, simulator ต้อง dump snapshot, silver ต้อง cast type และ map snapshot version → เวลา |
| S2 | Files API upload + Auto Loader `availableNow` บน serverless | ✅ ทุก case ผ่าน (upload 3 ไฟล์ 2.16 s, path ซ้ำ → `AlreadyExists`, schema evolution ผ่านด้วย retry 1 ครั้ง, `cleanSource` ใช้ได้) | **UC Volume + Files API + Auto Loader** ตาม design | `cloudFiles.cleanSource=DELETE` + `retentionDuration=30 days` แทน job ลบไฟล์รายสัปดาห์, pin `pyspark==4.2.*` / Python 3.12 / `environment_version: "4"` |
| S3 | ข้อจำกัด "1 active pipeline ต่อประเภท" | ✅ pipeline **ต่างตัวกัน** รันพร้อมกันได้, pipeline **ตัวเดียวกัน** สั่งซ้อน → task ล้มทันที ("Pipeline update already in progress.") และ task retry ไม่ช่วย, job ที่มี pipeline task ต่อกันทำงานปกติ | **concurrency group `databricks-pipelines`** ร่วมทุก workflow ที่สั่ง pipeline | pipeline task `max_retries: 0` (pipeline retry เองอยู่แล้ว), ตั้ง `timeout_seconds` ทุก job, ใช้ `mode: development` แทน `development: true`, SP ต้องมี `CREATE MATERIALIZED VIEW` |
| S4a | Databricks service principal (OAuth M2M) จาก GitHub Actions | ✅ ผ่าน (หลังสร้าง secret ใหม่ที่มี scope `all-apis`) | **SP `vulnintel-ci` OAuth M2M**, bundle `run_as` SP, ไม่ใช้ PAT | runbook: scope ของ secret, rotate secret ก่อนหมดอายุ, รายการสิทธิ์ของ SP |
| S4b | GCP OIDC (WIF) + BigQuery sandbox, `WRITE_TRUNCATE` ต่ออายุตารางไหม | ✅ WIF load ผ่าน, ❗ `WRITE_TRUNCATE` **ไม่ต่ออายุ** (expires คงที่ที่ created + 60 วัน), `update_table(["expires"])` เลื่อนได้และไม่ถูก cap, `delete_table` + load ใหม่ก็ได้ | **WIF** (ผูก `repository_id` + `refs/heads/main`), ไม่ใช้ SA key | export ต้อง `update_table` ตั้ง `expires = now + 60 วัน` หลัง load ทุกครั้ง และ monitoring เตือนเมื่อตารางเหลืออายุ < 30 วัน |

## การตัดสินใจ 5 ข้อ (Gate G0 Step 2)

1. **ทาง CMDB:** **B: snapshot** เพราะ Free Edition ปิด native CDC simulator dump `assets` และ `asset_software` เป็น CSV ลง `/Volumes/<catalog>/ops/landing/cmdb_<table>/YYYY/MM/DD/` ทุกวัน → `BronzeIngestor` อ่านเข้า `bronze.cmdb_*_snapshot` (มี `snapshot_id`) → ETL pipeline ใช้ `create_auto_cdc_from_snapshot_flow` ทำ SCD2 ใน silver ข้อดีที่ได้มา: ไม่ต้องดูแล replication slot (Neon ลบ slot ที่ไม่ใช้หลัง ~40 ชั่วโมง), เห็น hard delete, replay ได้จากไฟล์
2. **Landing:** **UC Volume** ผ่าน Files API (ไม่ใช้ Google Drive)
3. **วิธีลบไฟล์เก่า:** **`cloudFiles.cleanSource=DELETE`** `retentionDuration=30 days` (ยืนยันว่าลบจริงระหว่าง burn-in) ไม่มี job ลบไฟล์แยก
4. **Auth Databricks:** **SP OAuth M2M** (`vulnintel-ci`) ไม่ใช้ PAT
5. **Auth GCP และการกันตารางหมดอายุ:** **WIF/OIDC** ไม่ใช้ SA key และหลัง load แบบ `WRITE_TRUNCATE` ต้องเรียก `update_table` ตั้ง `expires = now + 60 วัน` ทุกครั้ง (เลือกแทน recreate เพราะตารางไม่หายระหว่างที่ Looker Studio อ่าน)

## ผลกระทบต่อแผน (Phase 1 เป็นต้นไป)

- **Task 1.1:** pin `pyspark==4.2.*`, Python 3.12
- **Task 1.2:**
  - bundle ใช้ `run_as` SP, `mode: development` ที่ target dev และ `environment_version: "4"`
  - ทุก job มี `timeout_seconds`
  - pipeline task ตั้ง `max_retries: 0`
  - bronze task ตั้ง `max_retries: 1`
- **Task 1.3:**
  - ทุก workflow ที่สั่ง pipeline ใช้ `concurrency: databricks-pipelines` และ `defaults.run.shell: bash`
  - action ที่ยังรันบน Node 20 (`checkout@v4`, `setup-python@v5`, `google-github-actions/auth@v2`) ต้องอัปเป็น major ที่รองรับ Node 24
  - ระวังการย้าย `ubuntu-latest` ไป Ubuntu 26
- **Phase 3/4 (CMDB):**
  - ตัด `resources/ingestion_cmdb.yml` (Lakeflow Connect) ออก
  - เพิ่ม source `cmdb_assets` และ `cmdb_asset_software` ใน `BronzeIngestor` (รวมกับ nvd, epss, kev = 5 task พร้อมกัน เต็มเพดานพอดี ถ้าต้องเพิ่ม source อีก ให้รวม CMDB 2 ตารางเป็น task เดียว)
  - snapshot version = เวลา dump แบบ `yyyyMMddHHmmss` (UTC)
  - `next_snapshot` cast type ก่อนส่งเข้า flow
  - เพิ่ม view map `snapshot_id` → เวลา สำหรับ query แบบ as-of
- **Phase 6 (export):** `update_table(expires)` หลัง load และ check อายุตารางใน `pipeline_health`
- **Runbook / สิทธิ์ของ SP:**
  - `USE CATALOG`, `USE SCHEMA`, `READ VOLUME`, `WRITE VOLUME`, `SELECT`, `MODIFY`, `CREATE TABLE` และ `CREATE MATERIALIZED VIEW` บน catalog
  - CAN USE บน SQL warehouse
  - secret scope `all-apis`

## งานเก็บกวาดหลังอนุมัติ (Gate G0 Step 3)

- Databricks:
  - `databricks bundle destroy` ใน `spikes/s1`, `spikes/s2`, `spikes/s3` (workflow `spike-s3` case `destroy` สำหรับ S3)
  - ลบตาราง `ops.spike_*_results`, `bronze.spike_raw`, `bronze.cmdb_assets_snapshot`, `silver.spike_dim_asset`
  - ลบไฟล์ใน `ops/landing` ของ spike
  - ลบ pipeline `spike_s1_qbc` และ connection `neon_cmdb` (ถ้ายังอยู่)
- Neon: เก็บตาราง `assets`/`asset_software` ไว้ใช้ต่อใน Phase 3 (ไม่มี slot หรือ publication ให้ลบ เพราะไม่ได้เปิด replication)
- GCP: `bash gcp_setup.sh --cleanup` ลบ dataset `vulnintel_spike` หลังเช็ก Looker Studio เสร็จ (pool, provider และ SA เก็บไว้ใช้ใน production)
