# VulnIntel Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** สร้าง pipeline ข้อมูลช่องโหว่ที่รันจริงทุกวันด้วยงบ 0 บาท บน Databricks Free Edition (NVD + EPSS + CISA KEV + mock CMDB → bronze/silver/gold → BigQuery sandbox → Looker Studio 4 หน้า)

**Architecture:** งานที่ติดต่อโลกภายนอก (simulator, extract, export) รันบน GitHub Actions ส่วน Databricks ทำแค่ ingest (Auto Loader / CDC) และ ETL ด้วย Lakeflow Spark Declarative Pipelines ทุก transform เป็นฟังก์ชัน `DataFrame → DataFrame` ใน `src/vulnintel/` ที่ test ได้ด้วย PySpark local ใน CI แล้ว deploy ด้วย Databricks Asset Bundles

**Tech Stack:** Python 3.12, PySpark (DataFrame API), `pyspark.pipelines` (`dp`), Databricks Asset Bundles + Lakeflow Jobs, `databricks-sdk`, `databricks-sql-connector`, Neon Postgres (`psycopg`), `google-cloud-bigquery` + `pyarrow`, `httpx` + `respx` (HTTP mock), `pytest`, `ruff`, GitHub Actions, healthchecks.io, Discord webhook

**Spec:** [`Vulnerability Intelligence Pipeline — Design Spec.md`](../Vulnerability%20Intelligence%20Pipeline%20—%20Design%20Spec.md) (อนุมัติแล้ว 22 ต.ค. 2026) ผู้ทำทุก task ต้องอ่านคู่กัน

## Global Constraints

- Databricks Free Edition: serverless เท่านั้น, outbound internet จำกัด, Lakeflow pipeline active ได้ 1 ตัวต่อประเภท, job task พร้อมกันสูงสุด 5, SQL warehouse 2X-Small 1 ตัว, ไม่มี Scala/R
- PySpark DataFrame API เท่านั้น: ห้ามใช้ RDD, `.cache()`, หรือตั้ง Spark config เอง
- Pipeline code ใช้ `from pyspark import pipelines as dp`
- ไม่ใช้ Airflow, Kafka, dbt, Terraform (ADR-001 และ "นอกขอบเขต" ใน spec)
- งานที่เรียก API ภายนอกทั้งหมดรันบน GitHub Actions; job บน GitHub Actions ไม่มี transform
- Bronze: `inferColumnTypes=false` (ทุกคอลัมน์เป็น string), `schemaEvolutionMode=addNewColumns`, `rescuedDataColumn=_rescued_data`, ห้ามตั้งชื่อคอลัมน์ `_metadata`, ใช้ `_file_modification_time`
- Bronze เก็บ `_load_dttm` เป็น UTC; แปลงเป็นวันที่ `Asia/Bangkok` ใน silver; ห้าม `current_date()` ใน bronze
- Checkpoint/schema location: `/Volumes/<catalog>/ops/_checkpoints/<table>`
- ชื่อไฟล์ landing ไม่ซ้ำทุกรอบ: `<source>/YYYY/MM/DD/<source>_<run_id>_<part>.json` (NDJSON); ลบไฟล์เก่ากว่า 30 วัน
- BronzeIngestor task: 1 task ต่อ source, retry อย่างน้อย 1 ครั้ง, trigger `availableNow`
- ห้ามเทียบ version เป็น string (`"7.2.10" < "7.2.9"`)
- ฉีดข้อมูลเสียเฉพาะ mock CMDB ด้วย `fault_rate` ค่าตั้งต้น `0.03`; ห้ามแตะ threat intel จริง
- Priority tier: T1 อยู่ใน KEV → T2 EPSS ≥ 0.1 และ CVSS ≥ 7 → T3 CVSS ≥ 7 → T4 ที่เหลือ; SLA T1/T2/T3/T4 = 7/14/30/90 วัน (ปรับใน config)
- DQ: quarantine > 10% ต่อตาราง = critical → job fail และไม่ export; ความสด `max(_ingested_at)` ต่อ source ไม่เกิน 36 ชั่วโมง
- NVD: 50 request / 30 วินาที (มี key), ช่วงวันที่ต่อ request ≤ 120 วัน, watermark = `max(last_modified)` ใน bronze ลบ 2 ชั่วโมง
- NVD หยุด enrich ทุก CVE ตั้งแต่ 15 เม.ย. 2026 → ใช้คะแนน CNA เมื่อไม่มีของ NIST พร้อม `score_source` และ `enrichment_status`
- BigQuery sandbox: ไม่มี DML/streaming, ตารางหมดอายุ 60 วัน → load แบบ `WRITE_TRUNCATE`
- Neon ลบ replication slot ที่ idle ราว 40 ชั่วโมง; asset สูงสุดราว 200-300 เครื่อง
- ข้อความ "This product uses the NVD API but is not endorsed or certified by the NVD." ต้องอยู่ใน README และ footer ทุกหน้า dashboard
- Repo layout ตาม spec: `databricks.yml`, `resources/`, `src/vulnintel/{ingestion,transforms,dq,pipelines}`, `jobs/{simulator,extract,export}`, `seeds/`, `tests/{unit,contract,fixtures}`, `docs/{adr,spikes,...}`, `.github/workflows/`
- Catalog: `vulnintel` (prod) และ `vulnintel_dev` (dev) แต่ละ catalog มี schema `bronze`, `silver`, `gold`, `ops`; volume `ops.landing` และ `ops._checkpoints`

## Review Focus

1. **Version ที่จำนวนส่วนไม่เท่าหรือมี suffix** (`7.2` กับ `7.2.0`, `2.4.0-rc1`, `latest`): `7.2` ต้องเท่ากับ `7.2.0`, suffix ไม่ทำให้ match ผิด, version ที่ parse ไม่ได้ต้องไม่ match และถูกนับเป็น unmatchable ไม่ใช่ error (Task 2.2)
2. **รอบแรกที่ตารางต้นทางยังไม่มี** (bronze ว่าง, ยังไม่มี `gold.asset_vulnerability` หรือ `gold.cve_watchlist`): NVD ต้อง backfill 3 ปี, EPSS ใช้ KEV อย่างเดียว, simulator ข้ามขั้น patch แทนที่จะล่ม (Task 3.2, 3.4, 4.1)
3. **กฎ DQ ที่ประเมินได้ NULL** (เช่น `criticality` เป็น null หรือ `try_cast` ล้มเหลว): ต้องนับเป็น "ไม่ผ่าน" ไม่ใช่หลุดเข้าตาราง clean (Task 2.3)
4. **รันซ้ำวันเดียวกัน / extract ถูก retry** (ไฟล์เนื้อหาเดียวกันแต่ `run_id` ต่างกัน): silver ต้องไม่มีแถวซ้ำ และ simulator รันซ้ำได้ความเปลี่ยนแปลงชุดเดิม (Task 2.4, 4.1)
5. **Source หนึ่งล่มทั้งวัน** (NVD ตอบ 503): source อื่นยังเข้า bronze, `pipeline_health` แสดงว่า NVD เก่า, `dq_check` ไม่ fail จนกว่าจะเกิน 36 ชั่วโมง (Task 5.4, 6.2)

## วิธีอ่านแผนนี้

- **Phase 0 (spikes)** เป็นการทดลองบน account จริง ไม่ใช่ TDD ผลลัพธ์ของแต่ละ spike คือไฟล์บันทึกผลใน `docs/spikes/` และคำตัดสิน ผ่าน/ไม่ผ่าน
- **Gate G0** หลัง spike ครบ: อัปเดต spec และ ADR ตามผล แล้วจึงเริ่ม Phase 1
- **Phase 1-2** ไม่ขึ้นกับผล spike เขียนละเอียดระดับ step
- **Phase 3-7** ใช้ "TDD cycle มาตรฐาน" ต่อ task: (1) เขียน test ที่ระบุ (2) รัน `pytest <file> -v` เห็น FAIL (3) implement ตาม signature (4) รันซ้ำเห็น PASS (5) `ruff check . && ruff format --check .` แล้ว commit แผนจึงระบุเฉพาะไฟล์, interface, test และค่าที่ spec กำหนด
- ขั้นที่ติดป้าย **[คุณ]** ต้องทำโดยเจ้าของ account (สมัครบริการ, กดใน console, ใส่ secret) Claude ช่วยเตรียมคำสั่งและตรวจผลได้

### ผลจากการตรวจเอกสารก่อนเริ่ม (22 ต.ค. 2026)

ตรวจเอกสารปัจจุบันแล้วพบ 3 เรื่องที่เปลี่ยนความคาดหวังของ spike:

- **S1 มีแนวโน้มไม่ผ่าน:** เอกสาร PostgreSQL connector ระบุว่า ingestion pipeline รันบน serverless แต่ **gateway ต้องรันบน classic compute และรันแบบ continuous** ซึ่ง Free Edition ไม่มี และ connector ยังเป็น Public Preview ที่ต้องให้ account team เปิดให้ ([Databricks: PostgreSQL limitations](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/postgresql-limits), [Neon: replicate to Databricks](https://neon.com/docs/guides/logical-replication-databricks)) จึงตั้ง time-box สั้นและเตรียมทดลองแผนสำรองในตัว spike เลย
- **S4 ฝั่ง Databricks อาจต้องใช้ PAT:** มีรายงานใน community ว่า service principal OAuth ใช้ไม่ได้บน Free Edition เพราะไม่มี account console แต่ยังไม่มีเอกสารทางการยืนยัน ([community thread](https://community.databricks.com/t5/administration-architecture/zerobus-ingestion-fails-in-databricks-free-edition-using-service/td-p/161228))
- **ความเสี่ยงใหม่ที่ spec ยังไม่ครอบ (ยังไม่ได้ยืนยัน):** `WRITE_TRUNCATE` ไม่ได้สร้างตารางใหม่ จึงอาจไม่ reset วันหมดอายุ 60 วันของ sandbox ตามที่ spec สมมติไว้ เพิ่มการตรวจข้อนี้ใน S4

---

## Phase 0: Spikes S1-S4

**ลำดับที่แนะนำ:** Task 0.1 → S4a (Databricks auth) → S2 → S1 → S3 → S4b (GCP) → Gate G0 เพราะ S2 ต้องใช้ auth จาก S4a, แผนสำรองของ S1 ใช้ผลของ S2 และ S3 ต้องรู้ก่อนว่ามี ingestion pipeline หรือไม่

**Time-box รวม:** ราว 2-3 วันทำงาน ถ้า spike ใดเกิน time-box ให้ถือว่า "ไม่ผ่าน" และใช้แผนสำรอง

### Task 0.1: เตรียม account และ repo สำหรับ spike

**Files:**
- Create: `README.md` (ชั่วคราว 3 บรรทัด), `.gitignore`, `docs/spikes/_template.md`, `spikes/README.md`

- [ ] **Step 1 [คุณ]: สร้าง account ฟรีที่ต้องใช้** Databricks Free Edition, Neon (free plan), GCP project ใหม่ที่ไม่ผูก billing (BigQuery sandbox), NVD API key ([request](https://nvd.nist.gov/developers/request-an-api-key)), healthchecks.io, Discord webhook บันทึก region ของ Databricks workspace ไว้ใน `docs/spikes/_environment.md` (ห้ามใส่ secret)
- [ ] **Step 2 [คุณ]: สร้าง GitHub repo สาธารณะ `vulnintel`** แล้วเพิ่ม repo เข้า project นี้ เพื่อให้ Claude เปิด branch และ PR ได้
- [ ] **Step 3: เขียน `docs/spikes/_template.md`** มีหัวข้อ: คำถาม, setup, ขั้นตอนที่ทำจริง, หลักฐาน (คำสั่ง + output/screenshot), ผล (ผ่าน/ไม่ผ่าน/บางส่วน), ผลต่อ design, เวลาที่ใช้
- [ ] **Step 4: Commit**

```bash
git add README.md .gitignore docs/spikes spikes
git commit -m "chore: bootstrap repo for Phase 0 spikes"
```

### Task 0.2: S4a — Databricks auth จาก GitHub Actions

**คำถาม:** GitHub Actions ยืนยันตัวตนกับ Free Edition workspace ด้วย service principal OAuth (M2M) ได้ไหม ถ้าไม่ได้ PAT มีข้อจำกัดอะไร

**Files:**
- Create: `.github/workflows/spike-s4a.yml`, `spikes/s4/dbx_auth_check.py`, `docs/spikes/S4.md`

**Time-box:** 3 ชั่วโมง

- [ ] **Step 1 [คุณ]: สร้าง service principal** Workspace settings → Identity and access → Service principals → Add → สร้าง OAuth secret ถ้าเมนูหรือปุ่ม secret ไม่มี ให้ capture หน้าจอและข้ามไป Step 4
- [ ] **Step 2 [คุณ]: ใส่ GitHub Secrets** `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET`
- [ ] **Step 3: เขียน `spikes/s4/dbx_auth_check.py`** ใช้ `databricks.sdk.WorkspaceClient()` (อ่าน env อัตโนมัติ) ทำ 4 อย่างและพิมพ์ผลทีละบรรทัด: `current_user.me()`, `files.upload()` ไฟล์เล็กไป `/Volumes/vulnintel_dev/ops/landing/spike/auth_check.txt`, `sql` query `SELECT 1` ผ่าน `databricks-sql-connector` (OAuth M2M ใช้ `credentials_provider`), `jobs.list(limit=1)` ส่วน workflow `spike-s4a.yml` เป็น `workflow_dispatch` ติดตั้ง `databricks-sdk databricks-sql-connector` แล้วรันสคริปต์ (สร้าง catalog/volume ตาม Task 0.3 Step 1 ก่อนถ้ายังไม่มี)
- [ ] **Step 4: ถ้า OAuth ไม่ผ่าน ทดสอบ PAT** [คุณ] สร้าง PAT ใส่ secret `DATABRICKS_TOKEN` แล้วรันสคริปต์เดิมโดยตั้ง `DATABRICKS_AUTH_TYPE=pat` บันทึก lifetime สูงสุดที่ตั้งได้และวันหมดอายุ
- [ ] **Step 5: บันทึกผลใน `docs/spikes/S4.md`** ส่วน Databricks

**ผ่าน:** ทั้ง 4 คำสั่งสำเร็จด้วย OAuth → ใช้ service principal และตั้ง `run_as` ของ bundle เป็น SP
**ไม่ผ่าน:** ใช้ PAT, เพิ่มงาน "ต่ออายุ PAT" ใน runbook และให้ workflow keepalive เตือนก่อน PAT หมดอายุ 14 วัน

### Task 0.3: S2 — Files API upload + Auto Loader บน serverless

**คำถาม:** GitHub Actions upload NDJSON เข้า UC Volume ผ่าน Files API ได้ไหม และ Auto Loader `availableNow` อ่านเข้า bronze บน serverless ได้ตามกฎของ BronzeIngestor ทุกข้อไหม

**Files:**
- Create: `spikes/s2/setup.sql`, `spikes/s2/upload_fixtures.py`, `spikes/s2/autoloader_probe.py`, `.github/workflows/spike-s2.yml`, `docs/spikes/S2.md`

**Time-box:** 4 ชั่วโมง

- [ ] **Step 1: เขียนและรัน `spikes/s2/setup.sql`** ใน SQL editor: `CREATE CATALOG IF NOT EXISTS vulnintel_dev`, schema `bronze`/`silver`/`gold`/`ops`, `CREATE VOLUME vulnintel_dev.ops.landing` และ `vulnintel_dev.ops._checkpoints` ถ้าชื่อที่ขึ้นต้นด้วย `_` ใช้ไม่ได้ ให้ใช้ `checkpoints` และบันทึกไว้
- [ ] **Step 2: เขียน `upload_fixtures.py`** สร้าง NDJSON 3 ไฟล์ (ไฟล์ละ 100 แถว, field `cve_id`, `score`, `date`) แล้ว upload ด้วย `WorkspaceClient().files.upload(path, f, overwrite=False)` ไปที่ `/Volumes/vulnintel_dev/ops/landing/spike/2026/10/22/spike_<GITHUB_RUN_ID>_<part>.json` รันผ่าน `spike-s2.yml` บันทึกเวลาที่ใช้ และลอง upload path เดิมซ้ำว่าได้ error อะไร
- [ ] **Step 3: เขียน `autoloader_probe.py`** รันเป็น Python task ใน serverless job ใช้ options: `cloudFiles.format=json`, `cloudFiles.inferColumnTypes=false`, `cloudFiles.schemaEvolutionMode=addNewColumns`, `rescuedDataColumn=_rescued_data`, `cloudFiles.schemaLocation=/Volumes/vulnintel_dev/ops/_checkpoints/spike_raw` เพิ่มคอลัมน์จาก `_metadata` (`file_path`, `file_name`, `file_size`, `file_modification_time`) และ `_load_dttm = current_timestamp()` เขียน `vulnintel_dev.bronze.spike_raw` ด้วย `trigger(availableNow=True)` พิมพ์ `spark.version` ด้วย
- [ ] **Step 4: รันชุดทดสอบ 5 กรณีและบันทึกผล**
  1. รันครั้งแรก: ได้ 300 แถว ทุกคอลัมน์เป็น string
  2. รันซ้ำโดยไม่มีไฟล์ใหม่: ได้ 0 แถวใหม่
  3. upload ไฟล์ที่มี field ใหม่ `vendor`: รันครั้งแรก fail ด้วย schema change, รันครั้งที่สอง (retry) ผ่านและมีคอลัมน์ `vendor`
  4. ตั้ง job task `max_retries=1` แล้วทำกรณี 3 ซ้ำด้วย field `product`: job จบด้วยสถานะสำเร็จ
  5. ลอง `cloudFiles.cleanSource=DELETE` กับ `cloudFiles.cleanSource.retentionDuration=30 days` บน serverless ว่ารองรับไหม
- [ ] **Step 5: บันทึก `spark.version` และ serverless environment version** ใน `docs/spikes/S2.md` ค่านี้ใช้ pin `pyspark` ใน Task 1.1

**ผ่าน:** กรณี 1-4 ผ่าน → ใช้ตาม design ถ้ากรณี 5 ผ่านใช้ `cleanSource` แทน job ลบไฟล์รายสัปดาห์
**ไม่ผ่าน:** ใช้ Google Drive connector เป็น landing แทน (แผนสำรองใน spec) และเขียน spike ย่อยของทางนั้น

### Task 0.4: S1 — PostgreSQL connector (CDC) จาก Neon

**คำถาม:** Lakeflow Connect PostgreSQL connector สร้าง gateway และ ingest จาก Neon พร้อม SCD2 ได้บน Free Edition ไหม ถ้าไม่ได้ แผนสำรอง snapshot ใช้ได้จริงไหม

**Files:**
- Create: `seeds/sql/cmdb_schema.sql`, `spikes/s1/neon_setup.sql`, `spikes/s1/snapshot_fallback_pipeline.py`, `spikes/s1/snapshot_upload.py`, `docs/spikes/S1.md`

**Time-box:** ส่วน CDC 2 ชั่วโมง, ส่วนแผนสำรอง 3 ชั่วโมง

- [ ] **Step 1: เขียน `seeds/sql/cmdb_schema.sql`** ตาราง `assets` และ `asset_software` ตามคอลัมน์ใน spec: `id bigserial PRIMARY KEY`, `asset_id text` (ไม่มี NOT NULL และ UNIQUE โดยตั้งใจ), `criticality int`, `internet_facing boolean`, `updated_at timestamptz NOT NULL DEFAULT now()` และ `ALTER TABLE ... REPLICA IDENTITY DEFAULT`
- [ ] **Step 2 [คุณ]: เตรียม Neon** สร้าง project (Postgres ≥ 13) ใน region ใกล้ workspace, Settings → Postgres → Logical replication → Enable (เปลี่ยนกลับไม่ได้), รัน `cmdb_schema.sql` และ insert 10 แถวต่อตาราง
- [ ] **Step 3: เขียนและรัน `spikes/s1/neon_setup.sql`** role `databricks_replication`, `GRANT CONNECT/USAGE/SELECT`, `ALTER DEFAULT PRIVILEGES`, `CREATE PUBLICATION databricks_publication FOR TABLE public.assets, public.asset_software`, `SELECT pg_create_logical_replication_slot('databricks_slot', 'pgoutput')` (รันในฐานะ role ใหม่) ตรวจ `SHOW wal_level` = `logical`
- [ ] **Step 4 [คุณ]: ลองสร้าง connector** ดูว่าเมนู Data ingestion → PostgreSQL มีบน Free Edition ไหม (หรือต้องเปิดใน Previews) สร้าง UC connection ด้วย host แบบ direct (ไม่มี `-pooler`) port 5432 แล้วสร้าง pipeline แบบ Change data capture เปิด SCD type 2 บันทึก error หรือ compute ที่ gateway ขอใช้แบบคำต่อคำ
- [ ] **Step 5 (ถ้า gateway รันได้): ทดสอบ CDC** update 2 แถว, delete 1 แถว, insert 1 แถว แล้ว trigger ingestion pipeline ตรวจว่า bronze มีแถวประวัติครบ (`__START_AT`, `__END_AT`) วัด compute ที่ gateway ใช้ใน 24 ชั่วโมงเทียบโควตา และดูว่า gateway นับเป็น active pipeline ไหม (ใช้ใน S3)
- [ ] **Step 6 (ถ้า Step 4 หรือ 5 ไม่ผ่าน): พิสูจน์แผนสำรอง snapshot** `snapshot_upload.py` ดึง `assets` จาก Neon เป็น CSV 2 รอบ (รอบ 2 แก้ 2 แถว ลบ 1 แถว) upload ไป `/Volumes/vulnintel_dev/ops/landing/cmdb_assets/2026/10/22/cmdb_assets_<run_id>_0.csv` ด้วยวิธีจาก S2 ingest เข้า `bronze.cmdb_assets_snapshot` ด้วย `autoloader_probe.py` (`format=csv`, `header=true`) แล้วรัน `snapshot_fallback_pipeline.py` เป็น SDP pipeline:

```python
dp.create_streaming_table("spike_dim_asset")
dp.create_auto_cdc_from_snapshot_flow(
    target="spike_dim_asset",
    source=next_snapshot,  # (latest_version) -> (DataFrame, version) | None
    keys=["id"],
    stored_as_scd_type=2,
)
```

  `next_snapshot` อ่าน `bronze.cmdb_assets_snapshot` เลือก `snapshot_id` (มาจาก `run_id` ในชื่อไฟล์) ที่มากกว่า `latest_version` ตัวถัดไป ตรวจว่าแถวที่แก้มีประวัติ 2 เวอร์ชันและแถวที่ลบถูกปิด `__END_AT`
- [ ] **Step 7: บันทึกผลใน `docs/spikes/S1.md`**

**ผ่าน:** Step 5 ผ่านและ gateway ใช้ compute ไม่เกินโควตา → ใช้ CDC ตาม design (Phase 4 ทาง A)
**ไม่ผ่าน:** Step 6 ผ่าน → ใช้ snapshot (Phase 4 ทาง B) ข้อดีคือหมดปัญหา Neon ลบ replication slot หลัง 40 ชั่วโมง ให้ลบ slot และ publication ใน Neon หลังจบ spike

### Task 0.5: S3 — ข้อจำกัด 1 active pipeline ต่อประเภท

**คำถาม:** เมื่อมี pipeline 2 ตัวถูกสั่งพร้อมกัน (dev กับ prod, หรือ ingestion กับ ETL) ระบบ queue, reject หรือรันพร้อมกัน และ job ที่มี pipeline task ต่อกันเป็นลำดับรันได้ปกติไหม

**Files:**
- Create: `spikes/s3/databricks.yml`, `spikes/s3/pipeline_a.py`, `spikes/s3/pipeline_b.py`, `docs/spikes/S3.md`

**Time-box:** 2 ชั่วโมง

- [ ] **Step 1: เขียน bundle เล็ก `spikes/s3/databricks.yml`** pipeline serverless 2 ตัว (`spike_a`, `spike_b`) แต่ละตัวมี materialized view เดียวที่ `spark.range(1000)` และ job `spike_seq` ที่รัน `spike_a` → `spike_b` ต่อกัน deploy ด้วย `databricks bundle deploy -t dev`
- [ ] **Step 2: สั่ง 2 pipeline พร้อมกัน** `databricks pipelines start-update <id_a> & databricks pipelines start-update <id_b>` บันทึกสถานะของตัวที่สองแบบคำต่อคำ (queued / error message / running)
- [ ] **Step 3: รัน job `spike_seq`** ต้องสำเร็จทั้ง 2 task
- [ ] **Step 4: รัน `spike_seq` ระหว่างที่ `spike_a` ถูกสั่งแยกอยู่** บันทึกว่า job รอ, fail หรือ retry ได้
- [ ] **Step 5: ถ้า S1 ใช้ CDC ได้** ทดสอบซ้ำโดยให้ ingestion pipeline (และ gateway) เป็นตัวหนึ่ง
- [ ] **Step 6: บันทึกผลใน `docs/spikes/S3.md`** และนโยบายที่ตัดสินใจ

**ผลที่ใช้ต่อ:** ทุก workflow ที่สั่งรัน pipeline (`daily-run`, `integration`) ใช้ `concurrency: group: databricks-pipelines` ร่วมกัน และ job task ที่เป็น pipeline ตั้ง retry ตามพฤติกรรมที่พบใน Step 4

### Task 0.6: S4b — GCP OIDC + BigQuery sandbox

**คำถาม:** GitHub Actions ใช้ Workload Identity Federation load ข้อมูลเข้า BigQuery sandbox ได้ไหม และ `WRITE_TRUNCATE` reset วันหมดอายุตารางไหม

**Files:**
- Create: `.github/workflows/spike-s4b.yml`, `spikes/s4/bq_load_check.py`, `spikes/s4/gcp_setup.sh`
- Modify: `docs/spikes/S4.md` (ส่วน GCP)

**Time-box:** 4 ชั่วโมง

- [ ] **Step 1: เขียน `spikes/s4/gcp_setup.sh`** (ให้คุณรันใน Cloud Shell) enable `bigquery`, `iamcredentials`, `sts`; สร้าง dataset `vulnintel_spike`; service account `vulnintel-export` พร้อม `roles/bigquery.jobUser` (project) และ `roles/bigquery.dataEditor` (dataset); workload identity pool `github` + provider `github-oidc` (issuer `https://token.actions.githubusercontent.com`) ที่มี attribute condition `assertion.repository == '<owner>/vulnintel' && assertion.ref == 'refs/heads/main'`; bind `roles/iam.workloadIdentityUser` ให้ `principalSet://.../attribute.repository/<owner>/vulnintel`
- [ ] **Step 2 [คุณ]: รัน `gcp_setup.sh`** และใส่ GitHub variables `GCP_WIF_PROVIDER`, `GCP_SA_EMAIL`, `GCP_PROJECT_ID`
- [ ] **Step 3: เขียน `bq_load_check.py` และ `spike-s4b.yml`** workflow มี `permissions: id-token: write` ใช้ `google-github-actions/auth@v2` สคริปต์สร้าง Parquet 10 แถวด้วย `pyarrow` แล้ว `load_table_from_file` ด้วย `SourceFormat.PARQUET`, `WriteDisposition.WRITE_TRUNCATE` ไป `vulnintel_spike.t1` พิมพ์ `table.created`, `table.modified`, `table.expires`
- [ ] **Step 4: ทดสอบการหมดอายุ** รัน workflow 2 ครั้งห่างกันอย่างน้อย 1 ชั่วโมง เทียบ `expires` ถ้าไม่เปลี่ยน ลอง 2 ทาง: (ก) `client.update_table(table, ["expires"])` เลื่อนเป็น now + 59 วัน (ข) `delete_table(not_found_ok=True)` ก่อน load บันทึกว่าทางไหน sandbox ยอมให้ทำ
- [ ] **Step 5: ถ้า WIF ไม่ผ่าน** [คุณ] สร้าง SA key ใส่ secret `GCP_SA_KEY` แล้วรันซ้ำด้วย `credentials_json`
- [ ] **Step 6 [คุณ]: เช็ก Looker Studio** ต่อ dataset `vulnintel_spike` และแชร์รายงานแบบ "anyone with the link can view" ได้
- [ ] **Step 7: บันทึกผลใน `docs/spikes/S4.md`** แล้วลบ dataset spike

**ผ่าน:** WIF load สำเร็จ → ใช้ OIDC **ไม่ผ่าน:** ใช้ SA key และเพิ่มการ rotate key ใน runbook ผลของ Step 4 กำหนดวิธี export ใน Task 6.1

### Task 0.7: Gate G0 — ตัดสินใจจากผล spike

**Files:**
- Create: `docs/spikes/SUMMARY.md`, `docs/adr/002-ingestion-cdc-and-file-landing.md` (เวอร์ชันที่สะท้อนผล S1/S2)
- Modify: spec ใน project files (ส่วน Spikes, Architecture และ Orchestration ตามผล)

- [ ] **Step 1: เขียน `SUMMARY.md`** ตาราง spike × ผล × ทางที่เลือก × สิ่งที่เปลี่ยนใน design
- [ ] **Step 2: ตัดสิน 5 ข้อนี้ให้ครบ** ทาง CMDB (A: CDC / B: snapshot), landing (Volume / Drive), วิธีลบไฟล์เก่า (`cleanSource` / job), auth Databricks (SP / PAT), auth GCP และวิธีกันตารางหมดอายุ
- [ ] **Step 3: [คุณ] review และอนุมัติ** ก่อนเริ่ม Phase 1 แล้วลบ resource ของ spike (`databricks bundle destroy` ใน `spikes/s3`, ตาราง `bronze.spike_raw`)
- [ ] **Step 4: Commit**

```bash
git add docs/spikes docs/adr
git commit -m "docs: record spike results and gate G0 decisions"
```

---

## Phase 1: Foundation (repo, CI, bundle)

### Task 1.1: Python project และ test harness

**Files:**
- Create: `pyproject.toml`, `src/vulnintel/__init__.py`, `tests/conftest.py`, `tests/unit/test_smoke.py`

**Interfaces:**
- Produces: fixture `spark` (session scope) ใน `tests/conftest.py` ที่ทุก test ของ Spark ใช้

- [ ] **Step 1: เขียน test**

```python
def test_spark_session_runs_local(spark):
    assert spark.range(3).count() == 3
    assert spark.conf.get("spark.sql.session.timeZone") == "UTC"
```

- [ ] **Step 2: รัน `pytest tests/unit/test_smoke.py -v`** คาดว่า FAIL (`fixture 'spark' not found`)
- [ ] **Step 3: เขียน `pyproject.toml` และ `conftest.py`** แพ็กเกจ `vulnintel` แบบ src layout; dependency กลุ่ม `dev`: `pyspark==<major.minor จาก S2>`, `pytest`, `ruff`, `httpx`, `respx`, `pyyaml`; กลุ่ม `jobs`: `databricks-sdk`, `databricks-sql-connector`, `psycopg[binary]`, `google-cloud-bigquery`, `pyarrow`; ruff `line-length = 100`, `target-version = "py312"` fixture `spark` ใช้ `SparkSession.builder.master("local[1]")` ตั้ง `spark.sql.session.timeZone=UTC` และ `spark.sql.shuffle.partitions=1` (config นี้ใช้เฉพาะ test local ไม่อยู่ในโค้ด production)
- [ ] **Step 4: รัน `pytest tests/unit/test_smoke.py -v`** คาดว่า PASS
- [ ] **Step 5: Commit** `git commit -m "chore: python project and local spark test harness"`

### Task 1.2: Bundle skeleton (dev/prod)

**Files:**
- Create: `databricks.yml`, `resources/.gitkeep`, `resources/schemas.yml`

**Interfaces:**
- Produces: bundle variable `${var.catalog}` (`vulnintel_dev` ใน target `dev`, `vulnintel` ใน `prod`) ที่ทุก resource ใช้

- [ ] **Step 1: เขียน `databricks.yml`** bundle `vulnintel`, target `dev` (`mode: development`, default) และ `prod` (`mode: production`), variable `catalog`, `run_as` ตามผล S4a
- [ ] **Step 2: เขียน `resources/schemas.yml`** schema `bronze`, `silver`, `gold`, `ops` และ volume `landing`, `_checkpoints` (ชื่อตามผล S2) ใน `${var.catalog}`
- [ ] **Step 3: รัน `databricks bundle validate -t dev` และ `-t prod`** คาดว่าไม่มี error
- [ ] **Step 4: รัน `databricks bundle deploy -t dev`** แล้วตรวจใน Catalog Explorer ว่า schema และ volume ถูกสร้าง
- [ ] **Step 5: Commit** `git commit -m "feat: asset bundle skeleton with dev and prod targets"`

### Task 1.3: CI และ CD workflows

**Files:**
- Create: `.github/workflows/ci.yml`, `.github/workflows/cd.yml`

- [ ] **Step 1: เขียน `ci.yml`** trigger `pull_request` และ `push` ไป `main`; job `test`: `actions/setup-java@v4` (Temurin 17), `actions/setup-python@v5` (3.12), `pip install -e .[dev]`, `ruff check .`, `ruff format --check .`, `pytest tests/unit -v`; job `bundle-validate`: `databricks/setup-cli` แล้ว `databricks bundle validate -t prod` ด้วย secret จาก S4a
- [ ] **Step 2: เขียน `cd.yml`** trigger `push` ไป `main` เมื่อ CI ผ่าน: `databricks bundle deploy -t prod`; ใส่ `concurrency: group: deploy-prod`
- [ ] **Step 3: เปิด PR ทดสอบ** ตรวจว่า CI เขียวทั้ง 2 job
- [ ] **Step 4 [คุณ]: ตั้ง branch protection บน `main`** ให้ต้องผ่าน `test` และ `bundle-validate`
- [ ] **Step 5: Commit** `git commit -m "ci: lint, unit tests, bundle validate and prod deploy"`

---

## Phase 2: Core libraries (ไม่ขึ้นกับผล spike)

### Task 2.1: เวลาและวันที่ (`to_bangkok_date`)

**Files:**
- Create: `src/vulnintel/transforms/time.py`, `tests/unit/transforms/test_time.py`

**Interfaces:**
- Produces: `to_bangkok_date(col: Column) -> Column` (DateType), `BANGKOK_TZ = "Asia/Bangkok"`

- [ ] **Step 1: เขียน test**

```python
def test_utc_evening_rolls_to_next_bangkok_day(spark):
    df = spark.createDataFrame([("2026-10-22 17:30:00",), ("2026-10-22 16:59:59",)], ["ts"])
    out = df.select(to_bangkok_date(F.to_timestamp("ts")).alias("d")).collect()
    assert [r.d.isoformat() for r in out] == ["2026-10-23", "2026-10-22"]
```

- [ ] **Step 2: รัน test** คาดว่า FAIL (`ImportError`)
- [ ] **Step 3: implement** ด้วย `F.to_date(F.from_utc_timestamp(col, BANGKOK_TZ))`
- [ ] **Step 4: รัน test** คาดว่า PASS
- [ ] **Step 5: Commit** `git commit -m "feat: bangkok date helper for silver"`

### Task 2.2: การเปรียบเทียบ version

**Files:**
- Create: `src/vulnintel/transforms/version.py`, `tests/unit/transforms/test_version.py`

**Interfaces:**
- Produces:
  - `normalize_version(col: Column) -> Column`: ตัดช่องว่างหัวท้าย, ตัด `v`/`V` นำหน้า, คืน string หรือ null
  - `version_key(col: Column) -> Column`: `array<int>` ยาวคงที่ `VERSION_PARTS = 6` (เติม 0) แต่ละส่วนคือเลขนำหน้าของส่วนนั้น (`"0-rc1"` → `0`); คืน null ถ้าส่วนแรกไม่มีตัวเลข
  - `version_in_range(v: Column, start_incl: Column, start_excl: Column, end_incl: Column, end_excl: Column) -> Column`: boolean; ขอบที่เป็น null คือไม่มีขอบด้านนั้น; `v` parse ไม่ได้ → `False`

- [ ] **Step 1: เขียน test**

```python
@pytest.mark.parametrize("a,b,expected", [
    ("7.2.10", "7.2.9", 1),
    ("7.2", "7.2.0", 0),
    ("v2.4", "2.4.0", 0),
    (" 2.4.0  ", "2.4", 0),
    ("2.4.0-rc1", "2.4.0", 0),   # suffix ถูกตัด: ตัดสินใจโดยตั้งใจ บันทึกใน dq_rules.md
    ("10.0", "9.99.99", 1),
    ("1.0.0.1", "1.0.0", 1),
])
def test_version_key_orders_numerically(spark, a, b, expected): ...
    # เทียบ version_key(normalize_version(a)) กับของ b ด้วย F.when(>,1).when(<,-1).otherwise(0)

def test_unparseable_version_is_null(spark):
    # "latest", "", None → version_key เป็น null

def test_version_in_range_respects_inclusive_and_exclusive_bounds(spark):
    # v="7.2.9": start_incl="7.0", end_excl="7.2.10" → True
    # v="7.2.10": start_incl="7.0", end_excl="7.2.10" → False
    # v="7.2.10": end_incl="7.2.10" → True
    # v="6.9": start_incl="7.0" → False
    # ไม่มีขอบเลย → True ; v="latest" → False
```

- [ ] **Step 2: รัน `pytest tests/unit/transforms/test_version.py -v`** คาดว่า FAIL
- [ ] **Step 3: implement** ใช้ `F.split` + `F.transform` + `F.regexp_extract(x, r"^(\d+)", 1)` + `try_cast` เป็น int แล้วเติม 0 ด้วย `F.concat` กับ `F.array_repeat(F.lit(0), ...)` และ `F.slice(..., 1, VERSION_PARTS)` การเทียบใช้การเทียบ array ของ Spark (lexicographic) ซึ่งถูกต้องเพราะยาวเท่ากันเสมอ
- [ ] **Step 4: รัน test** คาดว่า PASS ทั้งหมด
- [ ] **Step 5: Commit** `git commit -m "feat: numeric version comparison for CPE matching"`

### Task 2.3: DQ rules และ `validate()`

**Files:**
- Create: `src/vulnintel/dq/rules.py`, `src/vulnintel/dq/rules.yml`, `src/vulnintel/dq/validate.py`, `tests/unit/dq/test_validate.py`, `tests/unit/dq/test_rules_config.py`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class Rule: name: str; table: str; kind: Literal["key_null","invalid_format"]; expr: str; severity: Literal["warn","critical"]` โดย `expr` เป็น SQL boolean ที่เป็นจริงเมื่อแถว **ผ่าน**
  - `load_rules(table: str, path: Path | None = None) -> list[Rule]` อ่าน `rules.yml`
  - `validate(df: DataFrame, rules: list[Rule]) -> DataFrame` เพิ่มคอลัมน์ `_dq_failures: array<string>` (ชื่อกฎที่ไม่ผ่าน, ว่างถ้าผ่านหมด)
  - `split_valid_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]` clean = `size(_dq_failures) == 0` (ตัดคอลัมน์ทิ้ง), quarantine = ที่เหลือ (เก็บคอลัมน์ไว้)
- กฎตั้งต้นใน `rules.yml`: `cve.cve_id_not_null`, `cve.cve_id_format` (`cve_id rlike '^CVE-\\d{4}-\\d{4,}$'`), `epss.score_range` (`try_cast(epss as double) between 0 and 1`), `epss.date_parseable`, `kev.date_added_parseable`, `asset.asset_id_not_null`, `asset.environment_valid` (`environment in ('production','staging','development')` หลัง standardize), `asset.criticality_range` (`try_cast(criticality as int) between 1 and 4`), `asset.hostname_format`, `asset_software.asset_id_not_null`

- [ ] **Step 1: เขียน test**

```python
def test_validate_collects_every_failed_rule_name(spark):
    # แถว asset_id=None, criticality="9" → _dq_failures == ["asset_id_not_null","criticality_range"]

def test_rule_evaluating_to_null_counts_as_failure(spark):
    # criticality=None → "criticality_range" อยู่ใน _dq_failures (Review Focus 3)

def test_split_routes_rows_and_keeps_failures_column_only_in_quarantine(spark):
    # 3 แถว ไม่ผ่าน 1 → clean 2 แถวไม่มี _dq_failures, quarantine 1 แถวมี _dq_failures

def test_every_rule_in_config_has_pass_and_fail_case(spark):
    # test_rules_config.py: parametrize ทุก Rule ใน rules.yml กับ fixture
    # tests/fixtures/dq_cases.yml (pass_row, fail_row ต่อกฎ) → ต้องครบทุกชื่อกฎ
```

- [ ] **Step 2: รัน `pytest tests/unit/dq -v`** คาดว่า FAIL
- [ ] **Step 3: implement** `validate` สร้าง `F.array(*[F.when(~F.coalesce(F.expr(r.expr), F.lit(False)), F.lit(r.name)) for r in rules])` แล้ว `F.filter(..., lambda x: x.isNotNull())` (`coalesce` คือสิ่งที่ทำให้ NULL = ไม่ผ่าน)
- [ ] **Step 4: รัน test** คาดว่า PASS
- [ ] **Step 5: Commit** `git commit -m "feat: config-driven dq rules with quarantine split"`

### Task 2.4: Dedup (`row_dup`, `key_dup`) และ standardization

**Files:**
- Create: `src/vulnintel/dq/dedup.py`, `src/vulnintel/transforms/standardize.py`, `seeds/vendor_alias_map.csv`, `tests/unit/dq/test_dedup.py`, `tests/unit/transforms/test_standardize.py`

**Interfaces:**
- Produces:
  - `drop_row_dups(df: DataFrame, business_cols: list[str]) -> tuple[DataFrame, int]` คืน df ที่เหลือแถวเดียวต่อชุดค่าและจำนวนแถวที่ถูกตัด
  - `latest_per_key(df: DataFrame, keys: list[str], order_cols: list[str]) -> tuple[DataFrame, DataFrame]` คืน (แถวล่าสุดต่อ key, แถวที่ถูกแทน → audit) เรียงตาม `order_cols` จากมากไปน้อย (ค่าตั้งต้น `["last_modified", "_ingested_at"]`)
  - `standardize_environment(col: Column) -> Column`: `trim` + `lower` + map `prd|prod` → `production`, `stg|stage` → `staging`, `dev` → `development`
  - `normalize_vendor(df: DataFrame, alias_df: DataFrame, col: str = "vendor") -> DataFrame` แทนค่าด้วย `canonical` จาก `vendor_alias_map` (join แบบ `lower(trim())`), ค่าที่ไม่มีใน map ใช้ `lower(trim())`
  - `normalize_hostname(col: Column) -> Column`: `lower(trim())`
- `vendor_alias_map.csv` คอลัมน์ `alias,canonical` ตัวอย่างแถว `Microsoft Corp,microsoft`, `microsoft corporation,microsoft`

- [ ] **Step 1: เขียน test**

```python
def test_same_content_different_run_files_dedups_to_one_row(spark):
    # 2 แถวเหมือนกันทุก business col แต่ _filename ต่างกัน → เหลือ 1, dropped == 1 (Review Focus 4)

def test_latest_per_key_keeps_newest_last_modified_and_audits_rest(spark):
    # CVE-2026-0001 สองแถว last_modified ต่างกัน → kept มีแถวใหม่, superseded มีแถวเก่า

def test_latest_per_key_breaks_ties_by_ingested_at(spark): ...

def test_environment_variants_map_to_canonical(spark):
    # "prd", " Production  ", "PROD" → "production"; "qa" → "qa" (ให้ validate จับเป็น invalid)

def test_vendor_alias_and_unknown_vendor(spark):
    # "Microsoft Corp" → "microsoft"; " Fortinet " → "fortinet"
```

- [ ] **Step 2: รัน test** คาดว่า FAIL
- [ ] **Step 3: implement** `latest_per_key` ด้วย `Window.partitionBy(keys).orderBy(*[F.col(c).desc_nulls_last() for c in order_cols])` + `row_number()`
- [ ] **Step 4: รัน test** คาดว่า PASS
- [ ] **Step 5: Commit** `git commit -m "feat: row and key dedup plus standardization rules"`

### Task 2.5: `BronzeIngestor`

**Files:**
- Create: `src/vulnintel/ingestion/bronze_ingestor.py`, `src/vulnintel/ingestion/sources.yml`, `src/vulnintel/ingestion/run_bronze.py`, `tests/unit/ingestion/test_bronze_ingestor.py`, `tests/fixtures/landing/nvd_sample.json`

**Interfaces:**
- Produces:
  - `@dataclass(frozen=True) class SourceConfig: name: str; source_glob: str; file_format: Literal["json","csv"]; target_table: str; header: bool = False`
  - `load_source_config(name: str, catalog: str, path: Path | None = None) -> SourceConfig`
  - `class BronzeIngestor: __init__(self, spark: SparkSession, config: SourceConfig, checkpoint_root: str)`
    - `reader_options(self) -> dict[str, str]`
    - `read_stream_from_file(self) -> DataFrame`
    - `add_metadata_columns(df: DataFrame) -> DataFrame` (staticmethod)
    - `write_stream_to_bronze_table(self, df: DataFrame) -> StreamingQuery`
    - `run(self) -> None` (read → add metadata → write → `awaitTermination()`)
  - CLI: `python -m vulnintel.ingestion.run_bronze --source nvd --catalog vulnintel`
- `sources.yml`: `nvd` → glob `/Volumes/{catalog}/ops/landing/nvd/*/*/*/*.json`, table `{catalog}.bronze.nvd_cve_raw`; `epss` → `epss_score_raw`; `kev` → `kev_raw` (และ `cmdb_assets`, `cmdb_asset_software` แบบ csv ถ้า G0 เลือกทาง B)

- [ ] **Step 1: เขียน test**

```python
def test_reader_options_follow_bronze_rules(spark):
    opts = BronzeIngestor(spark, cfg_nvd, "/Volumes/c/ops/_checkpoints").reader_options()
    assert opts["cloudFiles.inferColumnTypes"] == "false"
    assert opts["cloudFiles.schemaEvolutionMode"] == "addNewColumns"
    assert opts["rescuedDataColumn"] == "_rescued_data"
    assert opts["cloudFiles.schemaLocation"] == "/Volumes/c/ops/_checkpoints/nvd_cve_raw"
    assert opts["cloudFiles.format"] == "json"

def test_csv_source_sets_header_option(spark): ...

def test_metadata_columns_added_without_reusing_metadata_name(spark, tmp_path):
    # batch read fixture ด้วย spark.read.json(path).select("*", "_metadata")
    # → มี _load_dttm, _filename, _file_path, _file_size, _file_modification_time
    # → ไม่มีคอลัมน์ชื่อ "_metadata" ในผลลัพธ์ และไม่มีคอลัมน์ที่เป็น DateType

def test_load_source_config_substitutes_catalog(): ...
```

- [ ] **Step 2: รัน test** คาดว่า FAIL
- [ ] **Step 3: implement** `write_stream_to_bronze_table` ใช้ `outputMode("append")`, `option("mergeSchema","true")`, `option("checkpointLocation", f"{checkpoint_root}/{table_name}")`, `trigger(availableNow=True)`, `toTable(config.target_table)` ไม่มี global state
- [ ] **Step 4: รัน test** คาดว่า PASS (ส่วน `cloudFiles` จริงพิสูจน์แล้วใน S2 และใน integration run Task 6.4)
- [ ] **Step 5: Commit** `git commit -m "feat: reusable auto loader bronze ingestor"`

---

## Phase 3: Extract jobs (GitHub Actions)

ทุก task ใน phase นี้ใช้ TDD cycle มาตรฐาน fixture ของ response จริงเก็บใน `tests/fixtures/api/` (บันทึกครั้งเดียวด้วยมือแล้ว commit) HTTP mock ด้วย `respx`

### Task 3.1: `volume_uploader` และ HTTP client กลาง

**Files:** `jobs/common/http.py`, `jobs/extract/volume_uploader.py`, `tests/unit/jobs/test_http.py`, `tests/unit/jobs/test_volume_uploader.py`

**Interfaces (Produces):**
- `get_with_backoff(client: httpx.Client, url: str, params: dict, *, max_attempts: int = 5, base_delay: float = 2.0) -> httpx.Response` retry เมื่อ 429/5xx/timeout, เคารพ header `Retry-After`, ไม่ retry 4xx อื่น
- `class RateLimiter(max_calls: int, period_s: float)` มีเมธอด `wait()` (NVD: `RateLimiter(50, 30)` ถ้ามี key, `RateLimiter(5, 30)` ถ้าไม่มี)
- `landing_path(catalog: str, source: str, run_date: date, run_id: str, part: int) -> str` → `/Volumes/{catalog}/ops/landing/{source}/YYYY/MM/DD/{source}_{run_id}_{part}.json`
- `upload_ndjson(ws: WorkspaceClient, path: str, records: Iterable[dict]) -> int` เขียน NDJSON ด้วย `ws.files.upload(path, buf, overwrite=False)` คืนจำนวนแถว
- `delete_older_than(ws: WorkspaceClient, root: str, days: int = 30, today: date) -> int` (ใช้ถ้า G0 ไม่เลือก `cleanSource`)

**Tests:** `test_retries_on_429_honoring_retry_after`, `test_does_not_retry_404`, `test_gives_up_after_max_attempts`, `test_rate_limiter_blocks_51st_call_within_window` (fake clock), `test_landing_path_format` (`nvd/2026/10/22/nvd_123_0.json`), `test_upload_writes_one_json_object_per_line`, `test_delete_older_than_keeps_day_30_deletes_day_31`

### Task 3.2: NVD extract

**Files:** `jobs/extract/nvd.py`, `jobs/extract/watermark.py`, `tests/unit/jobs/test_nvd.py`, `tests/fixtures/api/nvd_page_0.json`, `nvd_page_1.json`

**Interfaces:**
- Consumes: `get_with_backoff`, `RateLimiter`, `landing_path`, `upload_ndjson`
- Produces:
  - `read_watermark(conn, catalog: str) -> datetime | None` = `max(last_modified)` ใน `bronze.nvd_cve_raw` ลบ 2 ชั่วโมง; ตารางไม่มีหรือว่าง → `None`
  - `date_windows(start: datetime, end: datetime, max_days: int = 120) -> list[tuple[datetime, datetime]]`
  - `fetch_modified(client, start, end) -> Iterator[dict]` ใช้ `lastModStartDate`/`lastModEndDate`, `resultsPerPage=2000`, `startIndex` วนจนครบ `totalResults`
  - `fetch_by_ids(client, cve_ids: list[str]) -> Iterator[dict]` (CVE ใน KEV ที่เก่ากว่า 3 ปี, param `cveId`)
  - `slim_cve(item: dict) -> dict` เก็บเฉพาะ `id`, `published`, `lastModified`, `vulnStatus`, `descriptions[lang=en]`, `metrics`, `weaknesses`, `configurations`
  - `main(run_id: str, catalog: str) -> None` ไม่มี watermark → backfill จาก `today - 3 ปี`
- ตรวจชั้น 1: response ต้องมี `vulnerabilities` และ `totalResults` ไม่อย่างนั้น raise `ContractError`

**Tests:** `test_windows_split_three_years_into_120_day_chunks`, `test_pagination_follows_total_results`, `test_watermark_none_triggers_three_year_backfill` (Review Focus 2), `test_watermark_subtracts_two_hours`, `test_slim_cve_drops_unused_fields`, `test_missing_vulnerabilities_key_raises_contract_error`

### Task 3.3: KEV extract

**Files:** `jobs/extract/kev.py`, `tests/unit/jobs/test_kev.py`, `tests/fixtures/api/kev_catalog.json`

**Interfaces (Produces):**
- `KEV_URL = "https://www.cisa.gov/sites/default/files/feeds/known_exploited_vulnerabilities.json"`
- `row_hash(entry: dict) -> str` (sha256 ของ JSON ที่ `sort_keys=True`)
- `changed_entries(catalog: list[dict], known_hashes: set[str]) -> list[dict]`
- `read_known_hashes(conn, catalog: str) -> set[str]` อ่านจาก `silver.kev` (`_row_hash`) ตารางไม่มี → `set()` ซึ่งทำให้เขียนทั้ง catalog
- `main(run_id, catalog) -> list[str]` คืน `cve_id` ทั้งหมดใน KEV ให้ Task 3.4 และ 3.2 ใช้

**Tests:** `test_unchanged_catalog_writes_nothing`, `test_due_date_change_is_detected`, `test_empty_known_hashes_writes_full_catalog`, `test_missing_vulnerabilities_key_raises_contract_error`

### Task 3.4: EPSS extract

**Files:** `jobs/extract/epss.py`, `tests/unit/jobs/test_epss.py`, `tests/fixtures/api/epss_batch.json`

**Interfaces (Produces):**
- `read_watchlist(conn, catalog: str) -> list[str]` จาก `gold.cve_watchlist`; ตารางไม่มี → `[]`
- `resolve_watchlist(watchlist: list[str], kev_ids: list[str]) -> list[str]` (union, เรียง, ไม่ซ้ำ) ถ้า watchlist ว่างใช้ KEV อย่างเดียว
- `batches(ids: list[str], size: int = 100) -> list[list[str]]`
- `fetch_scores(client, ids: list[str]) -> Iterator[dict]` (`api.first.org/data/v1/epss?cve=...`) เก็บ `cve`, `epss`, `percentile`, `date` เป็น string ตามที่ API ส่งมา

**Tests:** `test_first_run_uses_kev_only` (Review Focus 2), `test_batches_of_100`, `test_scores_kept_as_strings`, `test_contract_error_on_missing_data_key`

### Task 3.5: Extract CLI และ contract tests

**Files:** `jobs/extract/__main__.py`, `tests/contract/test_live_apis.py`, `.github/workflows/contract-tests.yml`

- `python -m jobs.extract --source {nvd|kev|epss} --run-id $GITHUB_RUN_ID --catalog vulnintel` exit code ≠ 0 เมื่อ source นั้นล้มเหลว (ให้ step ของ source นั้นแดงโดยไม่ล้ม source อื่น)
- Contract test ยิง request จริงขนาดเล็ก (NVD 1 CVE, EPSS 1 CVE, KEV ทั้งไฟล์) ตรวจ key ที่ `slim_cve`/`fetch_scores`/`row_hash` ใช้ รันรายสัปดาห์ (`cron: "23 2 * * 1"`) และ `workflow_dispatch`

---

## Phase 4: Inventory simulator และเส้นทาง CMDB

### Task 4.1: Simulator

**Files:** `jobs/simulator/config.yml`, `jobs/simulator/catalog.py`, `jobs/simulator/simulate.py`, `jobs/simulator/faults.py`, `jobs/simulator/db.py`, `seeds/product_catalog.csv`, `tests/unit/simulator/test_simulate.py`, `tests/unit/simulator/test_faults.py`

**Interfaces (Produces):**
- `rng_for(run_date: date) -> random.Random` seed = `int(run_date.strftime("%Y%m%d"))`
- `plan_changes(state: InventoryState, run_date: date, cfg: SimConfig, vulns: list[OpenVuln] | None) -> list[Change]` ฟังก์ชันบริสุทธิ์ ไม่แตะ DB; `vulns is None` (ยังไม่มี gold) → ไม่มี change ประเภท patch
- `inject_faults(changes: list[Change], rng, fault_rate: float = 0.03) -> list[Change]` ชนิด fault ตาม spec: `asset_id` null, hostname ผิดรูปแบบ, environment สะกดผิด, criticality นอก 1-4, version ปนรูปแบบ, vendor alias, software ซ้ำ
- `apply_changes(conn, changes) -> None` (ทำใน transaction เดียว)
- `SimConfig` มี `max_assets = 250`, อัตราเพิ่ม/ปลด/ย้าย, `patch_prob_by_tier = {T1: 0.5, T2: 0.25, T3: 0.1, T4: 0.03}`, `slow_teams` และตัวคูณความช้า, `fault_rate = 0.03`
- `product_catalog.csv` 40-60 แถว `vendor,product,vulnerable_version,fixed_version` ใช้ชื่อจริงจาก KEV/CPE

**Tests:** `test_same_date_same_changes` (Review Focus 4), `test_different_dates_differ`, `test_asset_count_never_exceeds_cap`, `test_no_patches_when_gold_missing` (Review Focus 2), `test_fault_rate_close_to_config` (10,000 changes → 0.03 ± 0.005), `test_slow_team_patches_less_than_normal_team`, `test_patch_moves_to_fixed_version_from_cpe_upper_bound`

### Task 4.2A: CDC (ถ้า G0 เลือกทาง A)

**Files:** `resources/ingestion_cmdb.yml`, `docs/runbook.md` (หัวข้อ replication slot)
- ingestion pipeline ด้วย connection จาก S1 ตาราง `assets` → `bronze.cmdb_assets`, `asset_software` → `bronze.cmdb_asset_software` เปิด SCD type 2
- **ตรวจ:** `databricks bundle deploy -t dev` แล้วรัน simulator 1 ครั้ง + refresh pipeline → bronze มีแถวที่เปลี่ยน

### Task 4.2B: Snapshot (ถ้า G0 เลือกทาง B)

**Files:** `jobs/simulator/snapshot_export.py`, `tests/unit/simulator/test_snapshot_export.py`, เพิ่ม `cmdb_assets`/`cmdb_asset_software` ใน `sources.yml`
- `export_snapshot(conn, ws, catalog, run_date, run_id) -> dict[str, int]` dump 2 ตารางเป็น CSV (header, ทุกค่าเป็น text) ไป `landing_path(..., source="cmdb_assets", ...)` แทน `.json` ด้วย `.csv`
- Bronze เข้าทาง `BronzeIngestor` (task `bronze_cmdb_assets`, `bronze_cmdb_asset_software`) รวมกับ 3 source เดิมเป็น 5 task พร้อมกัน = เพดาน 5 พอดี
- **Tests:** `test_snapshot_contains_every_row_including_faulty_ones`, `test_snapshot_id_from_run_id_is_monotonic`

---

## Phase 5: ETL pipeline (silver + gold) และ `dq_check`

ทุก transform อยู่ใน `src/vulnintel/transforms/` เป็น `DataFrame → DataFrame` และใช้ TDD cycle มาตรฐาน ไฟล์ใน `src/vulnintel/pipelines/` แค่เรียกฟังก์ชันและใส่ decorator ของ `dp` (ไม่มี unit test ของตัวเอง ทดสอบผ่าน integration run)

### Task 5.1: Silver transforms ของ threat intel

**Files:** `src/vulnintel/transforms/cve.py`, `src/vulnintel/transforms/epss.py`, `src/vulnintel/transforms/kev.py`, tests คู่กันใน `tests/unit/transforms/`, fixture จาก Task 3.2-3.4

**Interfaces (Produces):**
- `parse_nvd_raw(df) -> DataFrame` จาก bronze string → คอลัมน์ `cve_id`, `published`, `last_modified` (timestamp), `vuln_status`, `description`, `cwe_ids: array<string>`, `metrics_json`, `configurations_json`, `_ingested_at` (= `_load_dttm`)
- `coalesce_cvss(df) -> DataFrame` เพิ่ม `cvss_score: double`, `cvss_version`, `score_source ∈ {"NVD","CNA",null}`, `enrichment_status` ลำดับความสำคัญ: v4.0 → v3.1 → v3.0 ภายในแต่ละ version ใช้ `type="Primary"` (NIST) ก่อน `Secondary` (CNA)
- `drop_rejected(df) -> DataFrame` ตัด `vuln_status = "Rejected"`
- `explode_cpe(df) -> DataFrame` → `cve_affected_product(cve_id, vendor, product, version_exact, start_incl, start_excl, end_incl, end_excl, vulnerable)` เฉพาะ `vulnerable = true`; CVE ที่ไม่มี configurations → 1 แถวที่ `vendor` เป็น null และ `matchable = false`
- `parse_epss(df) -> DataFrame` → `cve_id`, `epss: double`, `percentile: double`, `score_date: date`
- `parse_kev(df) -> DataFrame` → `cve_id`, `vendor_project`, `product`, `date_added: date`, `due_date: date`, `known_ransomware: boolean`, `_row_hash`

**Tests (สำคัญ):** `test_nist_primary_wins_over_cna`, `test_cna_used_when_nvd_not_enriched_and_flagged`, `test_v40_preferred_over_v31`, `test_rejected_dropped`, `test_cpe_23_string_split_into_vendor_product_version`, `test_unenriched_cve_marked_unmatchable`, `test_epss_string_to_double`, `test_kev_ransomware_known_to_bool`

### Task 5.2: Silver pipeline (`dp`) พร้อม quarantine และ SCD

**Files:** `src/vulnintel/pipelines/silver.py`, `src/vulnintel/transforms/asset.py`, `tests/unit/transforms/test_asset.py`, `resources/etl_pipeline.yml`

**Interfaces:**
- Consumes: Task 2.1-2.4, 5.1, `load_rules()`
- Produces ตาราง silver: `dim_asset`, `asset_software` (+ `version_key`), `cve` (SCD2 ผ่าน `dp.create_auto_cdc_flow(keys=["cve_id"], sequence_by="last_modified", stored_as_scd_type=2)`), `cve_affected_product`, `epss_daily`, `kev` (SCD1 ผ่าน AUTO CDC), `*_quarantine` และ `*_dup_audit`
- แบบแผนต่อตาราง: view `<t>_validated` = `validate(standardize(parse(bronze)), load_rules(t))` → `<t>` (clean) และ `<t>_quarantine` จาก view เดียวกัน; ตาราง clean มี `@dp.expect_or_fail("no_dq_failures_leaked", ...)` สำหรับ key ที่ต้องไม่เป็น null
- `dim_asset`/`asset_software` ตาม G0: ทาง A อ่าน SCD2 จาก bronze ของ connector; ทาง B ใช้ `dp.create_auto_cdc_from_snapshot_flow(keys=["id"], stored_as_scd_type=2)` ตามที่พิสูจน์ใน S1
- `resources/etl_pipeline.yml`: serverless, catalog `${var.catalog}`, `libraries` ชี้ `src/vulnintel/pipelines/`

**Tests:** `test_asset_standardize_then_validate_routes_bad_environment_to_quarantine`, `test_asset_software_version_key_populated`, `test_software_dup_within_asset_counted_as_row_dup`
**ตรวจ:** `databricks bundle deploy -t dev` + run pipeline บน fixture เล็ก ตาราง clean และ quarantine มีแถวตามคาด

### Task 5.3: Gold transforms และ gold pipeline

**Files:** `src/vulnintel/transforms/matching.py`, `priority.py`, `metrics.py`, `threat.py`, tests คู่กัน, `src/vulnintel/pipelines/gold.py`, `src/vulnintel/config/policy.yml`

**Interfaces (Produces):**
- `match_cpe_to_inventory(software: DataFrame, affected: DataFrame) -> DataFrame` join `vendor` + `product` แล้วกรองด้วย `version_in_range` หรือ `version_exact` → `asset_vulnerability(asset_id, software_id, cve_id, first_seen, resolved_at, status ∈ {"open","resolved"})`
- `assign_priority_tier(df, epss_threshold: float = 0.1, cvss_threshold: float = 7.0) -> DataFrame` เพิ่ม `tier ∈ {"T1","T2","T3","T4"}` ตามลำดับใน Global Constraints และ `priority_score` (tier ถ่วงด้วย `criticality` และ `internet_facing`)
- `compute_sla_due(df, sla_days: dict[str,int] = {"T1":7,"T2":14,"T3":30,"T4":90}) -> DataFrame` เพิ่ม `due_date`, `is_overdue`
- `open_vuln_daily(asset_vuln: DataFrame, spine_start: date, spine_end: date) -> DataFrame` จาก SCD2 + date spine (`F.sequence`)
- `remediation_metrics(asset_vuln) -> DataFrame` MTTR (วัน) และ % SLA ต่อ `owner_team`
- `epss_movers(epss_daily, kev, as_of: date, window_days: int = 7, min_delta: float = 0.05) -> DataFrame`
- `time_to_kev(cve, kev) -> DataFrame`, `vendor_risk_scorecard(cve, affected, kev, epss) -> DataFrame`, `nvd_enrichment_gap(cve) -> DataFrame`, `cve_watchlist(asset_vuln, kev) -> DataFrame`
- ค่า threshold, SLA, `min_delta` อยู่ใน `policy.yml`

**Tests (สำคัญ):** `test_kev_beats_high_epss` (T1), `test_epss_0_1_and_cvss_7_is_t2_boundary_inclusive`, `test_cvss_7_without_epss_is_t3`, `test_null_cvss_falls_to_t4`, `test_due_date_t1_is_7_days`, `test_version_7_2_10_not_matched_by_end_excl_7_2_10`, `test_open_vuln_daily_recomputes_from_history`, `test_mttr_only_counts_resolved`, `test_epss_mover_excludes_kev`, `test_watchlist_union_inventory_and_kev`

### Task 5.4: `dq_daily_summary`, `pipeline_health` และ `dq_check`

**Files:** `src/vulnintel/dq/summary.py`, `src/vulnintel/dq/check.py`, `src/vulnintel/dq/thresholds.yml`, `tests/unit/dq/test_summary.py`, `tests/unit/dq/test_check.py`

**Interfaces (Produces):**
- `dq_daily_summary(quarantines: dict[str, DataFrame], cleans: dict[str, DataFrame], run_date: date) -> DataFrame` (ตาราง, กฎ, จำนวน, สัดส่วน)
- `pipeline_health(bronze_tables: dict[str, DataFrame], now: datetime) -> DataFrame` (`source`, `last_ingested_at`, `age_hours`, `is_stale` เมื่อ > 36)
- `@dataclass class CheckResult: level: Literal["ok","warn","critical"]; findings: list[str]`
- `evaluate(summary_rows, health_rows, row_counts_today, row_counts_yesterday, orphan_assets: int, thresholds) -> CheckResult` critical เมื่อ quarantine > 0.10, ความสด > 36 ชั่วโมง, หรือ orphan asset ใน gold > 0; warn เมื่อจำนวนแถวลดลงเกิน `row_drop_warn` (ค่าตั้งต้น 0.2)
- `main()` (Python task): เขียนผลลง `ops.dq_check_results` แล้ว `raise SystemExit(1)` ถ้า critical (Discord ส่งจาก GitHub Actions เพราะ Databricks ออกเน็ตไม่ได้)

**Tests:** `test_quarantine_above_10_percent_is_critical`, `test_quarantine_exactly_10_percent_is_not_critical`, `test_nvd_stale_30_hours_is_ok_but_flagged` (Review Focus 5), `test_stale_37_hours_is_critical`, `test_orphan_asset_is_critical`, `test_row_drop_is_warn`

### Task 5.5: Lakeflow Job `daily_job`

**Files:** `resources/daily_job.yml`
- Task ตามลำดับ: (ทาง A) `refresh_cmdb_ingestion` → `bronze_nvd`, `bronze_epss`, `bronze_kev` (+ `bronze_cmdb_*` ทาง B) พร้อมกัน `max_retries: 1`, python task เรียก `vulnintel.ingestion.run_bronze` ด้วย serverless environment ที่ติดตั้ง wheel ของ bundle → `etl_pipeline` → `dq_check`
- ไม่มี schedule ใน Databricks (GitHub Actions เป็นคนสั่ง)
- **ตรวจ:** `databricks bundle run daily_job -t dev` สำเร็จบนข้อมูลชุดเล็ก จำนวน task ที่รันพร้อมกันไม่เกิน 5

---

## Phase 6: Export, orchestration และการเฝ้าระวัง

### Task 6.1: BigQuery export

**Files:** `jobs/export/bigquery_export.py`, `jobs/export/tables.yml`, `tests/unit/jobs/test_bigquery_export.py`

**Interfaces (Produces):**
- `EXPORT_TABLES` จาก `tables.yml`: gold ทั้ง 11 ตาราง (`asset_vulnerability`, `patch_priority_queue`, `open_vuln_daily`, `remediation_metrics`, `epss_movers`, `time_to_kev`, `vendor_risk_scorecard`, `nvd_enrichment_gap`, `cve_watchlist`, `dq_daily_summary`, `pipeline_health`) ห้ามมี silver
- `read_gold_arrow(conn, catalog: str, table: str) -> pyarrow.Table` (`cursor.fetchall_arrow()`)
- `load_truncate(bq: bigquery.Client, dataset: str, table: str, data: pyarrow.Table) -> None` Parquet + `WRITE_TRUNCATE` แล้วกันหมดอายุตามวิธีที่ G0 เลือกจาก S4b
- `main(catalog, dataset) -> None` ล้มทั้ง job ถ้าตารางใดล้ม (ไม่ export ครึ่ง ๆ กลาง ๆ โดยเงียบ)

**Tests:** `test_only_gold_tables_listed`, `test_load_uses_parquet_and_write_truncate` (mock client), `test_expiry_refresh_applied_after_load`, `test_failure_on_one_table_exits_nonzero`

### Task 6.2: `daily-run.yml`

**Files:** `.github/workflows/daily-run.yml`
- `schedule: cron: "37 3 * * *"` (10:37 น. เวลาไทย นาทีเลขคี่) + `workflow_dispatch`; `concurrency: group: databricks-pipelines, cancel-in-progress: false`
- Jobs: `simulate_inventory` → `extract` (matrix `source: [nvd, kev, epss]`, `fail-fast: false`) → `run_databricks_job` (`if: always() && needs.simulate_inventory.result == 'success'`, `databricks bundle run daily_job -t prod`) → `export_to_bigquery` (`if: needs.run_databricks_job.result == 'success'`) → `ping_healthcheck` (success ping) และ step `notify_failure` (`if: failure()`) ส่ง Discord + ping `/fail`
- หลัง `run_databricks_job` อ่าน `ops.dq_check_results` ผ่าน SQL warehouse ถ้า level = warn ส่ง Discord แต่ไม่หยุด
- **ตรวจ (Review Focus 5):** รัน `workflow_dispatch` โดยตั้ง env `NVD_BASE_URL` ชี้ host ที่ไม่มีอยู่ → step NVD แดง, KEV/EPSS เขียว, Databricks job ยังรัน, `pipeline_health` แสดง NVD ตาม `age_hours`

### Task 6.3: `keepalive.yml`, retention และ healthchecks

**Files:** `.github/workflows/keepalive.yml`, `.github/workflows/volume-retention.yml` (ถ้า G0 ไม่ใช้ `cleanSource`), `docs/runbook.md`
- keepalive รายสัปดาห์ (`cron: "41 1 * * 0"`) เรียก `gh api -X PUT repos/{owner}/{repo}/actions/workflows/daily-run.yml/enable` และเตือน PAT ใกล้หมดอายุถ้า G0 เลือก PAT
- retention รายสัปดาห์เรียก `delete_older_than(..., days=30)`
- [คุณ] ตั้ง check บน healthchecks.io: period 1 วัน, grace 2 ชั่วโมง (รวม 26 ชั่วโมง), ช่องทางอีเมล; ใส่ secret `HEALTHCHECK_URL`

### Task 6.4: Integration run บน `dev`

**Files:** `.github/workflows/integration.yml`, `tests/fixtures/integration/` (NDJSON ชุดเล็ก)
- `workflow_dispatch` เท่านั้น ใช้ concurrency group เดียวกับ daily-run: deploy `-t dev` → upload fixture ด้วย `upload_ndjson` → `bundle run daily_job -t dev` → ตรวจว่าทุกตาราง gold มีแถว > 0 และ quarantine มีแถวจาก fixture ที่เสียโดยตั้งใจ

### Task 6.5: Looker Studio dashboard

**Files:** `docs/dashboard.md` (ลิงก์, data source ต่อหน้า, ภาพหน้าจอ)
- [คุณ + Claude] สร้าง 4 หน้าตามตาราง "Dashboard 4 หน้า" ใน spec ทุก metric มาจาก gold ห้ามมี calculated field ที่เป็น business logic, เปิด cache, footer ทุกหน้ามีข้อความ NVD และ "ข้อมูลบริษัทเป็นของสมมติ", หน้า Data Quality แสดงสถานะ "ข้อมูลล่าช้า" จาก `pipeline_health`

---

## Phase 7: เอกสารและ burn-in

### Task 7.1: ADR และเอกสาร

**Files:** `docs/adr/001-no-airflow.md` ถึง `005-simulated-inventory.md` (002 ทำแล้วใน G0), `docs/data_dictionary.md`, `docs/dq_rules.md`, `docs/runbook.md`, `README.md`
- `dq_rules.md` สร้างจาก `rules.yml` ด้วยสคริปต์ `scripts/gen_dq_catalog.py` และ CI ตรวจว่าไม่ล้าสมัย (`test_dq_catalog_up_to_date`)
- README: ภาพ architecture, ลิงก์ dashboard, คำถามธุรกิจ 5 ข้อ, ตารางค่าใช้จ่าย 0 บาทพร้อมโควตา, badge จำนวนวันที่รันต่อเนื่อง, อัตราฉีดข้อมูลเสีย 3%, ข้อความ NVD
- runbook: ทุกแถวในตาราง "การจัดการเมื่อเกิดปัญหา" ของ spec + ต่ออายุ PAT/rotate key ตามผล S4

### Task 7.2: Burn-in 30 วัน

- เปิด `daily-run` บน prod แล้วติดตาม 30 วัน เป้าหมาย ≥ 90% ของวันสำเร็จ (≥ 27/30) บันทึกทุกวันที่ล้มใน `docs/burn_in.md` พร้อมสาเหตุ
- หลังวันที่ 14-28 ตรวจกราฟ MTTR และแนวโน้มว่ามีความหมาย (ตาม "ข้อจำกัด" ของ simulator ใน spec)

---

## Spec coverage

| ส่วนของ spec | Task |
| --- | --- |
| Spikes S1-S4 และแผนสำรอง | 0.2-0.7, 4.2A/4.2B |
| Bronze ingestor ทุกข้อ | 0.3, 2.5, 5.5 |
| แหล่งข้อมูล NVD/EPSS/KEV | 3.2-3.5 |
| Silver + quarantine + SCD | 2.3, 2.4, 5.1, 5.2 |
| Gold 10 ตาราง + `asset_vulnerability` | 5.3, 5.4 |
| Version comparison | 2.2 |
| Simulator | 4.1 |
| DQ 5 ชั้น + threshold | 3.2-3.4 (ชั้น 1), 2.5 (ชั้น 2), 5.2 (ชั้น 3-4), 5.4 (ชั้น 5) |
| Orchestration + error handling | 5.5, 6.2, 6.3 |
| CI/CD + bundle | 1.1-1.3 |
| Export + dashboard + NVD notice | 6.1, 6.5, 7.1 |
| Testing strategy | ทุก phase, 3.5 (contract), 6.4 (integration) |
| ADR + เอกสาร | 0.7, 7.1 |
| เกณฑ์ความสำเร็จ 90% / 30 วัน | 7.2 |
