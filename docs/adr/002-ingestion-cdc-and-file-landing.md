# ADR-002: Ingestion — snapshot SCD2 สำหรับ CMDB และ file landing + Auto Loader สำหรับข้อมูลภายนอก

- สถานะ: Accepted (Gate G0, 2026-10-26)
- แทนที่: ฉบับร่างใน design spec ("CDC สำหรับข้อมูลภายใน, file landing + Auto Loader สำหรับข้อมูลภายนอก")
- หลักฐาน: `docs/spikes/S1.md`, `docs/spikes/S2.md`, `docs/spikes/SUMMARY.md`

## บริบท

ข้อมูลเข้า Databricks Free Edition มีสองแบบ
- **ข้อมูลภายใน (mock CMDB):** ตาราง `assets` และ `asset_software` ใน Neon Postgres เปลี่ยนทุกวัน ต้องเก็บประวัติแบบ SCD2 และต้องเห็นแถวที่ถูกลบ (asset ที่ปลดระวาง)
- **ข้อมูลภายนอก (NVD, EPSS, KEV):** ต้องดึงจาก internet ซึ่ง Free Edition จำกัด outbound งานดึงจึงรันบน GitHub Actions

ข้อจำกัดที่พบใน spike:
- S1: Lakeflow Connect PostgreSQL connector มีให้ใช้บน Free Edition แต่ตัวเลือก **Change data capture ถูก disable** เหลือเพียง Query based capture (Preview) ซึ่งดึงตาม cursor column จึงไม่เห็น hard delete
- S2: Files API upload และ Auto Loader `availableNow` ใช้ได้บน serverless รวมถึง `cloudFiles.cleanSource`

## ทางเลือกที่พิจารณา

| ทางเลือก | ข้อดี | ข้อเสีย |
| --- | --- | --- |
| A. Lakeflow Connect CDC | SCD2 จาก connector ตรงๆ, ใกล้เคียงงานจริง | **ใช้ไม่ได้บน Free Edition** (CDC disabled), ต้องเปิด logical replication ใน Neon (ย้อนกลับไม่ได้) และดูแล replication slot |
| A2. Lakeflow Connect Query based capture | managed, ไม่ต้องเขียนโค้ด ingest | ไม่เห็น hard delete, เป็น Preview, เพิ่ม pipeline อีกตัว |
| **B. Snapshot CSV → UC Volume → Auto Loader → `create_auto_cdc_from_snapshot_flow`** | ใช้ได้จริง (S1 `scd2_ok=True`), เห็น hard delete, ใช้ `BronzeIngestor` ตัวเดียวกับข้อมูลภายนอก, replay ได้จากไฟล์ ไม่ผูกกับ replication slot | เขียนโค้ด dump/ingest เอง, SCD2 อิง snapshot version ไม่ใช่เวลาที่เปลี่ยนจริง (ละเอียดแค่รายวัน), ไฟล์ snapshot เต็มตารางทุกวัน |
| C. Google Drive connector เป็น landing | ไม่ต้องใช้ Files API | ไม่จำเป็น เพราะ S2 ผ่าน |

## การตัดสินใจ

1. **CMDB ใช้ทาง B.** simulator บน GitHub Actions dump แต่ละตารางเป็น CSV ลง `/Volumes/<catalog>/ops/landing/cmdb_<table>/YYYY/MM/DD/cmdb_<table>_<version>_0.csv` โดย `<version>` เป็นเวลา dump แบบ `yyyyMMddHHmmss` (UTC) ซึ่งเพิ่มขึ้นเสมอ
   - `BronzeIngestor` อ่านเข้า `bronze.cmdb_<table>_snapshot` แบบ append โดยมี `snapshot_id` และ metadata ของไฟล์ติดมาด้วย
   - ETL pipeline ใช้ `dp.create_auto_cdc_from_snapshot_flow(stored_as_scd_type=2, keys=["id"])` กับ `next_snapshot(latest_version)` ที่เลือก `snapshot_id` ถัดไปจาก bronze และ cast type ก่อนส่งเข้า flow
2. **ข้อมูลภายนอกใช้ file landing + Auto Loader ตามเดิม** ใน UC Volume ผ่าน Files API (`overwrite=False`, ชื่อไฟล์ไม่ซ้ำต่อ run attempt) และ Auto Loader `availableNow`, `inferColumnTypes=false`, `schemaEvolutionMode=addNewColumns`, `rescuedDataColumn`, task `max_retries: 1`
3. **ลบไฟล์เก่าด้วย `cloudFiles.cleanSource=DELETE`, `retentionDuration=30 days`** ไม่มี job ลบไฟล์แยก
4. **ไม่เปิด logical replication ใน Neon**

## ผลที่ตามมา

- `__START_AT` และ `__END_AT` ใน silver เป็น snapshot version (bigint) ไม่ใช่ timestamp ต้องมี view map `snapshot_id` → เวลา สำหรับ query แบบ as-of และ `open_vuln_daily`
- ความละเอียดของประวัติ CMDB เป็นรายวัน (การเปลี่ยนหลายครั้งในวันเดียวเห็นแค่สถานะสุดท้าย) ซึ่งพอสำหรับ design ที่รันวันละครั้ง
- daily job มี bronze task 5 ตัวพร้อมกัน (nvd, epss, kev, cmdb_assets, cmdb_asset_software) เต็มเพดาน 5 task ถ้าต้องเพิ่ม source ให้รวม CMDB เป็น task เดียว
- ความเสี่ยงเรื่อง "PostgreSQL connector เป็น Public Preview" และ "Neon ลบ replication slot หลัง ~40 ชั่วโมง" หายไป
- ถ้าภายหลังย้ายไป workspace ที่มี classic compute สามารถเปลี่ยนเป็นทาง A ได้โดยเปลี่ยนแค่ชั้น bronze → silver ของ CMDB ส่วน gold ไม่ต้องแก้
