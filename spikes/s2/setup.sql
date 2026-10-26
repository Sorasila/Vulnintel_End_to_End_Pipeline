-- spike S2/S4a: สร้าง catalog, schema, volume สำหรับ Phase 0
-- รันใน Databricks SQL editor (ย้ายมาจาก Task 0.3 Step 1 เพราะ S4a ต้องใช้ volume ก่อน)

CREATE CATALOG IF NOT EXISTS vulnintel_dev;

CREATE SCHEMA IF NOT EXISTS vulnintel_dev.bronze;
CREATE SCHEMA IF NOT EXISTS vulnintel_dev.silver;
CREATE SCHEMA IF NOT EXISTS vulnintel_dev.gold;
CREATE SCHEMA IF NOT EXISTS vulnintel_dev.ops;

CREATE VOLUME IF NOT EXISTS vulnintel_dev.ops.landing;

-- ถ้าชื่อที่ขึ้นต้นด้วย "_" ถูกปฏิเสธ ให้ใช้ `checkpoints` แทน
-- แล้วบันทึกไว้ใน docs/spikes/S2.md
CREATE VOLUME IF NOT EXISTS vulnintel_dev.ops._checkpoints;

-- สิทธิ์ของ service principal (S4a): เอา comment ออกแล้วรันหลังสร้าง SP
-- แทน <sp-application-id> ด้วย Application ID ของ service principal (ไม่ใช่ secret)
-- GRANT USE CATALOG    ON CATALOG vulnintel_dev TO `<sp-application-id>`;
-- GRANT USE SCHEMA     ON CATALOG vulnintel_dev TO `<sp-application-id>`;
-- GRANT READ VOLUME    ON CATALOG vulnintel_dev TO `<sp-application-id>`;
-- GRANT WRITE VOLUME   ON CATALOG vulnintel_dev TO `<sp-application-id>`;
-- GRANT SELECT         ON CATALOG vulnintel_dev TO `<sp-application-id>`;
-- GRANT MODIFY         ON CATALOG vulnintel_dev TO `<sp-application-id>`;
-- GRANT CREATE TABLE   ON CATALOG vulnintel_dev TO `<sp-application-id>`;
-- GRANT CREATE MATERIALIZED VIEW ON CATALOG vulnintel_dev TO `<sp-application-id>`;  -- เพิ่มหลัง S3 รอบ 1: SDP pipeline ที่สร้าง MV ต้องใช้สิทธิ์นี้
