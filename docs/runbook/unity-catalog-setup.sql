-- Runbook: เตรียม Unity Catalog สำหรับ asset bundle (Task 1.2)
-- รันใน Databricks SQL editor โดยเจ้าของ catalog (ผู้ใช้เอง ไม่ใช่ service principal)
-- เอา <sp-application-id> ออกแล้วแทนด้วย Application ID ของ service principal vulnintel-ci (ไม่ใช่ secret) ก่อนรัน
-- ลำดับการรัน: ข้อ 1-3 รัน "ก่อน" merge PR #9 (cd deploy prod ครั้งแรกหลัง merge)
--   ข้อ 4 (DROP) รันครั้งเดียว หลังลบ job/pipeline ของ spike ทั้งหมดแล้ว และก่อน `databricks bundle deploy -t dev` ครั้งแรก
-- หมายเหตุ: สิทธิ์ CAN USE บน SQL warehouse ให้ grant ผ่าน UI (SQL Warehouses > Permissions) ไม่ใช่ SQL ในไฟล์นี้
--
-- bundle (databricks.yml + resources/schemas.yml) สร้างและเป็นเจ้าของ schema กับ volume เอง
-- ดังนั้น service principal ต้องมี CREATE SCHEMA และ CREATE VOLUME ที่ระดับ catalog (ต่างจาก grant ใน spikes/s2/setup.sql)

-- 1) catalog ของ prod (vulnintel_dev มีอยู่แล้วจาก spikes/s2/setup.sql)
CREATE CATALOG IF NOT EXISTS vulnintel;

-- 2) สิทธิ์ของ service principal บน catalog dev
GRANT USE CATALOG    ON CATALOG vulnintel_dev TO `<sp-application-id>`;
GRANT CREATE SCHEMA  ON CATALOG vulnintel_dev TO `<sp-application-id>`;
GRANT USE SCHEMA     ON CATALOG vulnintel_dev TO `<sp-application-id>`;
GRANT CREATE VOLUME  ON CATALOG vulnintel_dev TO `<sp-application-id>`;
GRANT READ VOLUME    ON CATALOG vulnintel_dev TO `<sp-application-id>`;
GRANT WRITE VOLUME   ON CATALOG vulnintel_dev TO `<sp-application-id>`;
GRANT SELECT         ON CATALOG vulnintel_dev TO `<sp-application-id>`;
GRANT MODIFY         ON CATALOG vulnintel_dev TO `<sp-application-id>`;
GRANT CREATE TABLE   ON CATALOG vulnintel_dev TO `<sp-application-id>`;
GRANT CREATE MATERIALIZED VIEW ON CATALOG vulnintel_dev TO `<sp-application-id>`;

-- 3) สิทธิ์ของ service principal บน catalog prod
GRANT USE CATALOG    ON CATALOG vulnintel TO `<sp-application-id>`;
GRANT CREATE SCHEMA  ON CATALOG vulnintel TO `<sp-application-id>`;
GRANT USE SCHEMA     ON CATALOG vulnintel TO `<sp-application-id>`;
GRANT CREATE VOLUME  ON CATALOG vulnintel TO `<sp-application-id>`;
GRANT READ VOLUME    ON CATALOG vulnintel TO `<sp-application-id>`;
GRANT WRITE VOLUME   ON CATALOG vulnintel TO `<sp-application-id>`;
GRANT SELECT         ON CATALOG vulnintel TO `<sp-application-id>`;
GRANT MODIFY         ON CATALOG vulnintel TO `<sp-application-id>`;
GRANT CREATE TABLE   ON CATALOG vulnintel TO `<sp-application-id>`;
GRANT CREATE MATERIALIZED VIEW ON CATALOG vulnintel TO `<sp-application-id>`;

-- 4) ทำครั้งเดียว: ล้าง schema ที่สร้างมือใน Phase 0 เพื่อให้ bundle สร้างและเป็นเจ้าของแทน
--    ลบเฉพาะข้อมูล spike ของ Phase 0 ใน vulnintel_dev (CASCADE ลบตารางและ volume ข้างใน)
--    ต้องรัน "หลัง" ลบ job/pipeline ของ spike ทั้งหมดแล้ว และ "ก่อน" `databricks bundle deploy -t dev` ครั้งแรก
--    ปิดด้วย comment ไว้ตั้งใจ: รันทั้งไฟล์โดยไม่ตั้งใจจะได้ไม่ลบอะไร เอา "-- " ออกเฉพาะบล็อกนี้เมื่อพร้อมเท่านั้น
-- DROP SCHEMA IF EXISTS vulnintel_dev.bronze CASCADE;
-- DROP SCHEMA IF EXISTS vulnintel_dev.silver CASCADE;
-- DROP SCHEMA IF EXISTS vulnintel_dev.gold   CASCADE;
-- DROP SCHEMA IF EXISTS vulnintel_dev.ops    CASCADE;
